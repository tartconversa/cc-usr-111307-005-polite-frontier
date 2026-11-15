"""调度器行为测试：手工时钟推进，全部合规结论来自事件日志而非休眠。"""
import os
import tempfile
import unittest
from email.utils import formatdate

from src.clock import ManualClock
from src.fetcher import FetchResult
from src.scheduler import OriginPolicy, Scheduler, SchedulerConfig
from src.store import Store

ALLOW_ALL = 'User-agent: *\nDisallow:\n'
FIXTURE_RULES = ('User-agent: DirectoryBot\n'
                 'Disallow: /private\n'
                 'Allow: /private/open$\n'
                 'Disallow: /*?secret=*\n')


def new_scheduler(policies, seeds=(), store=None, **overrides):
    options = dict(user_agent='DirectoryBot/1.0', origins=policies,
                   tracking_params=frozenset({'utm'}), robots_ttl=300.0,
                   robots_retry_delay=30.0, default_backoff=5.0,
                   max_attempts=3, max_hops=10, lease_ttl=60.0)
    options.update(overrides)
    clock = ManualClock()
    scheduler = Scheduler(store or Store(':memory:'),
                          SchedulerConfig(**options), clock)
    for seed in seeds:
        scheduler.add_seed(seed)
    return scheduler, clock


def serve_robots(scheduler, origin, text=ALLOW_ALL, status=200):
    """完成该站点排队中的 robots 请求（单站点场景）。"""
    claims = [c for c in scheduler.pump()
              if c.kind == 'robots' and c.origin == origin]
    assert claims, '没有等待中的 robots 请求: %s' % origin
    scheduler.complete(claims[0].token,
                       FetchResult(status, {'content-type': 'text/plain'},
                                   text.encode()))


def serve_all_robots(scheduler, texts=None, statuses=None):
    """一次泵取并完成所有待发的 robots 请求（多站点场景）。"""
    claims = [c for c in scheduler.pump() if c.kind == 'robots']
    assert claims, '没有等待中的 robots 请求'
    for claim in claims:
        text = (texts or {}).get(claim.origin, ALLOW_ALL)
        status = (statuses or {}).get(claim.origin, 200)
        scheduler.complete(claim.token, FetchResult(
            status, {'content-type': 'text/plain'}, text.encode()))


def drain(scheduler, body=b'<h1>ok</h1>'):
    """派发并完成所有当前可派发的工作（不推进时钟）。"""
    while not scheduler.done():
        claims = scheduler.pump()
        if not claims:
            return
        for claim in claims:
            scheduler.complete(claim.token,
                               FetchResult(200, {'content-type': 'text/html'}, body))


def assert_compliance(test, scheduler, policies):
    """从事件日志回放：任何时刻不超出站点并发上限与最小起始间隔。"""
    for origin, policy in policies.items():
        active = 0
        last_start = None
        for event in scheduler.store.events(origin=origin):
            if event['kind'] == 'dispatch':
                if last_start is not None:
                    test.assertGreaterEqual(
                        event['at'] - last_start, policy.min_interval - 1e-9,
                        '%s 起始间隔不足: %s' % (origin, event))
                last_start = event['at']
                active += 1
                test.assertLessEqual(active, policy.max_concurrency,
                                     '%s 并发超限: %s' % (origin, event))
            elif event['kind'] in ('finish', 'lease_expired', 'recovered'):
                active -= 1
                test.assertGreaterEqual(active, 0, '%s 槽位计数为负' % origin)


class BackoffTests(unittest.TestCase):

    def test_backoff_isolates_origin_and_log_proves_compliance(self):
        policies = {'http://a.test': OriginPolicy(2, 1.0, 50),
                    'http://b.test': OriginPolicy(2, 1.0, 50)}
        seeds = (['http://a.test/a1', 'http://a.test/a2']
                 + ['http://b.test/b%d' % i for i in range(1, 5)])
        sched, clock = new_scheduler(policies, seeds)
        serve_all_robots(sched)
        clock.advance(1.0)
        claims = sched.pump()
        self.assertEqual({c.url for c in claims},
                         {'http://a.test/a1', 'http://b.test/b1'})
        token_a = next(c.token for c in claims if c.origin == 'http://a.test')
        token_b = next(c.token for c in claims if c.origin == 'http://b.test')
        t429 = clock.now()
        sched.complete(token_a, FetchResult(429, {'retry-after': '10'}))
        sched.complete(token_b, FetchResult(200, {'content-type': 'text/html'}, b''))

        # 退避期间：A 静默，B 持续采集
        for _ in range(4):
            clock.advance(1.0)
            for claim in sched.pump():
                self.assertEqual(claim.origin, 'http://b.test')
                sched.complete(claim.token,
                               FetchResult(200, {'content-type': 'text/html'}, b''))
        waits = sched.store.events(origin='http://a.test', kinds=('wait',))
        self.assertTrue(any(w['detail']['reason'] == 'origin_backoff' for w in waits))

        # 退避结束：A 恢复，a1 以第 2 次尝试重新派发
        clock.advance_to(t429 + 10.0)
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://a.test/a1'])
        self.assertEqual(claims[0].attempt, 2)
        sched.complete(claims[0].token, FetchResult(200, {}, b''))

        b_log = sched.dispatch_log('http://b.test')
        a_log = sched.dispatch_log('http://a.test')
        self.assertTrue(any(t429 < e['at'] < t429 + 10 for e in b_log),
                        '退避窗口内 B 应继续采集')
        self.assertFalse(any(t429 < e['at'] < t429 + 10 for e in a_log),
                         '退避窗口内 A 不应有派发')
        drain(sched)
        clock.advance(1.0)
        drain(sched)
        assert_compliance(self, sched, policies)

    def test_retry_after_http_date(self):
        sched, clock = new_scheduler({'http://a.test': OriginPolicy(1, 0.0, 10)},
                                     ['http://a.test/p'])
        serve_robots(sched, 'http://a.test')
        claims = sched.pump()
        moment = clock.now()
        http_date = formatdate(moment + 30, usegmt=True)
        sched.complete(claims[0].token,
                       FetchResult(429, {'retry-after': http_date}))
        status = sched.origin_status()['http://a.test']
        self.assertAlmostEqual(status['backoff_until'], moment + 30, places=0)
        backoff = sched.store.events(origin='http://a.test', kinds=('backoff',))[0]
        self.assertEqual(backoff['detail']['retry_after'], http_date)


class RobotsLifecycleTests(unittest.TestCase):

    def test_rules_enforced_with_version(self):
        policies = {'http://a.test': OriginPolicy(4, 0.0, 20)}
        seeds = ['http://a.test/private', 'http://a.test/private/open',
                 'http://a.test/ok?secret=1', 'http://a.test/ok']
        sched, clock = new_scheduler(policies, seeds)
        serve_robots(sched, 'http://a.test', FIXTURE_RULES)
        claims = sched.pump()
        self.assertEqual({c.url for c in claims},
                         {'http://a.test/private/open', 'http://a.test/ok'})
        for claim in claims:
            sched.complete(claim.token, FetchResult(200, {}, b''))
        rejected = sched.store.events(kinds=('rejected',))
        by_url = {e['url']: e for e in rejected}
        self.assertEqual(by_url['http://a.test/private']['detail']['rule'], '/private')
        self.assertEqual(by_url['http://a.test/ok?secret=1']['detail']['rule'],
                         '/*?secret=*')
        for event in rejected:
            self.assertEqual(event['detail']['rules_version'], 1)
        assert_compliance(self, sched, policies)

    def test_robots_404_means_no_rules(self):
        sched, clock = new_scheduler({'http://a.test': OriginPolicy(2, 0.0, 10)},
                                     ['http://a.test/anything'])
        serve_robots(sched, 'http://a.test', status=404)
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://a.test/anything'])
        robots = sched.store.events(kinds=('robots',))[-1]
        self.assertEqual(robots['detail']['how'], 'no_rules')

    def test_robots_5xx_defers_only_that_origin(self):
        policies = {'http://a.test': OriginPolicy(2, 0.0, 10),
                    'http://b.test': OriginPolicy(2, 0.0, 10)}
        sched, clock = new_scheduler(policies,
                                     ['http://a.test/p', 'http://b.test/q'],
                                     robots_retry_delay=30.0)
        serve_all_robots(sched, statuses={'http://a.test': 500})
        # A 被暂缓，B 正常派发
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://b.test/q'])
        sched.complete(claims[0].token, FetchResult(200, {}, b''))
        robots = sched.store.events(origin='http://a.test', kinds=('robots',))[-1]
        self.assertEqual(robots['detail']['state'], 'deferred')
        waits = sched.store.events(origin='http://a.test', kinds=('wait',))
        self.assertTrue(any(w['detail']['reason'] == 'robots_refresh' for w in waits))
        # 暂缓期结束后重新获取 robots，A 恢复
        clock.advance(30.0)
        serve_robots(sched, 'http://a.test')
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://a.test/p'])
        assert_compliance(self, sched, policies)

    def test_expired_robots_blocks_dispatch_until_refresh_done(self):
        policies = {'http://a.test': OriginPolicy(2, 0.0, 10)}
        sched, clock = new_scheduler(policies, ['http://a.test/a'],
                                     robots_ttl=10.0)
        serve_robots(sched, 'http://a.test', 'User-agent: *\nDisallow: /gone\n')
        claims = sched.pump()
        sched.complete(claims[0].token, FetchResult(200, {}, b''))

        clock.advance(11.0)  # robots 缓存过期
        sched.add_seed('http://a.test/b')
        claims = sched.pump()
        self.assertEqual([c.kind for c in claims], ['robots'],
                         '缓存过期后只能派发 robots 刷新')
        self.assertEqual(sched.pump(), [], '刷新未完成前不得派发新请求')
        expiry = clock.now() - 1.0
        page_dispatches = [e for e in sched.dispatch_log('http://a.test')
                           if e['detail']['kind'] == 'page' and e['at'] > expiry]
        self.assertEqual(page_dispatches, [])

        sched.complete(claims[0].token,
                       FetchResult(200, {}, b'User-agent: *\nDisallow:\n'))
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://a.test/b'])
        dispatch = sched.dispatch_log('http://a.test')[-1]
        self.assertEqual(dispatch['detail']['rules_version'], 2,
                         '刷新后派发应使用新规则版本')
        sched.complete(claims[0].token, FetchResult(200, {}, b''))
        versions = [e['detail']['version']
                    for e in sched.store.events(kinds=('robots',))]
        self.assertEqual(versions, [1, 2], 'robots 重载应可观察')


class DedupAndRedirectTests(unittest.TestCase):

    def test_dedup_keeps_original_request_url(self):
        sched, clock = new_scheduler(
            {'http://a.test': OriginPolicy(5, 0.0, 20)},
            ['http://a.test/page?utm=x&b=1#frag'])
        serve_robots(sched, 'http://a.test')
        claims = sched.pump()
        self.assertEqual(claims[0].url, 'http://a.test/page?utm=x&b=1',
                         '请求使用原 URL：去片段、保留跟踪参数')
        html = (b'<a href="/page?b=1">x</a><a href="/page?utm=y&b=1">y</a>'
                b'<a href="/other">z</a>')
        sched.complete(claims[0].token,
                       FetchResult(200, {'content-type': 'text/html'}, html))
        rejected = sched.store.events(kinds=('rejected',))
        self.assertEqual(sum(1 for e in rejected
                             if e['detail']['reason'] == 'duplicate'), 2)
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://a.test/other'])
        sched.complete(claims[0].token, FetchResult(200, {}, b''))

    def test_signed_url_query_never_reordered(self):
        sched, clock = new_scheduler(
            {'http://a.test': OriginPolicy(5, 0.0, 20)},
            ['http://a.test/s?z=1&a=2&sig=%2Fabc&z=3'])
        serve_robots(sched, 'http://a.test')
        claims = sched.pump()
        self.assertEqual(claims[0].url, 'http://a.test/s?z=1&a=2&sig=%2Fabc&z=3')
        sched.complete(claims[0].token, FetchResult(200, {}, b''))

    def test_visited_marks_are_per_origin(self):
        policies = {'http://a.test': OriginPolicy(2, 0.0, 10),
                    'http://b.test': OriginPolicy(2, 0.0, 10)}
        sched, clock = new_scheduler(policies,
                                     ['http://a.test/shared', 'http://b.test/shared'])
        serve_all_robots(sched)
        claims = sched.pump()
        self.assertEqual({c.url for c in claims},
                         {'http://a.test/shared', 'http://b.test/shared'},
                         '不同 origin 不因同一路径共享已访问标记')
        for claim in claims:
            sched.complete(claim.token, FetchResult(200, {}, b''))

    def test_redirect_loop_terminates_with_chain(self):
        sched, clock = new_scheduler({'http://a.test': OriginPolicy(2, 0.0, 20)},
                                     ['http://a.test/x'])
        serve_robots(sched, 'http://a.test')
        claims = sched.pump()
        sched.complete(claims[0].token, FetchResult(302, {'location': '/y'}))
        claims = sched.pump()
        self.assertEqual([c.url for c in claims], ['http://a.test/y'])
        sched.complete(claims[0].token,
                       FetchResult(302, {'location': '/x'}))
        terminated = sched.store.events(kinds=('terminated',))
        self.assertEqual(len(terminated), 1)
        detail = terminated[0]['detail']
        self.assertEqual(detail['reason'], 'redirect_loop')
        self.assertEqual(detail['chain'], ['http://a.test/x', 'http://a.test/y',
                                           'http://a.test/x'])

    def test_redirect_hop_budget_terminates(self):
        sched, clock = new_scheduler({'http://a.test': OriginPolicy(2, 0.0, 20)},
                                     ['http://a.test/r0'], max_hops=2)
        serve_robots(sched, 'http://a.test')
        for i in range(3):
            claims = sched.pump()
            if not claims:
                break
            sched.complete(claims[0].token,
                           FetchResult(302, {'location': '/r%d' % (i + 1)}))
        terminated = sched.store.events(kinds=('terminated',))
        self.assertEqual(terminated[0]['detail']['reason'],
                         'redirect_hops_exhausted')

    def test_redirect_re_admitted_and_cross_origin(self):
        policies = {'http://a.test': OriginPolicy(2, 0.0, 20),
                    'http://b.test': OriginPolicy(2, 0.0, 20)}
        sched, clock = new_scheduler(
            policies, ['http://a.test/go1', 'http://a.test/go2'])
        serve_robots(sched, 'http://a.test')
        claims = {c.url: c for c in sched.pump()}
        # 跳转到 B 站被 robots 禁止的路径：每一跳都重新准入
        sched.complete(claims['http://a.test/go1'].token,
                       FetchResult(302, {'location': 'http://b.test/private'}))
        # 跳转到配置外的 origin：拒绝
        sched.complete(claims['http://a.test/go2'].token,
                       FetchResult(302, {'location': 'http://evil.test/x'}))
        rejected = sched.store.events(kinds=('rejected',))
        reasons = {(e['origin'], e['detail']['reason']) for e in rejected}
        self.assertIn(('http://evil.test', 'origin_not_allowed'), reasons)
        # B 站出现待抓任务后才懒加载它的 robots
        serve_robots(sched, 'http://b.test', 'User-agent: *\nDisallow: /private\n')
        self.assertEqual(sched.pump(), [], 'B 站 /private 应被 robots 拒绝')
        robot_rejects = sched.store.events(origin='http://b.test',
                                           kinds=('rejected',))
        self.assertEqual(robot_rejects[0]['detail']['reason'], 'robots_disallow')
        self.assertEqual(robot_rejects[0]['detail']['rule'], '/private')
        task = sched.store.tasks_by_url('http://b.test/private')[0]
        self.assertEqual(task['state'], 'rejected')
        self.assertIn('http://a.test/go1', task['chain'])


class BudgetLeaseRecoveryTests(unittest.TestCase):

    def test_budget_exhaustion_terminates_queue(self):
        sched, clock = new_scheduler(
            {'http://a.test': OriginPolicy(5, 0.0, 3)},
            ['http://a.test/%d' % i for i in range(1, 5)])
        serve_robots(sched, 'http://a.test')  # 预算含 robots：剩 2
        claims = sched.pump()
        self.assertEqual(len(claims), 2)
        for claim in claims:
            sched.complete(claim.token, FetchResult(200, {}, b''))
        self.assertEqual(sched.pump(), [])
        terminated = sched.store.events(kinds=('terminated',))
        self.assertEqual(len(terminated), 2)
        self.assertTrue(all(e['detail']['reason'] == 'budget_exhausted'
                            for e in terminated))
        self.assertTrue(sched.done())
        self.assertEqual(sched.origin_status()['http://a.test']['requests_used'], 3)

    def test_lease_expiry_reclaims_and_stale_worker_ignored(self):
        sched, clock = new_scheduler({'http://a.test': OriginPolicy(2, 0.0, 10)},
                                     ['http://a.test/p'], lease_ttl=10.0)
        serve_robots(sched, 'http://a.test')
        first = sched.pump()[0]
        clock.advance(11.0)  # 租约过期
        second = sched.pump()[0]
        self.assertNotEqual(first.token, second.token)
        expired = sched.store.events(kinds=('lease_expired',))
        self.assertEqual(expired[0]['detail']['token'], first.token)
        self.assertEqual(sched.origin_status()['http://a.test']['inflight'], 1,
                         '回收后只有新认领占用槽位')

        self.assertTrue(sched.complete(second.token, FetchResult(200, {}, b'')))
        # 过期 worker 迟到：不能释放别人的槽位，也不能覆盖较新的结果
        self.assertFalse(sched.complete(first.token, FetchResult(500, {}, b'')))
        key = 'http://a.test/p'
        self.assertEqual(sched.store.get_result('http://a.test', key)['status'], 200)
        task = sched.store.tasks_by_url('http://a.test/p')[0]
        self.assertEqual(task['state'], 'done')
        self.assertEqual(sched.inflight_count(), 0)
        stale = sched.store.events(kinds=('stale_completion',))
        self.assertEqual(stale[0]['detail']['token'], first.token)

    def test_crash_recovery_restores_queue_and_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'frontier.db')
            policies = {'http://a.test': OriginPolicy(1, 0.0, 3)}
            seeds = ['http://a.test/1', 'http://a.test/2', 'http://a.test/3']
            store1 = Store(path)
            sched1, clock = new_scheduler(policies, seeds, store=store1)
            serve_robots(sched1, 'http://a.test')      # 预算 1
            sched1.pump()                               # 预算 2，随后"崩溃"
            store1.close()

            store2 = Store(path)
            sched2, _ = new_scheduler(policies, (), store=store2)
            recovered = store2.events(kinds=('recovered',))
            self.assertEqual(len(recovered), 1, '中断的认领应恢复回队列')
            status = sched2.origin_status()['http://a.test']
            self.assertEqual(status['requests_used'], 2, '恢复后请求预算不重置')
            self.assertEqual(status['robots_version'], 1, '恢复后 robots 缓存仍有效')

            claims = sched2.pump()
            self.assertEqual([c.url for c in claims], ['http://a.test/1'])
            sched2.complete(claims[0].token, FetchResult(200, {}, b''))  # 预算 3
            self.assertEqual(sched2.pump(), [])
            terminated = store2.events(kinds=('terminated',))
            self.assertEqual(len(terminated), 2)
            self.assertTrue(all(e['detail']['reason'] == 'budget_exhausted'
                                for e in terminated))
            self.assertTrue(sched2.done())
            store2.close()


class FairnessAndGateTests(unittest.TestCase):

    def test_fair_rotation_prevents_starvation(self):
        policies = {'http://a.test': OriginPolicy(1, 0.0, 50),
                    'http://b.test': OriginPolicy(1, 0.0, 50)}
        seeds = (['http://a.test/a%d' % i for i in range(1, 7)]
                 + ['http://b.test/b1'])
        sched, clock = new_scheduler(policies, seeds)
        serve_all_robots(sched)
        drain(sched)
        page_log = [e for e in sched.store.events(kinds=('dispatch',))
                    if e['detail']['kind'] == 'page']
        order = [e['url'] for e in page_log]
        self.assertLess(order.index('http://b.test/b1'),
                        order.index('http://a.test/a2'),
                        '单任务站点不应被大积压站点饿死: %s' % order)
        self.assertEqual(order[:2], ['http://a.test/a1', 'http://b.test/b1'])
        self.assertTrue(sched.done())

    def test_concurrency_and_interval_gates(self):
        policies = {'http://a.test': OriginPolicy(2, 5.0, 20)}
        sched, clock = new_scheduler(policies, ['http://a.test/%d' % i
                                                for i in range(1, 4)])
        serve_robots(sched, 'http://a.test')
        clock.advance(5.0)
        first = sched.pump()
        self.assertEqual([c.url for c in first], ['http://a.test/1'])
        self.assertEqual(sched.pump(), [])  # 间隔未到
        clock.advance(5.0)
        second = sched.pump()
        self.assertEqual([c.url for c in second], ['http://a.test/2'])
        clock.advance(5.0)
        self.assertEqual(sched.pump(), [])  # 并发已满（两个在途）
        waits = sched.store.events(kinds=('wait',))
        reasons = [w['detail']['reason'] for w in waits]
        self.assertIn('min_interval', reasons)
        self.assertIn('concurrency_limit', reasons)
        sched.complete(first[0].token, FetchResult(200, {}, b''))
        third = sched.pump()
        self.assertEqual([c.url for c in third], ['http://a.test/3'])
        sched.complete(second[0].token, FetchResult(200, {}, b''))
        sched.complete(third[0].token, FetchResult(200, {}, b''))
        assert_compliance(self, sched, policies)

    def test_explain_traces_url_lifecycle(self):
        sched, clock = new_scheduler({'http://a.test': OriginPolicy(2, 1.0, 10)},
                                     ['http://a.test/p'])
        serve_robots(sched, 'http://a.test')
        sched.pump()  # 间隔未到 → 等待
        clock.advance(1.0)
        claims = sched.pump()
        sched.complete(claims[0].token, FetchResult(200, {}, b''))
        report = sched.explain('http://a.test/p')
        kinds = [e['kind'] for e in report['events']]
        self.assertEqual(kinds, ['enqueued', 'wait', 'wait', 'dispatch', 'finish'])
        waits = [e['detail']['reason'] for e in report['events'] if e['kind'] == 'wait']
        self.assertEqual(waits, ['robots_refresh', 'min_interval'])
        dispatch = report['events'][3]
        self.assertEqual(dispatch['detail']['rules_version'], 1)
        self.assertEqual(report['tasks'][0]['state'], 'done')
        self.assertEqual(report['tasks'][0]['detail'], 'ok')


if __name__ == '__main__':
    unittest.main()
