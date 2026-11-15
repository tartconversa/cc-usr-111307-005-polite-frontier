"""端到端：两个本机演练站，一个 429 退避、一个持续采集。

时钟是手工推进的；合规结论全部来自调度器的事件日志，
不依赖任何休眠时长。A 站第一次 429 后由 FlipFetcher 同步解除繁忙，
演示退避结束后的恢复。
"""
import json
import threading
import unittest
from urllib.request import Request, urlopen

from src import source
from src.clock import ManualClock
from src.fetcher import HttpFetcher
from src.runner import Runner
from src.scheduler import OriginPolicy, Scheduler, SchedulerConfig
from src.store import Store


def control(port, **fields):
    request = Request('http://127.0.0.1:%d/__control' % port,
                      data=json.dumps(fields).encode(), method='POST')
    with urlopen(request, timeout=5):
        pass


class FlipFetcher:
    """包装真实抓取器：A 站第一次 429 后同步解除其繁忙状态。"""

    def __init__(self, inner, port):
        self.inner = inner
        self.port = port
        self.flipped = False

    def fetch(self, url):
        result = self.inner.fetch(url)
        if result.status == 429 and not self.flipped:
            self.flipped = True
            control(self.port, busy=False)
        return result


def assert_compliance(test, scheduler, policies):
    for origin, policy in policies.items():
        active = 0
        last_start = None
        for event in scheduler.store.events(origin=origin):
            if event['kind'] == 'dispatch':
                if last_start is not None:
                    test.assertGreaterEqual(event['at'] - last_start,
                                            policy.min_interval - 1e-9)
                last_start = event['at']
                active += 1
                test.assertLessEqual(active, policy.max_concurrency)
            elif event['kind'] in ('finish', 'lease_expired', 'recovered'):
                active -= 1
                test.assertGreaterEqual(active, 0)


class EndToEndTests(unittest.TestCase):

    def test_backoff_isolation_against_live_servers(self):
        server_a, server_b = source.make_server(), source.make_server()
        threads = [threading.Thread(target=s.serve_forever, daemon=True)
                   for s in (server_a, server_b)]
        for thread in threads:
            thread.start()
        try:
            port_a, port_b = server_a.server_port, server_b.server_port
            control(port_a, retry_after='2', location='/jump')  # /busy→429, /jump 自跳转
            control(port_b, busy=False)
            origin_a = 'http://127.0.0.1:%d' % port_a
            origin_b = 'http://127.0.0.1:%d' % port_b
            policies = {origin_a: OriginPolicy(2, 0.2, 40),
                        origin_b: OriginPolicy(2, 0.2, 40)}
            store = Store(':memory:')
            config = SchedulerConfig(
                user_agent='DirectoryBot', origins=policies,
                tracking_params=frozenset({'utm'}), robots_ttl=60.0,
                robots_retry_delay=5.0, default_backoff=2.0,
                max_attempts=4, lease_ttl=60.0)
            sched = Scheduler(store, config, ManualClock())
            for seed in ('%s/' % origin_a, '%s/' % origin_b,
                         '%s/jump' % origin_a, '%s/jump' % origin_b):
                sched.add_seed(seed)

            fetcher = FlipFetcher(HttpFetcher('DirectoryBot', timeout=5), port_a)
            self.assertTrue(Runner(sched, fetcher, workers=4).run(),
                            '队列应全部处理完')
            self.assertTrue(fetcher.flipped, 'A 站应发生过 429')

            # A 的 /busy：先 429 退避，恢复后抓到 200
            busy = sched.explain('%s/busy' % origin_a)
            outcomes = [e['detail'].get('outcome') for e in busy['events']
                        if e['kind'] == 'finish']
            self.assertEqual(outcomes, ['requeued', 'fetched'])
            self.assertEqual(busy['tasks'][0]['state'], 'done')

            # 退避窗口内：A 静默，B 继续派发
            backoff = store.events(origin=origin_a, kinds=('backoff',))[0]
            until = backoff['detail']['until']
            a_log = sched.dispatch_log(origin_a)
            b_log = sched.dispatch_log(origin_b)
            self.assertFalse(any(backoff['at'] < e['at'] < until for e in a_log))
            self.assertTrue(any(backoff['at'] < e['at'] < until for e in b_log))

            assert_compliance(self, sched, policies)

            # robots 规则生效：/private 两站都被拒，且记录了命中规则
            rejected = store.events(kinds=('rejected',))
            private = [e for e in rejected if e['url'].endswith('/private')]
            self.assertEqual(len(private), 2)
            for event in private:
                self.assertEqual(event['detail']['reason'], 'robots_disallow')
                self.assertEqual(event['detail']['rule'], '/private')

            # 跟踪参数判重：/detail/1?utm=x 与 /detail/1#top 只抓取一次
            a_requests = server_a.source.state['requests']
            detail1 = [r for r in a_requests if r['target'].startswith('/detail/1')]
            self.assertEqual(len(detail1), 1)
            # robots 每站只取一次（缓存生效）
            self.assertEqual(len([r for r in a_requests
                                  if r['target'] == '/robots.txt']), 1)
            # 所有请求都带指定的 User-Agent
            for request in a_requests + server_b.source.state['requests']:
                self.assertEqual(request['headers'].get('User-Agent'),
                                 'DirectoryBot')

            # A 站 /jump 自跳转：环路终止；B 站 /jump → /detail/1：判重
            terminated = store.events(origin=origin_a, kinds=('terminated',))
            self.assertTrue(any(e['detail']['reason'] == 'redirect_loop'
                                for e in terminated))
            b_rejected = store.events(origin=origin_b, kinds=('rejected',))
            self.assertTrue(any(e['detail']['reason'] == 'duplicate'
                                for e in b_rejected))

            # 预算可观察：A 站 robots+/+/jump+/detail/1+/busy×2，B 站少一次重试
            status = sched.origin_status()
            self.assertEqual(status[origin_a]['requests_used'], 6)
            self.assertEqual(status[origin_b]['requests_used'], 5)
        finally:
            for server in (server_a, server_b):
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join()


if __name__ == '__main__':
    unittest.main()
