"""驱动调度器的工作池与演示入口。

worker 线程只执行已认领的请求；退避、间隔、预算都是调度器状态，
没有任何线程为等待而睡眠占位——一个站退避不会占住其他站的工作线程。
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor

from .fetcher import FetchResult


class Runner:
    """泵取调度器认领的工作，用线程池执行 HTTP 请求并回报结果。"""

    def __init__(self, scheduler, fetcher, workers: int = 8):
        self.sched = scheduler
        self.fetcher = fetcher
        self.workers = workers

    def _work(self, claim):
        try:
            result = self.fetcher.fetch(claim.url)
        except Exception as exc:  # 抓取器不应抛异常，兜底保证调度器不失联
            result = FetchResult(None, {}, error='%s: %s' % (type(exc).__name__, exc))
        self.sched.complete(claim.token, result)

    def run(self, max_rounds: int = 100000) -> bool:
        """运行到队列耗尽；返回是否全部完成。"""
        with ThreadPoolExecutor(self.workers) as pool:
            for _ in range(max_rounds):
                if self.sched.done():
                    return True
                claims = self.sched.pump()
                for claim in claims:
                    pool.submit(self._work, claim)
                if claims:
                    continue
                if self.sched.inflight_count() > 0:
                    self.sched.wait_idle(timeout=10.0)  # 等在途请求完成
                    continue
                moment = self.sched.next_wakeup()
                if moment is None:
                    break
                clock = self.sched.clock
                advance_to = getattr(clock, 'advance_to', None)
                if advance_to is not None:
                    advance_to(max(moment, clock.now()))
                else:
                    self.sched.wait_change(timeout=max(0.0, moment - clock.now()))
        return self.sched.done()


def _demo():
    """两个本机演练站：A 站持续 429 后退避恢复，B 站正常采集。"""
    import json
    import threading
    from . import source
    from .clock import SystemClock
    from .fetcher import HttpFetcher
    from .scheduler import OriginPolicy, Scheduler, SchedulerConfig
    from .store import Store
    from urllib.request import Request, urlopen

    server_a, server_b = source.make_server(), source.make_server()
    for server in (server_a, server_b):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    port_a, port_b = server_a.server_port, server_b.server_port

    def control(port, **fields):
        request = Request('http://127.0.0.1:%d/__control' % port,
                          data=json.dumps(fields).encode(), method='POST')
        with urlopen(request, timeout=5):
            pass

    control(port_a, retry_after='2')   # A 站 /busy 返回 429，Retry-After: 2
    control(port_b, busy=False)        # B 站正常
    threading.Timer(3.0, control, args=(port_a,), kwargs={'busy': False}).start()

    origin_a = 'http://127.0.0.1:%d' % port_a
    origin_b = 'http://127.0.0.1:%d' % port_b
    policies = {origin_a: OriginPolicy(2, 0.5, 40), origin_b: OriginPolicy(2, 0.5, 40)}
    config = SchedulerConfig(user_agent='DirectoryBot', origins=policies,
                             tracking_params=frozenset({'utm'}), robots_ttl=60.0,
                             max_attempts=4)
    scheduler = Scheduler(Store(':memory:'), config, SystemClock())
    for seed in ('%s/' % origin_a, '%s/' % origin_b):
        scheduler.add_seed(seed)

    finished = Runner(scheduler, HttpFetcher('DirectoryBot'), workers=4).run()
    print('完成:', finished)
    print(json.dumps(scheduler.origin_status(), ensure_ascii=False, indent=2))
    print('--- A 站 /busy 的完整轨迹 ---')
    for event in scheduler.explain('%s/busy' % origin_a)['events']:
        print('%.3f %-10s %s' % (event['at'], event['kind'],
                                 json.dumps(event['detail'], ensure_ascii=False)))
    for server in (server_a, server_b):
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    _demo()
