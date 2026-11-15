"""按站点准入与公平调度的爬取调度器。

设计要点：
- 每个 origin 独立的并发上限、最小起始间隔、请求预算与退避截止时刻；
  一个站 429/503 退避只暂停该站，其他站继续采集。
- robots.txt 按配置的 User-Agent 获取并缓存；缓存过期且刷新未完成前
  不派发该站任何新请求；5xx/网络失败暂缓该站，其他站不受影响。
- 重定向不自动跟随：每一跳作为新任务重新过 robots/预算/判重准入，
  环路跳回已访问键或跳数超预算即终止；已访问标记按 origin 隔离。
- 每次派发记录时间、准入规则版本与租约令牌；每个 URL 都能通过
  explain() 解释为什么被访问、等待或拒绝。
- 队列、租约与结果持久化在 SQLite（Store），进程崩溃重开即可恢复；
  过期租约被回收后，迟到的旧 worker 无法释放他人槽位或覆盖较新结果。

公平算法：按站点轮转——每次成功派发后把该站点移到队尾，站点内按
入队顺序先进先出；被门控（退避/间隔/并发/robots）的站点让出本轮，
不阻塞其他站点，因此退避站点不会占住工作线程。
"""
from __future__ import annotations
import json
import threading
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

from .fetcher import FetchResult
from .links import extract_links
from .robots import RobotsRules, ua_token_of
from .store import Store
from .urlnorm import UrlError, normalize_url, origin_of, resolve_url

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
RETRYABLE_STATUSES = frozenset({429, 503})


@dataclass
class OriginPolicy:
    max_concurrency: int = 2   # 站点最大并发
    min_interval: float = 1.0  # 相邻请求起始的最小间隔（秒）
    budget: int = 100          # 请求预算（含 robots.txt 请求）


@dataclass
class SchedulerConfig:
    user_agent: str
    origins: dict              # origin -> OriginPolicy，仅这些 origin 允许抓取
    tracking_params: frozenset = frozenset()  # 判重时删除的跟踪参数
    robots_ttl: float = 300.0
    robots_retry_delay: float = 30.0   # robots 获取失败后的暂缓时长
    default_backoff: float = 5.0       # 无 Retry-After 时的退避时长
    max_attempts: int = 3              # 单 URL 最大尝试次数
    max_hops: int = 10                 # 重定向链跳数预算
    lease_ttl: float = 60.0            # 认领租约时长
    owner: str = 'scheduler'


@dataclass
class Claim:
    """一次已认领、待执行 HTTP 请求的工作单元。"""
    token: int
    kind: str        # 'page' | 'robots'
    origin: str
    url: str
    task_id: int | None
    attempt: int


class OriginRuntime:
    """站点运行状态；持久字段写穿到 origins 表，崩溃后仍可恢复。"""

    def __init__(self, row=None):
        row = row or {}
        self.backoff_until = row.get('backoff_until') or 0.0
        self.requests_used = row.get('requests_used') or 0
        self.last_start = row.get('last_start')
        self.robots_state = row.get('robots_state') or 'unknown'
        self.robots_text = row.get('robots_text')
        self.robots_version = row.get('robots_version') or 0
        self.robots_fetched_at = row.get('robots_fetched_at')
        self.robots_expires_at = row.get('robots_expires_at')
        self.robots_retry_at = row.get('robots_retry_at') or 0.0
        self.refreshing = False  # 仅内存：刷新中的请求随进程消失
        self.rules = (RobotsRules.parse(self.robots_text)
                      if self.robots_text else RobotsRules.allow_all())

    def robots_fresh(self, now) -> bool:
        return (self.robots_state == 'fresh'
                and self.robots_expires_at is not None
                and now < self.robots_expires_at)


def parse_retry_after(value, now: float):
    """Retry-After 支持秒数或 HTTP 日期；返回相对秒数，无法解析返回 None。"""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment is None:
        return None
    return max(0.0, moment.timestamp() - now)


def _path_query(url: str) -> str:
    parts = urlsplit(url)
    text = parts.path or '/'
    if parts.query:
        text += '?' + parts.query
    return text


class Scheduler:

    def __init__(self, store: Store, config: SchedulerConfig, clock):
        self.store = store
        self.cfg = config
        self.clock = clock
        # 配置里的 origin 统一规范化，避免大小写/默认端口写法不一致
        self.cfg.origins = {origin_of(key): policy for key, policy in config.origins.items()}
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self._generation = 0
        self.inflight: dict[int, tuple] = {}  # token -> (origin, kind, task_id)
        self._origins: dict[str, OriginRuntime] = {}
        for row in store.all_origins():
            self._origins[row['origin']] = OriginRuntime(row)
        for origin in self.cfg.origins:
            if origin not in self._origins:
                store.ensure_origin(origin)
                self._origins[origin] = OriginRuntime()
        self._served_last = None
        now = self.clock.now()
        for task in store.recover():
            store.event(now, task['origin'], task['id'], task['url'], 'recovered',
                        {'from': 'claimed', 'token': task['lease_token']})

    # ------------------------------------------------------------------
    # 入队与准入
    # ------------------------------------------------------------------

    def add_seed(self, url: str):
        with self.lock:
            return self._admit(None, url, 'seed', [], 0, self.clock.now()) is not None

    def _admit(self, parent, raw_url, reason, chain, hops, now):
        """链接/种子/跳转目标的统一入口：解析、origin 白名单、环路、判重。"""
        base = parent['url'] if parent else raw_url
        parent_id = parent['id'] if parent else None
        try:
            url = resolve_url(base, raw_url)
            key = normalize_url(url, self.cfg.tracking_params)
            origin = origin_of(url)
        except UrlError as exc:
            self.store.event(now, None, parent_id, raw_url, 'rejected',
                             {'reason': 'invalid_url', 'error': str(exc), 'via': reason})
            return None
        if origin not in self.cfg.origins:
            self.store.event(now, origin, parent_id, url, 'rejected',
                             {'reason': 'origin_not_allowed', 'via': reason})
            return None
        if key in chain:
            self.store.event(now, origin, parent_id, url, 'terminated',
                             {'reason': 'redirect_loop', 'chain': chain + [key]})
            return None
        task_id = self.store.enqueue(origin, url, key, reason, parent_id, chain, hops, now)
        if task_id is None:
            self.store.event(now, origin, parent_id, url, 'rejected',
                             {'reason': 'duplicate', 'via': reason})
            return None
        self.store.event(now, origin, task_id, url, 'enqueued',
                         {'reason': reason, 'parent': parent_id, 'chain': chain, 'hops': hops})
        return task_id

    # ------------------------------------------------------------------
    # 派发
    # ------------------------------------------------------------------

    def pump(self):
        """评估当前时刻可派发的工作并认领；返回 Claim 列表（可能为空）。"""
        now = self.clock.now()
        with self.lock:
            self._reclaim(now)
            claims = []
            for origin in sorted(self.cfg.origins):
                if self._robots_needed(origin, now):
                    claim = self._try_claim_robots(origin, now)
                    if claim:
                        claims.append(claim)
            while True:
                claim = self._try_claim_page(now)
                if claim is None:
                    break
                claims.append(claim)
            if claims:
                self._touch()
            return claims

    def _reclaim(self, now):
        for row in self.store.reclaim_expired(now):
            self.inflight.pop(row['lease_token'], None)
            self.store.event(now, row['origin'], row['id'], row['url'],
                             'lease_expired', {'token': row['lease_token']})

    def _robots_needed(self, origin, now) -> bool:
        runtime = self._origins[origin]
        if runtime.refreshing or runtime.robots_fresh(now):
            return False
        if runtime.robots_state == 'deferred' and now < runtime.robots_retry_at:
            return False
        return self.store.has_pending(origin)

    def _origin_gates(self, runtime, policy, now):
        """robots 与页面共用的站点级门控；返回 None 表示通过，否则返回原因。"""
        if now < runtime.backoff_until:
            return 'origin_backoff'
        if runtime.requests_used >= policy.budget:
            return 'budget_exhausted'
        if runtime.last_start is not None and now < runtime.last_start + policy.min_interval:
            return 'min_interval'
        return None

    def _try_claim_robots(self, origin, now):
        runtime = self._origins[origin]
        policy = self.cfg.origins[origin]
        if self._origin_gates(runtime, policy, now):
            return None
        if self._inflight_origin(origin) >= policy.max_concurrency:
            return None
        token = self.store.next_token()
        runtime.refreshing = True
        runtime.requests_used += 1
        runtime.last_start = now
        self.store.bump_origin_counters(origin, now)
        self.inflight[token] = (origin, 'robots', None)
        url = origin + '/robots.txt'
        self.store.event(now, origin, None, url, 'dispatch',
                         {'token': token, 'kind': 'robots',
                          'rules_version': runtime.robots_version,
                          'budget_used': runtime.requests_used})
        return Claim(token, 'robots', origin, url, None, 1)

    def _fair_order(self):
        """按站点轮转：上次被服务的站点排到队尾，其余按最早任务入队序。"""
        heads = self.store.pending_heads()
        heads.sort(key=lambda h: h['head'])
        origins = [h['origin'] for h in heads]
        if self._served_last in origins:
            at = origins.index(self._served_last)
            origins = origins[at + 1:] + origins[:at + 1]
        return origins

    def _try_claim_page(self, now):
        for origin in self._fair_order():
            runtime = self._origins[origin]
            policy = self.cfg.origins[origin]
            while True:
                task = self.store.head_task(origin)
                if task is None:
                    break
                action = self._evaluate(origin, runtime, policy, task, now)
                if action == 'claim':
                    return self._claim_page(origin, runtime, policy, task, now)
                if action == 'drop':
                    continue  # 该任务已终结，看本站点下一个
                break  # wait：让出本轮，看下一个站点
        return None

    def _evaluate(self, origin, runtime, policy, task, now):
        """返回 'claim' / 'wait' / 'drop'；wait 与 drop 都会留下可解释记录。"""
        if self.store.get_result(origin, task['url_key']):
            self._drop(origin, task, 'rejected', 'duplicate',
                       {'via': 'result_exists'}, now)
            return 'drop'
        if runtime.requests_used >= policy.budget:
            self._drop(origin, task, 'terminated', 'budget_exhausted',
                       {'budget': policy.budget, 'requests_used': runtime.requests_used}, now)
            return 'drop'
        if not runtime.robots_fresh(now):
            self._wait(origin, task, 'robots_refresh',
                       {'robots_state': runtime.robots_state,
                        'retry_at': runtime.robots_retry_at}, now)
            return 'wait'
        allowed, rule = runtime.rules.check(ua_token_of(self.cfg.user_agent),
                                            _path_query(task['url']))
        if not allowed:
            self._drop(origin, task, 'rejected', 'robots_disallow',
                       {'rule': rule.pattern if rule else None,
                        'rules_version': runtime.robots_version}, now)
            return 'drop'
        gate = self._origin_gates(runtime, policy, now)
        if gate == 'origin_backoff':
            self._wait(origin, task, 'origin_backoff',
                       {'until': runtime.backoff_until}, now)
            return 'wait'
        if self._inflight_origin(origin) >= policy.max_concurrency:
            self._wait(origin, task, 'concurrency_limit',
                       {'max': policy.max_concurrency}, now)
            return 'wait'
        if gate == 'min_interval':
            self._wait(origin, task, 'min_interval',
                       {'ready_at': runtime.last_start + policy.min_interval}, now)
            return 'wait'
        return 'claim'

    def _claim_page(self, origin, runtime, policy, task, now):
        token = self.store.next_token()
        expires = now + self.cfg.lease_ttl
        if not self.store.claim_task(task['id'], self.cfg.owner, token, expires, origin, now):
            return None
        runtime.requests_used += 1
        runtime.last_start = now
        self.inflight[token] = (origin, 'page', task['id'])
        self._served_last = origin
        self.store.event(now, origin, task['id'], task['url'], 'dispatch',
                         {'token': token, 'kind': 'page', 'attempt': task['attempts'] + 1,
                          'rules_version': runtime.robots_version,
                          'budget_used': runtime.requests_used,
                          'lease_expires': expires})
        return Claim(token, 'page', origin, task['url'], task['id'], task['attempts'] + 1)

    def _wait(self, origin, task, reason, extra, now):
        if task['wait_reason'] != reason:  # 同一原因只记一次，避免刷屏
            self.store.set_wait(task['id'], reason)
            detail = {'reason': reason}
            detail.update(extra)
            self.store.event(now, origin, task['id'], task['url'], 'wait', detail)

    def _drop(self, origin, task, state, reason, extra, now):
        self.store.drop_task(task['id'], state, reason)
        detail = {'reason': reason}
        detail.update(extra)
        self.store.event(now, origin, task['id'], task['url'],
                         'rejected' if state == 'rejected' else 'terminated', detail)

    # ------------------------------------------------------------------
    # 完成
    # ------------------------------------------------------------------

    def complete(self, token: int, result: FetchResult) -> bool:
        """worker 回报结果；令牌不认识的（租约已过期被回收）一律忽略。"""
        now = self.clock.now()
        with self.lock:
            entry = self.inflight.pop(token, None)
            if entry is None:
                self.store.event(now, None, None, None, 'stale_completion',
                                 {'token': token})
                self._touch()
                return False
            origin, kind, task_id = entry
            if kind == 'robots':
                self._complete_robots(origin, token, result, now)
            else:
                self._complete_page(origin, task_id, token, result, now)
            self._touch()
            return True

    def _complete_robots(self, origin, token, result, now):
        runtime = self._origins[origin]
        runtime.refreshing = False
        status = result.status
        if result.error or status is None:
            outcome = 'deferred'
            self._defer_robots(origin, runtime, now, 'network: %s' % result.error)
        elif status in RETRYABLE_STATUSES:
            outcome = 'deferred'
            delay = parse_retry_after(result.header('retry-after'), now)
            self._set_backoff(origin, runtime, now, delay, result.header('retry-after'), status)
            self._defer_robots(origin, runtime, now, 'http_%d' % status,
                               retry_at=runtime.backoff_until)
        elif 200 <= status < 300:
            outcome = 'fresh'
            text = result.body.decode('utf-8', 'replace')
            runtime.robots_text = text
            runtime.rules = RobotsRules.parse(text)
            self._fresh_robots(origin, runtime, now, 'parsed')
        elif 400 <= status < 500:  # 404 等：视为无规则
            outcome = 'fresh'
            runtime.robots_text = ''
            runtime.rules = RobotsRules.allow_all()
            self._fresh_robots(origin, runtime, now, 'no_rules')
        else:  # 5xx：暂缓该站
            outcome = 'deferred'
            self._defer_robots(origin, runtime, now, 'http_%d' % status)
        self.store.event(now, origin, None, origin + '/robots.txt', 'finish',
                         {'token': token, 'outcome': outcome, 'status': status,
                          'error': result.error})

    def _fresh_robots(self, origin, runtime, now, how):
        runtime.robots_state = 'fresh'
        runtime.robots_fetched_at = now
        runtime.robots_expires_at = now + self.cfg.robots_ttl
        runtime.robots_version += 1
        self.store.update_origin(
            origin, robots_state='fresh', robots_text=runtime.robots_text,
            robots_version=runtime.robots_version, robots_fetched_at=now,
            robots_expires_at=runtime.robots_expires_at)
        self.store.event(now, origin, None, origin + '/robots.txt', 'robots',
                         {'state': 'fresh', 'how': how, 'version': runtime.robots_version,
                          'expires_at': runtime.robots_expires_at})

    def _defer_robots(self, origin, runtime, now, why, retry_at=None):
        runtime.robots_state = 'deferred'
        runtime.robots_retry_at = (retry_at if retry_at is not None
                                   else now + self.cfg.robots_retry_delay)
        self.store.update_origin(origin, robots_state='deferred',
                                 robots_retry_at=runtime.robots_retry_at)
        self.store.event(now, origin, None, origin + '/robots.txt', 'robots',
                         {'state': 'deferred', 'why': why,
                          'retry_at': runtime.robots_retry_at})

    def _complete_page(self, origin, task_id, token, result, now):
        runtime = self._origins[origin]
        policy = self.cfg.origins[origin]
        task = self.store.task_by_id(task_id)
        status = result.status
        if result.error or status is None:
            outcome = self._retry_or_terminate(origin, task, token, now,
                                               'network', result.error)
        elif status in RETRYABLE_STATUSES:
            delay = parse_retry_after(result.header('retry-after'), now)
            self._set_backoff(origin, runtime, now, delay,
                              result.header('retry-after'), status)
            outcome = self._retry_or_terminate(origin, task, token, now,
                                               'http_%d' % status, None)
        elif 500 <= status:
            self._set_backoff(origin, runtime, now, None, None, status)
            outcome = self._retry_or_terminate(origin, task, token, now,
                                               'http_%d' % status, None)
        elif status in REDIRECT_STATUSES and result.header('location'):
            self.store.save_result(origin, task['url_key'], status, token, now)
            self.store.finish_task(task_id, token, 'done', 'redirect:%d' % status)
            self._follow_redirect(origin, task, result.header('location'), now)
            outcome = 'redirect'
        else:
            self.store.save_result(origin, task['url_key'], status, token, now)
            self.store.finish_task(task_id, token, 'done',
                                   'ok' if 200 <= status < 300 else 'http_%s' % status)
            content_type = (result.header('content-type') or '').lower()
            if 200 <= status < 300 and 'text/html' in content_type:
                for href in extract_links(result.body):
                    self._admit(task, href, 'discovered', [], 0, now)
            outcome = 'fetched'
        self.store.event(now, origin, task_id, task['url'], 'finish',
                         {'token': token, 'outcome': outcome, 'status': status,
                          'error': result.error})

    def _set_backoff(self, origin, runtime, now, delay, raw, status):
        if delay is None:
            delay = self.cfg.default_backoff
        runtime.backoff_until = max(runtime.backoff_until, now + delay)
        self.store.update_origin(origin, backoff_until=runtime.backoff_until)
        self.store.event(now, origin, None, None, 'backoff',
                         {'until': runtime.backoff_until, 'retry_after': raw,
                          'status': status})

    def _retry_or_terminate(self, origin, task, token, now, why, error):
        if task['attempts'] + 1 >= self.cfg.max_attempts:
            self.store.finish_task(task_id, token, 'terminated', 'attempts_exhausted')
            self.store.event(now, origin, task['id'], task['url'], 'terminated',
                             {'reason': 'attempts_exhausted', 'why': why,
                              'attempts': task['attempts'] + 1})
            return 'terminated'
        self.store.requeue(task['id'], token)
        self.store.event(now, origin, task['id'], task['url'], 'requeued',
                         {'why': why, 'attempt': task['attempts'] + 1, 'error': error})
        return 'requeued'

    def _follow_redirect(self, origin, task, location, now):
        """重定向每一跳都作为新任务重新准入；环路或跳数超预算即终止。"""
        hops = task['hops'] + 1
        chain = json.loads(task['chain']) + [task['url_key']]
        if hops > self.cfg.max_hops:
            self.store.event(now, origin, task['id'], task['url'], 'terminated',
                             {'reason': 'redirect_hops_exhausted', 'hops': hops,
                              'chain': chain})
            return
        child = self._admit(task, location, 'redirect', chain, hops, now)
        self.store.event(now, origin, task['id'], task['url'], 'redirect',
                         {'location': location, 'hops': hops, 'chain': chain,
                          'admitted': child is not None})

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    def done(self) -> bool:
        with self.lock:
            return not self.inflight and self.store.pending_count() == 0

    def inflight_count(self) -> int:
        with self.lock:
            return len(self.inflight)

    def _inflight_origin(self, origin) -> int:
        return sum(1 for entry in self.inflight.values() if entry[0] == origin)

    def next_wakeup(self):
        """下一个可能解除门控的时刻；None 表示只能等完成事件或没有工作。"""
        now = self.clock.now()
        with self.lock:
            moments = []
            for origin, policy in self.cfg.origins.items():
                if not self.store.has_pending(origin):
                    continue
                runtime = self._origins[origin]
                candidates = [runtime.backoff_until]
                if runtime.last_start is not None:
                    candidates.append(runtime.last_start + policy.min_interval)
                if runtime.robots_state == 'deferred':
                    candidates.append(runtime.robots_retry_at)
                if runtime.robots_state == 'fresh' and runtime.robots_expires_at:
                    candidates.append(runtime.robots_expires_at)
                moments.extend(t for t in candidates if t is not None and t > now)
            moments.extend(t for t in self.store.claimed_expiries() if t > now)
            return min(moments) if moments else None

    def wait_change(self, timeout=None) -> bool:
        """等待任意状态变化（完成、回收等）；真实时钟下用于驱动循环。"""
        with self.cond:
            generation = self._generation
            self.cond.wait_for(lambda: self._generation != generation, timeout)
            return self._generation != generation

    def wait_idle(self, timeout=None) -> bool:
        """等到在途请求归零；谓词与 complete() 同锁求值，不会错过通知。"""
        with self.cond:
            self.cond.wait_for(lambda: not self.inflight, timeout)
            return not self.inflight

    def _touch(self):
        self._generation += 1
        self.cond.notify_all()

    def explain(self, url: str):
        """解释一个 URL 为什么被访问、等待或拒绝。"""
        with self.lock:
            return {'url': url,
                    'tasks': self.store.tasks_by_url(url),
                    'events': self.store.events(url=url)}

    def dispatch_log(self, origin=None):
        with self.lock:
            return self.store.events(origin=origin, kinds=('dispatch',))

    def origin_status(self):
        with self.lock:
            return {origin: {
                'requests_used': runtime.requests_used,
                'budget': self.cfg.origins.get(origin) and self.cfg.origins[origin].budget,
                'backoff_until': runtime.backoff_until,
                'robots_state': runtime.robots_state,
                'robots_version': runtime.robots_version,
                'robots_expires_at': runtime.robots_expires_at,
                'pending': self.store.pending_count(origin),
                'inflight': self._inflight_origin(origin),
            } for origin, runtime in self._origins.items()}
