"""SQLite 持久化：任务队列、租约、抓取结果、站点运行状态与准入事件日志。

进程崩溃后重新打开同一个数据库文件即可恢复：claimed 状态的任务退回
pending 等待重新认领；租约令牌（fencing token）保证迟到的旧 worker
无法释放别人的槽位，也无法覆盖较新的请求结果。
"""
from __future__ import annotations
import json
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  origin TEXT NOT NULL,
  url TEXT NOT NULL,              -- 实际请求用的原始 URL（仅去片段）
  url_key TEXT NOT NULL,          -- 规范化判重键
  state TEXT NOT NULL DEFAULT 'pending',  -- pending/claimed/done/rejected/terminated
  detail TEXT,                    -- 最终处理原因
  reason TEXT,                    -- 入队原因: seed/discovered/redirect
  parent INTEGER,                 -- 来源任务
  chain TEXT NOT NULL DEFAULT '[]',  -- 重定向链上各跳的 url_key
  hops INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  wait_reason TEXT,             -- 当前等待原因（可解释）
  lease_owner TEXT,
  lease_token INTEGER,
  lease_expires REAL,
  created_at REAL NOT NULL,
  UNIQUE (origin, url_key)        -- 同一 origin 内 URL 全周期判重
);
CREATE TABLE IF NOT EXISTS results (
  origin TEXT NOT NULL,
  url_key TEXT NOT NULL,
  status INTEGER,
  token INTEGER NOT NULL,         -- 写入结果的租约令牌
  finished_at REAL NOT NULL,
  PRIMARY KEY (origin, url_key)   -- 已访问标记按 origin 隔离
);
CREATE TABLE IF NOT EXISTS origins (
  origin TEXT PRIMARY KEY,
  backoff_until REAL NOT NULL DEFAULT 0,
  requests_used INTEGER NOT NULL DEFAULT 0,
  last_start REAL,
  robots_state TEXT NOT NULL DEFAULT 'unknown',  -- unknown/fresh/deferred
  robots_text TEXT,
  robots_version INTEGER NOT NULL DEFAULT 0,
  robots_fetched_at REAL,
  robots_expires_at REAL,
  robots_retry_at REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  at REAL NOT NULL,
  origin TEXT,
  task_id INTEGER,
  url TEXT,
  kind TEXT NOT NULL,
  detail TEXT                     -- JSON
);
"""

ORIGIN_COLUMNS = frozenset({
    'backoff_until', 'requests_used', 'last_start',
    'robots_state', 'robots_text', 'robots_version',
    'robots_fetched_at', 'robots_expires_at', 'robots_retry_at',
})


class Store:

    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock, self.db:
            self.db.executescript(SCHEMA)

    def close(self):
        with self.lock:
            self.db.close()

    # ---- 恢复与租约 ----

    def recover(self):
        """进程重启：把上个进程认领未完成的任务退回 pending。"""
        with self.lock, self.db:
            rows = [dict(r) for r in self.db.execute(
                "SELECT id, origin, url, lease_token FROM tasks WHERE state='claimed'")]
            self.db.execute(
                "UPDATE tasks SET state='pending', lease_owner=NULL, lease_token=NULL,"
                " lease_expires=NULL WHERE state='claimed'")
            return rows

    def next_token(self) -> int:
        """全局单调递增的租约令牌。"""
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO meta(k, v) VALUES ('next_token', '0')")
            self.db.execute("UPDATE meta SET v = CAST(v AS INTEGER) + 1 WHERE k = 'next_token'")
            return int(self.db.execute("SELECT v FROM meta WHERE k = 'next_token'").fetchone()['v'])

    def claim_task(self, task_id, owner, token, expires, origin, now) -> bool:
        """认领任务，同事务消耗站点预算并记录起始时刻。"""
        with self.lock, self.db:
            cur = self.db.execute(
                "UPDATE tasks SET state='claimed', lease_owner=?, lease_token=?, lease_expires=?,"
                " wait_reason=NULL WHERE id=? AND state='pending'",
                (owner, token, expires, task_id))
            if cur.rowcount != 1:
                return False
            self.db.execute("INSERT OR IGNORE INTO origins(origin) VALUES (?)", (origin,))
            self.db.execute(
                "UPDATE origins SET last_start=?, requests_used=requests_used+1 WHERE origin=?",
                (now, origin))
            return True

    def finish_task(self, task_id, token, state, detail) -> bool:
        """只有持有当前租约令牌的 worker 才能改写任务终态。"""
        with self.lock, self.db:
            cur = self.db.execute(
                "UPDATE tasks SET state=?, detail=?, lease_owner=NULL, lease_token=NULL,"
                " lease_expires=NULL WHERE id=? AND lease_token=? AND state='claimed'",
                (state, detail, task_id, token))
            return cur.rowcount == 1

    def requeue(self, task_id, token) -> bool:
        with self.lock, self.db:
            cur = self.db.execute(
                "UPDATE tasks SET state='pending', lease_owner=NULL, lease_token=NULL,"
                " lease_expires=NULL, attempts=attempts+1"
                " WHERE id=? AND lease_token=? AND state='claimed'", (task_id, token))
            return cur.rowcount == 1

    def reclaim_expired(self, now):
        """回收过期租约：任务退回 pending，由调度器重新认领。"""
        with self.lock, self.db:
            rows = [dict(r) for r in self.db.execute(
                "SELECT id, origin, url, lease_token FROM tasks"
                " WHERE state='claimed' AND lease_expires IS NOT NULL AND lease_expires < ?",
                (now,))]
            self.db.execute(
                "UPDATE tasks SET state='pending', lease_owner=NULL, lease_token=NULL,"
                " lease_expires=NULL WHERE state='claimed'"
                " AND lease_expires IS NOT NULL AND lease_expires < ?", (now,))
            return rows

    def drop_task(self, task_id, state, detail):
        with self.lock, self.db:
            self.db.execute(
                "UPDATE tasks SET state=?, detail=? WHERE id=? AND state='pending'",
                (state, detail, task_id))

    def set_wait(self, task_id, reason):
        with self.lock, self.db:
            self.db.execute(
                "UPDATE tasks SET wait_reason=? WHERE id=? AND state='pending'",
                (reason, task_id))

    # ---- 任务 ----

    def enqueue(self, origin, url, url_key, reason, parent, chain, hops, now):
        """入队；(origin, url_key) 已存在时返回 None 表示判重拒绝。"""
        with self.lock, self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO tasks(origin, url, url_key, state, reason, parent,"
                " chain, hops, created_at) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?)",
                (origin, url, url_key, reason, parent,
                 json.dumps(chain, ensure_ascii=False), hops, now))
            return cur.lastrowid if cur.rowcount else None

    def head_task(self, origin):
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM tasks WHERE origin=? AND state='pending' ORDER BY id LIMIT 1",
                (origin,)).fetchone()
            return dict(row) if row else None

    def pending_heads(self):
        with self.lock:
            return [dict(r) for r in self.db.execute(
                "SELECT origin, MIN(id) AS head FROM tasks WHERE state='pending' GROUP BY origin")]

    def has_pending(self, origin) -> bool:
        with self.lock:
            return self.db.execute(
                "SELECT 1 FROM tasks WHERE origin=? AND state='pending' LIMIT 1",
                (origin,)).fetchone() is not None

    def pending_count(self, origin=None) -> int:
        with self.lock:
            if origin is None:
                row = self.db.execute("SELECT COUNT(*) AS n FROM tasks WHERE state='pending'").fetchone()
            else:
                row = self.db.execute(
                    "SELECT COUNT(*) AS n FROM tasks WHERE state='pending' AND origin=?",
                    (origin,)).fetchone()
            return int(row['n'])

    def task_by_id(self, task_id):
        with self.lock:
            row = self.db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            return dict(row) if row else None

    def tasks_by_url(self, url):
        with self.lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM tasks WHERE url=? ORDER BY id", (url,))]

    def claimed_expiries(self):
        with self.lock:
            return [float(r['lease_expires']) for r in self.db.execute(
                "SELECT lease_expires FROM tasks WHERE state='claimed'"
                " AND lease_expires IS NOT NULL")]

    # ---- 结果 ----

    def save_result(self, origin, url_key, status, token, now):
        """令牌更大的结果才允许覆盖：迟到的旧 worker 写不动较新结果。"""
        with self.lock, self.db:
            self.db.execute(
                "INSERT INTO results(origin, url_key, status, token, finished_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(origin, url_key) DO UPDATE SET status=excluded.status,"
                " token=excluded.token, finished_at=excluded.finished_at"
                " WHERE excluded.token > results.token",
                (origin, url_key, status, token, now))

    def get_result(self, origin, url_key):
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM results WHERE origin=? AND url_key=?",
                (origin, url_key)).fetchone()
            return dict(row) if row else None

    # ---- 站点状态 ----

    def ensure_origin(self, origin):
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO origins(origin) VALUES (?)", (origin,))

    def all_origins(self):
        with self.lock:
            return [dict(r) for r in self.db.execute("SELECT * FROM origins")]

    def update_origin(self, origin, **fields):
        unknown = set(fields) - ORIGIN_COLUMNS
        if unknown:
            raise ValueError('未知站点字段: %s' % sorted(unknown))
        if not fields:
            return
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO origins(origin) VALUES (?)", (origin,))
            sets = ', '.join('%s=?' % name for name in fields)
            self.db.execute('UPDATE origins SET %s WHERE origin=?' % sets,
                            (*fields.values(), origin))

    def bump_origin_counters(self, origin, now):
        """robots 请求认领：消耗预算并记录起始时刻。"""
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO origins(origin) VALUES (?)", (origin,))
            self.db.execute(
                "UPDATE origins SET last_start=?, requests_used=requests_used+1 WHERE origin=?",
                (now, origin))

    # ---- 事件 ----

    def event(self, at, origin, task_id, url, kind, detail=None):
        with self.lock, self.db:
            self.db.execute(
                "INSERT INTO events(at, origin, task_id, url, kind, detail)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (at, origin, task_id, url, kind,
                 json.dumps(detail, ensure_ascii=False) if detail is not None else None))

    def events(self, origin=None, url=None, kinds=None):
        query = "SELECT * FROM events WHERE 1=1"
        args = []
        if origin is not None:
            query += " AND origin=?"
            args.append(origin)
        if url is not None:
            query += " AND url=?"
            args.append(url)
        if kinds:
            query += " AND kind IN (%s)" % ','.join('?' * len(kinds))
            args.extend(kinds)
        query += " ORDER BY seq"
        with self.lock:
            return [self._event(r) for r in self.db.execute(query, args)]

    @staticmethod
    def _event(row):
        item = dict(row)
        if item['detail'] is not None:
            item['detail'] = json.loads(item['detail'])
        return item
