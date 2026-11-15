"""robots.txt 解析与匹配。

支持 Allow/Disallow、* 通配、结尾 $ 锚定、最长匹配，等长时 Allow 胜。
按指定 User-Agent 选择规则组：精确匹配产品名优先，否则用 * 组。
"""
from __future__ import annotations
import re


class Rule:
    __slots__ = ('allow', 'pattern', 'regex')

    def __init__(self, allow: bool, pattern: str):
        self.allow = allow
        self.pattern = pattern
        rx = re.escape(pattern).replace(r'\*', '.*')
        if rx.endswith(r'\$'):
            rx = rx[:-2] + '$'
        self.regex = re.compile(rx)

    def matches(self, path_query: str) -> bool:
        return self.regex.match(path_query) is not None

    def __repr__(self):
        return 'Rule(%s, %r)' % ('Allow' if self.allow else 'Disallow', self.pattern)


class RobotsRules:
    """一个 robots.txt 的解析结果：若干 (user-agent 列表, 规则列表) 组。"""

    def __init__(self, groups):
        self.groups = groups  # list[(agents: list[str], rules: list[Rule])]

    @classmethod
    def allow_all(cls) -> 'RobotsRules':
        return cls([])

    @classmethod
    def parse(cls, text: str) -> 'RobotsRules':
        groups = []
        agents: list[str] = []
        rules: list[Rule] = []
        seen_rule = False

        def flush():
            nonlocal agents, rules, seen_rule
            if agents:
                groups.append((agents, rules))
            agents, rules, seen_rule = [], [], False

        for raw in text.splitlines():
            line = raw.split('#', 1)[0].strip()
            if not line or ':' not in line:
                continue
            field, _, value = line.partition(':')
            field, value = field.strip().lower(), value.strip()
            if field == 'user-agent':
                if seen_rule:
                    flush()
                if value:
                    agents.append(value.lower())
            elif field in ('allow', 'disallow'):
                if not agents or not value:
                    continue  # 空 Disallow 表示允许，忽略
                seen_rule = True
                rules.append(Rule(field == 'allow', value))
            # 其他字段（sitemap、crawl-delay 等）忽略
        flush()
        return cls(groups)

    def check(self, ua_token: str, path_query: str):
        """返回 (是否允许, 命中的规则或 None)。最长匹配，等长 Allow 胜。"""
        ua_token = (ua_token or '').lower()
        exact = [g for g in self.groups if ua_token in g[0]]
        chosen = exact if exact else [g for g in self.groups if '*' in g[0]]
        best = None
        for _, rules in chosen:
            for rule in rules:
                if not rule.matches(path_query):
                    continue
                if (best is None
                        or len(rule.pattern) > len(best.pattern)
                        or (len(rule.pattern) == len(best.pattern) and rule.allow)):
                    best = rule
        if best is None:
            return True, None
        return best.allow, best


def ua_token_of(user_agent: str) -> str:
    """从完整 User-Agent 取产品名（/ 之前）用于规则组匹配。"""
    return user_agent.split('/', 1)[0].strip().lower()
