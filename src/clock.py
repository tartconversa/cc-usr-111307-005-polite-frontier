"""时钟抽象：调度器只通过时钟读取时间，测试用可推进时钟复现退避场景。"""
from __future__ import annotations
import time


class ManualClock:
    """只能显式推进的时钟；不推进就静止，用于确定性地复现退避与恢复。"""

    def __init__(self, start: float = 1_700_000_000.0):
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError('时钟不能倒退')
        self._now += seconds

    def advance_to(self, moment: float) -> None:
        if moment < self._now - 1e-9:
            raise ValueError('时钟不能倒退')
        self._now = float(moment)


class SystemClock:
    """真实时钟（epoch 秒），用于演示与实际运行。"""

    def now(self) -> float:
        return time.time()
