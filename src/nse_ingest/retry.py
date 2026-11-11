"""Retry / backoff state machine for file downloads (pure).

States: discovered -> downloading -> downloaded
                              \\-> failed (next_retry_at set) -> downloading ...
                              \\-> dead   (after max_attempts, or permanent 404)
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable

from .config import RetryConfig

DISCOVERED = "discovered"
DOWNLOADING = "downloading"
DOWNLOADED = "downloaded"
FAILED = "failed"
DEAD = "dead"
ALL_STATUSES = (DISCOVERED, DOWNLOADING, DOWNLOADED, FAILED, DEAD)


class FailureKind(Enum):
    TRANSIENT = "transient"          # timeouts, 5xx, 429, truncated body
    BLOCKED = "blocked"              # 401/403/stream reset that survived a session refresh
    NOT_FOUND = "not_found"          # 404 on the archive host: permanent
    INVALID_CONTENT = "invalid_content"  # e.g. HTML error page where a PDF was expected


@dataclass(frozen=True)
class RetryDecision:
    status: str
    attempts: int
    next_retry_at: datetime | None


def backoff_seconds(attempt: int, base_s: float, max_s: float, jitter_fraction: float, rand: float) -> float:
    """Exponential backoff with symmetric jitter. `attempt` is 1-based; `rand` in [0, 1)."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    raw = min(max_s, base_s * (2 ** (attempt - 1)))
    jitter = raw * jitter_fraction * (2 * rand - 1)
    return max(0.0, raw + jitter)


def on_failure(prev_attempts: int, kind: FailureKind, now: datetime, cfg: RetryConfig, rand: float) -> RetryDecision:
    attempts = prev_attempts + 1
    limit = cfg.blocked_max_attempts if kind is FailureKind.BLOCKED else cfg.max_attempts
    if kind is FailureKind.NOT_FOUND or attempts >= limit:
        return RetryDecision(DEAD, attempts, None)
    delay = backoff_seconds(attempts, cfg.base_delay_s, cfg.max_delay_s, cfg.jitter_fraction, rand)
    if kind is FailureKind.BLOCKED:
        delay = max(delay, cfg.blocked_cooldown_s)
    return RetryDecision(FAILED, attempts, now + timedelta(seconds=delay))


class BlockTracker:
    """Escalating global pause while NSE blocks us, with a give-up deadline.

    Consecutive blocks pause base, 2x base, ... capped at max. Any success resets the streak.
    Once a streak has lasted `give_up_s` of wall-clock time without a single success, every
    retry is spent: on_block() returns None and the caller should stop its stage.
    """

    def __init__(self, base_s: float, max_s: float, give_up_s: float,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._base = base_s
        self._max = max_s
        self._give_up = give_up_s
        self._clock = clock
        self._since: float | None = None
        self.consecutive = 0

    @classmethod
    def from_config(cls, cfg: RetryConfig) -> BlockTracker:
        return cls(cfg.blocked_cooldown_s, cfg.blocked_cooldown_max_s, cfg.blocked_give_up_s)

    @property
    def blocked_for_s(self) -> float:
        return 0.0 if self._since is None else self._clock() - self._since

    def on_block(self) -> float | None:
        if self._since is None:
            self._since = self._clock()
        if self.blocked_for_s >= self._give_up:
            return None
        self.consecutive += 1
        return min(self._max, self._base * (2 ** (self.consecutive - 1)))

    def on_success(self) -> None:
        self._since = None
        self.consecutive = 0
