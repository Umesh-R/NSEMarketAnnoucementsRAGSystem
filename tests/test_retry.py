from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from nse_ingest.config import RetryConfig
from nse_ingest.retry import DEAD, FAILED, FailureKind, backoff_seconds, on_failure

NOW = datetime(2026, 11, 9, 20, 0, tzinfo=timezone.utc)
CFG = RetryConfig(max_attempts=5, base_delay_s=30, max_delay_s=3600, jitter_fraction=0.25)


def test_backoff_doubles_without_jitter() -> None:
    assert [backoff_seconds(a, 30, 3600, 0.25, 0.5) for a in (1, 2, 3, 4)] == [30, 60, 120, 240]


def test_backoff_caps_and_jitter_bounds() -> None:
    assert backoff_seconds(20, 30, 3600, 0.25, 0.5) == 3600
    lo, hi = backoff_seconds(2, 30, 3600, 0.25, 0.0), backoff_seconds(2, 30, 3600, 0.25, 0.999999)
    assert lo == pytest.approx(45) and hi == pytest.approx(75, rel=1e-4)


def test_backoff_rejects_zero_attempt() -> None:
    with pytest.raises(ValueError):
        backoff_seconds(0, 30, 3600, 0.25, 0.5)


def test_transient_failures_schedule_retry_then_die() -> None:
    attempts = 0
    for expected_delay in (30, 60, 120, 240):
        d = on_failure(attempts, FailureKind.TRANSIENT, NOW, CFG, 0.5)
        assert d.status == FAILED and d.attempts == attempts + 1
        assert d.next_retry_at == NOW + timedelta(seconds=expected_delay)
        attempts = d.attempts
    d = on_failure(attempts, FailureKind.TRANSIENT, NOW, CFG, 0.5)
    assert (d.status, d.attempts, d.next_retry_at) == (DEAD, 5, None)


def test_not_found_is_immediately_dead() -> None:
    d = on_failure(0, FailureKind.NOT_FOUND, NOW, CFG, 0.5)
    assert (d.status, d.attempts) == (DEAD, 1)


@pytest.mark.parametrize("kind", [FailureKind.BLOCKED, FailureKind.INVALID_CONTENT])
def test_other_kinds_retry(kind: FailureKind) -> None:
    assert on_failure(0, kind, NOW, CFG, 0.5).status == FAILED


def test_blocked_gets_larger_budget_and_min_cooldown() -> None:
    cfg = RetryConfig(max_attempts=5, base_delay_s=30, blocked_cooldown_s=300, blocked_max_attempts=12)
    d = on_failure(5, FailureKind.BLOCKED, NOW, cfg, 0.5)  # past max_attempts, still under blocked budget
    assert d.status == FAILED and d.next_retry_at >= NOW + timedelta(seconds=300)
    assert on_failure(11, FailureKind.BLOCKED, NOW, cfg, 0.5).status == DEAD


def test_block_tracker_escalates_caps_resets_and_gives_up() -> None:
    from nse_ingest.retry import BlockTracker
    now = [0.0]
    t = BlockTracker(300, 3600, 3 * 3600, clock=lambda: now[0])
    pauses = []
    for _ in range(6):
        pauses.append(t.on_block())
        now[0] += pauses[-1] + 90  # pause, then a request that times out after 90 s
    assert pauses == [300, 600, 1200, 2400, 3600, 3600]
    assert t.on_block() is None  # ~3.4 h of continuous blocking: every retry spent
    t.on_success()
    assert t.on_block() == 300 and t.blocked_for_s == 0  # a success starts a fresh streak


def test_intermittent_blocks_never_give_up() -> None:
    from nse_ingest.retry import BlockTracker
    now = [0.0]
    t = BlockTracker(300, 3600, 3 * 3600, clock=lambda: now[0])
    for _ in range(20):  # hourly ~50 min blocks with successes in between, for 20 hours
        assert t.on_block() is not None
        now[0] += 3000
        t.on_success()
        now[0] += 600
