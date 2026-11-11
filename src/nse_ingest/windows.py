"""Harvest window generation (pure)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True, order=True)
class Window:
    start: date
    end: date  # inclusive, matching NSE's to_date semantics


def _add_months(d: date, months: int) -> date:
    total = d.year * 12 + (d.month - 1) + months
    return date(total // 12, total % 12 + 1, 1)


def calendar_windows(start: date, end: date, months: int = 3) -> list[Window]:
    """Calendar-aligned windows of `months` months covering [start, end], clipped at both ends.

    Alignment (Jan/Apr/Jul/Oct for quarters) keeps window keys stable across runs, so
    harvest_log can recognise already-completed windows.
    """
    if months <= 0 or 12 % months != 0:
        raise ValueError(f"months must divide 12, got {months}")
    if start > end:
        return []
    first_month = ((start.month - 1) // months) * months + 1
    cursor = date(start.year, first_month, 1)
    out: list[Window] = []
    while cursor <= end:
        nxt = _add_months(cursor, months)
        out.append(Window(max(cursor, start), min(nxt - timedelta(days=1), end)))
        cursor = nxt
    return out


def years_back(today: date, years: int) -> date:
    try:
        return today.replace(year=today.year - years)
    except ValueError:  # 29 Feb
        return today.replace(year=today.year - years, day=28)


def is_window_settled(window: Window, fetched_on: date, settle_days: int) -> bool:
    """A window fetched at least `settle_days` after it ended won't gain new filings."""
    return fetched_on >= window.end + timedelta(days=settle_days)
