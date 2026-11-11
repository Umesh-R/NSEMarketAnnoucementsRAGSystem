from __future__ import annotations

from datetime import date

import pytest

from nse_ingest.windows import Window, calendar_windows, is_window_settled, years_back


def test_quarters_are_calendar_aligned_and_clipped() -> None:
    ws = calendar_windows(date(2023, 11, 9), date(2024, 5, 2), 3)
    assert ws == [
        Window(date(2023, 11, 9), date(2023, 12, 31)),
        Window(date(2024, 1, 1), date(2024, 3, 31)),
        Window(date(2024, 4, 1), date(2024, 5, 2)),
    ]


def test_windows_are_contiguous_and_cover_range() -> None:
    ws = calendar_windows(date(2023, 10, 1), date(2026, 11, 9), 3)
    assert ws[0].start == date(2023, 10, 1) and ws[-1].end == date(2026, 11, 9)
    for a, b in zip(ws, ws[1:]):
        assert (b.start - a.end).days == 1
    assert len(ws) == 13


def test_monthly_and_single_day() -> None:
    assert len(calendar_windows(date(2024, 1, 15), date(2024, 12, 31), 1)) == 12
    assert calendar_windows(date(2024, 2, 29), date(2024, 2, 29), 3) == [Window(date(2024, 2, 29), date(2024, 2, 29))]


def test_empty_and_invalid() -> None:
    assert calendar_windows(date(2024, 2, 1), date(2024, 1, 1)) == []
    with pytest.raises(ValueError):
        calendar_windows(date(2024, 1, 1), date(2024, 2, 1), 5)


def test_years_back_leap_day() -> None:
    assert years_back(date(2024, 2, 29), 3) == date(2021, 2, 28)
    assert years_back(date(2026, 11, 9), 3) == date(2023, 11, 9)


def test_settled() -> None:
    w = Window(date(2024, 1, 1), date(2024, 3, 31))
    assert not is_window_settled(w, date(2024, 3, 31), 1)
    assert is_window_settled(w, date(2024, 4, 1), 1)
