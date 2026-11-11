from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from nse_ingest import retry
from nse_ingest.manifest import Manifest
from nse_ingest.records import to_records
from nse_ingest.windows import Window

W = Window(date(2024, 1, 1), date(2024, 3, 31))
SHARED = "https://nsearchives.nseindia.com/corporate/X_14042021234810_D3STXdisclosurefinal.pdf"


def rows() -> list[dict]:
    base = {"an_dt": "14-Apr-2021 23:48:11", "dt": "14042021234811", "symbol": "X", "attchmntText": "t"}
    return [
        {**base, "seq_id": "1", "desc": "Demerger", "attchmntFile": SHARED},
        {**base, "seq_id": "2", "desc": "Amalgamation/Merger", "attchmntFile": SHARED},
        {**base, "seq_id": "3", "desc": "News Verification", "attchmntFile": "-"},
        {**base, "seq_id": "4", "desc": "News Verification", "attchmntFile": "-"},
        {**base, "seq_id": "1", "desc": "Demerger", "attchmntFile": SHARED},  # NSE returns exact dupes
    ]


@pytest.fixture
def m(tmp_path: Path) -> Manifest:
    with Manifest(tmp_path / "manifest.db") as man:
        yield man


def test_shared_url_and_text_only_filings_are_all_kept(m: Manifest) -> None:
    res = m.record_window("X", W, "announcement", to_records("announcement", rows(), "X"))
    assert (res.rows, res.new_filings, res.new_files) == (5, 4, 1)
    assert m.filing_counts() == {"announcement": 4}
    assert m.file_status_counts() == {"discovered": 1}


def test_reharvest_is_idempotent(m: Manifest) -> None:
    recs = to_records("announcement", rows(), "X")
    m.record_window("X", W, "announcement", recs)
    again = m.record_window("X", W, "announcement", recs)
    assert (again.new_filings, again.new_files) == (0, 0)
    assert m.filing_counts() == {"announcement": 4}
    assert sum(m.file_status_counts().values()) == 1


def test_reharvest_does_not_reset_download_state(m: Manifest) -> None:
    recs = to_records("announcement", rows(), "X")
    m.record_window("X", W, "announcement", recs)
    task = m.claim_next()
    m.mark_downloaded(task.source_url, "ab" * 32, "pdf", 10, None)
    m.record_window("X", W, "announcement", recs)
    assert m.file_status_counts() == {"downloaded": 1}


def test_window_done_requires_settling(m: Manifest) -> None:
    assert not m.is_window_done("X", W, "announcement", 1)
    m.record_window("X", W, "announcement", [], now=datetime(2024, 3, 31, 12, tzinfo=timezone.utc))
    assert not m.is_window_done("X", W, "announcement", 1)  # fetched before window closed
    m.record_window("X", W, "announcement", [], now=datetime(2024, 4, 2, tzinfo=timezone.utc))
    assert m.is_window_done("X", W, "announcement", 1)
    m.record_window_failure("X", W, "board_meeting", "boom")
    assert not m.is_window_done("X", W, "board_meeting", 1)


def test_claim_failure_retry_and_crash_recovery(m: Manifest) -> None:
    m.record_window("X", W, "announcement", to_records("announcement", rows(), "X"))
    t = m.claim_next()
    assert t is not None and m.claim_next() is None
    now = datetime.now(timezone.utc)
    m.mark_failure(t.source_url, retry.RetryDecision(retry.FAILED, 1, now + timedelta(hours=1)), "x")
    assert m.claim_next(now) is None
    assert m.claim_next(now + timedelta(hours=2)).attempts == 1
    assert m.reset_in_flight() == 1
    assert m.file_status_counts() == {"discovered": 1}
