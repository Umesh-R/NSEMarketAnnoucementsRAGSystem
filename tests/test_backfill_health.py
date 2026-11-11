"""Verdict logic of `scripts/backfill.py health`, which the hourly monitor agent acts on."""
from __future__ import annotations

import importlib.util
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "backfill", Path(__file__).resolve().parent.parent / "scripts" / "backfill.py")
backfill = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(backfill)

NOW = datetime.now(timezone.utc)
ALIVE = os.getpid()      # this pytest process: a live python process
DEAD = 999_999_999       # no such process


def ago(minutes: float) -> str:
    return (NOW - timedelta(minutes=minutes)).isoformat()


def health(tmp: Path, capsys: pytest.CaptureFixture[str], state: dict | None,
           events: tuple[tuple[float, str, dict], ...] = (), hold: dict | None = None) -> dict:
    logs = tmp / "logs"
    logs.mkdir(parents=True)
    if state is not None:
        (logs / "backfill.json").write_text(json.dumps(state))
    if hold is not None:
        (logs / "HOLD.json").write_text(json.dumps(hold))
    with (logs / "download-20261109.jsonl").open("w") as fh:
        for minutes, name, fields in events:
            fh.write(json.dumps({"ts": ago(minutes), "event": name, **fields}) + "\n")
    backfill.main(["--data-dir", str(tmp), "health"])
    return json.loads(capsys.readouterr().out)


RUNNING = {"started_at": ago(360), "stage": "download", "runner_pid": ALIVE}
OK_EVENT = (5, "download.ok", {})
BLOCK = (10, "download.blocked_pause", {"pause_s": 600})


@pytest.mark.parametrize("state, events, expected", [
    (None, (), "NOT_STARTED"),
    (RUNNING, (OK_EVENT,), "OK"),
    (RUNNING, ((70, "download.ok", {}), BLOCK), "BLOCKED_ESCALATING"),
    (RUNNING, ((300, "download.ok", {}), (200, "download.blocked_pause", {}), BLOCK), "BLOCKED_GAVE_UP"),
    (RUNNING, ((90, "download.ok", {}),), "STALLED"),
    ({**RUNNING, "stage": "finished", "download_rc": 3},
     ((30, "download.aborted", {"reason": "NSE blocked continuously for 3.4 h"}),), "BLOCKED_GAVE_UP"),
    ({**RUNNING, "stage": "finished", "download_rc": 0}, (OK_EVENT,), "FINISHED"),
    ({**RUNNING, "runner_pid": DEAD}, ((20, "crashed", {"command": "download", "error": "KeyError: 'x'"}),),
     "CRASHED"),
    ({**RUNNING, "stage": "stopped", "runner_pid": DEAD}, (), "STOPPED"),
])
def test_verdicts(tmp_path: Path, capsys: pytest.CaptureFixture[str], state: dict | None,
                  events: tuple, expected: str) -> None:
    report = health(tmp_path, capsys, state, events)
    assert report["verdict"] == expected, report["reasons"]


def test_hold_wins_and_crash_reason_is_reported(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    report = health(tmp_path, capsys, {**RUNNING, "runner_pid": DEAD}, ((20, "crashed", {"error": "boom"}),),
                    hold={"reason": "NSE kept timing out", "at": ago(5)})
    assert report["verdict"] == "HELD" and "NSE kept timing out" in report["reasons"][0]


def test_crash_reason_and_restart_budget(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    with (logs / "backfill-actions.jsonl").open("w") as fh:
        for reason in ("setup", "restart after crash: x", "gap-fill"):
            fh.write(json.dumps({"ts": ago(60), "action": "start", "reason": reason}) + "\n")
    (logs / "backfill.json").write_text(json.dumps({**RUNNING, "runner_pid": DEAD}))
    (logs / "download-20261109.jsonl").write_text(
        json.dumps({"ts": ago(20), "event": "crashed", "command": "download", "error": "KeyError: 'x'"}) + "\n")
    backfill.main(["--data-dir", str(tmp_path), "health"])
    report = json.loads(capsys.readouterr().out)
    assert report["verdict"] == "CRASHED" and any("KeyError" in r for r in report["reasons"])
    assert report["restarts_last_6h"] == 2 and report["restart_budget_left"] == 1
