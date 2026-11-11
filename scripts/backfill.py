"""Ops wrapper for the long-running backfill: start it detached, stop it, report its health.

Run with the venv's Python (paths resolve from this file, so any working directory works):

    python scripts/backfill.py start  [--rps 1.0] [--skip-harvest] [--reason TEXT] [--force]
    python scripts/backfill.py stop   [--hold REASON] [--reason TEXT]
    python scripts/backfill.py health                  # one JSON document with a verdict

`start` runs `nse-ingest harvest` and then `nse-ingest download` in one detached background
process. Download is skipped when harvest aborted (NSE blocked through every retry), crashed or
was interrupted. `stop --hold` also writes a HOLD marker, and `start` then refuses until a human
passes `--force`. The hourly monitor agent (.claude/agents/backfill-monitor.md) drives these.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = REPO / "annoucement docs"
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0

# nse-ingest exit codes (src/nse_ingest/cli.py)
RC_OK, RC_WINDOWS_FAILED = 0, 1

MAX_RPS = 2.0
STALL_MIN = 60        # running, no success and no blocking logged for this long: probably hung
GIVE_UP_MIN = 240     # safety net only; the job itself stops after ~3.4 h of continuous blocking
RESTART_WINDOW_H = 6
RESTART_LIMIT = 3     # automatic restarts per window before a human has to look
AUTO_RESTART_PREFIXES = ("restart", "gap-fill")
HISTORY_MIN_GAP_MIN = 10
CONSOLE_TAIL_LINES = 15
TERMINAL_STAGES = {"finished", "stopped"}
PID_KEYS = ("runner_pid", "launcher_pid", "child_pid")
SUCCESS_EVENTS = {"harvest.window", "download.ok"}
BLOCK_EVENTS = {"harvest.blocked", "download.blocked_pause"}
ABORT_EVENTS = {"harvest.aborted", "download.aborted"}


class Files:
    def __init__(self, data_dir: Path) -> None:
        self.data = data_dir
        self.logs = data_dir / "logs"
        self.manifest = data_dir / "manifest.db"
        self.state = self.logs / "backfill.json"
        self.hold = self.logs / "HOLD.json"
        self.actions = self.logs / "backfill-actions.jsonl"
        self.history = self.logs / "health-history.jsonl"
        self.console = self.logs / "backfill-console.txt"


# ---- small utilities ----------------------------------------------------------------------------
def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def parse_ts(s: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(s) if s else None
    except (TypeError, ValueError):
        return None


def read_json(p: Path) -> dict[str, Any] | None:
    for _ in range(10):  # Windows: the writer may be mid-replace
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (PermissionError, json.JSONDecodeError):
            time.sleep(0.2)
    raise RuntimeError(f"cannot read {p}")


def write_json(p: Path, data: dict[str, Any]) -> None:
    tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    for _ in range(10):
        try:
            os.replace(tmp, p)
            return
        except PermissionError:
            time.sleep(0.2)
    os.replace(tmp, p)


def update_state(f: Files, **fields: Any) -> None:
    state = read_json(f.state) or {}
    state.update(fields)
    write_json(f.state, state)


def append_jsonl(p: Path, rec: dict[str, Any]) -> None:
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, default=str) + "\n")


def read_jsonl(p: Path) -> list[dict[str, Any]]:
    if not p.exists():
        return []
    out = []
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def log_action(f: Files, action: str, **fields: Any) -> None:
    append_jsonl(f.actions, {"ts": iso(now()), "action": action, **fields})


# ---- processes -----------------------------------------------------------------------------------
def pid_alive(pid: Any) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                             capture_output=True, text=True, creationflags=NO_WINDOW).stdout
        return f'"{pid}"' in out and "python" in out.lower()  # guards against PID reuse
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def kill_tree(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, creationflags=NO_WINDOW)
        return
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        pass


def spawn_detached(cmd: list[str], out: Any) -> int:
    kw: dict[str, Any] = {"stdin": subprocess.DEVNULL, "stdout": out, "stderr": subprocess.STDOUT,
                          "cwd": REPO, "close_fds": True}
    if os.name != "nt":
        return subprocess.Popen(cmd, start_new_session=True, **kw).pid
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    try:  # outlive the shell / IDE session that launched us, when its job object allows it
        return subprocess.Popen(cmd, creationflags=flags | subprocess.CREATE_BREAKAWAY_FROM_JOB, **kw).pid
    except OSError:
        return subprocess.Popen(cmd, creationflags=flags, **kw).pid


def is_running(state: dict[str, Any]) -> bool:
    return state.get("stage") not in TERMINAL_STAGES and any(pid_alive(state.get(k)) for k in PID_KEYS)


# ---- commands ------------------------------------------------------------------------------------
def cmd_start(f: Files, args: argparse.Namespace) -> int:
    if not 0 < args.rps <= MAX_RPS:
        print(f"REFUSED: --rps must be in (0, {MAX_RPS}]")
        return 2
    try:
        import nse_ingest  # noqa: F401
    except ImportError:
        print("REFUSED: nse_ingest is not importable; run this script with the project's .venv python")
        return 2
    f.logs.mkdir(parents=True, exist_ok=True)
    hold = read_json(f.hold)
    if hold and not args.force:
        print(f"REFUSED: on hold since {hold.get('at')}: {hold.get('reason')}. Only a human should pass --force.")
        return 2
    state = read_json(f.state) or {}
    if is_running(state):
        print(f"REFUSED: already running (stage {state.get('stage')}, pids {[state.get(k) for k in PID_KEYS]})")
        return 2
    f.hold.unlink(missing_ok=True)
    write_json(f.state, {"started_at": iso(now()), "rps": args.rps, "skip_harvest": args.skip_harvest,
                         "stage": "launching", "reason": args.reason})
    cmd = [sys.executable, str(Path(__file__).resolve()), "--data-dir", str(f.data), "run", "--rps", str(args.rps)]
    if args.skip_harvest:
        cmd.append("--skip-harvest")
    with f.console.open("ab") as out:
        pid = spawn_detached(cmd, out)
    update_state(f, launcher_pid=pid)
    log_action(f, "start", pid=pid, rps=args.rps, skip_harvest=args.skip_harvest, reason=args.reason,
               forced=args.force)
    print(f"started backfill (pid {pid}); console: {f.console}")
    return 0


def cmd_run(f: Files, args: argparse.Namespace) -> int:
    """Body of the detached job. Started by `start`; not meant to be run by hand."""
    me = os.getpid()
    update_state(f, runner_pid=me, stage="starting")
    rc_h: int | None = None
    rc_d: int | None = None
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    try:
        with f.console.open("ab") as out:
            def stage(command: str) -> int:
                cmd = [sys.executable, "-m", "nse_ingest.cli", "--data-dir", str(f.data), "--rps", str(args.rps),
                       command]
                p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, cwd=REPO,
                                     env=env, creationflags=NO_WINDOW)
                update_state(f, runner_pid=me, stage=command, child_pid=p.pid)
                return p.wait()

            if not args.skip_harvest:
                rc_h = stage("harvest")
            if rc_h in (None, RC_OK, RC_WINDOWS_FAILED):
                rc_d = stage("download")
    finally:
        update_state(f, stage="finished", harvest_rc=rc_h, download_rc=rc_d, finished_at=iso(now()), child_pid=None)
        log_action(f, "finished", harvest_rc=rc_h, download_rc=rc_d)
    return 0


def cmd_stop(f: Files, args: argparse.Namespace) -> int:
    state = read_json(f.state) or {}
    killed = []
    for key in PID_KEYS:  # runner first, so it cannot launch the next stage
        pid = state.get(key)
        if pid_alive(pid):
            kill_tree(pid)
            killed.append(pid)
    deadline = time.monotonic() + 15
    while any(pid_alive(p) for p in killed) and time.monotonic() < deadline:
        time.sleep(0.5)
    still_alive = [p for p in killed if pid_alive(p)]
    if args.hold:
        f.logs.mkdir(parents=True, exist_ok=True)
        write_json(f.hold, {"reason": args.hold, "at": iso(now())})
    if state:
        update_state(f, stage="stopped", stopped_at=iso(now()), stop_reason=args.hold or args.reason)
    log_action(f, "stop", killed=killed, still_alive=still_alive, hold=args.hold, reason=args.reason)
    print(json.dumps({"killed": killed, "still_alive": still_alive, "hold": bool(args.hold)}))
    return 1 if still_alive else 0


def load_events(logs: Path, since: datetime) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in sorted(logs.glob("*.jsonl")):
        if not p.name.startswith(("harvest-", "download-")) or p.stat().st_mtime < since.timestamp():
            continue
        with p.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    e = json.loads(line)
                    ts = datetime.fromisoformat(e["ts"])
                except (ValueError, KeyError, TypeError):
                    continue
                if ts >= since:
                    e["_ts"] = ts
                    out.append(e)
    out.sort(key=lambda e: e["_ts"])
    return out


def manifest_stats(db: Path) -> dict[str, Any]:
    if not db.exists():
        return {}
    con = sqlite3.connect(str(db), timeout=30)
    try:
        con.execute("PRAGMA query_only=ON")

        def q(sql: str) -> list[tuple]:
            return con.execute(sql).fetchall()

        return {
            "files": dict(q("SELECT status, COUNT(*) FROM files GROUP BY status")),
            "dead_by_kind": dict(q("SELECT CASE WHEN last_error LIKE 'NotFoundError%' THEN 'not_found' ELSE 'other' END,"
                                   " COUNT(*) FROM files WHERE status='dead' GROUP BY 1")),
            "downloaded_gb": round(q("SELECT COALESCE(SUM(byte_size),0) FROM files WHERE status='downloaded'")[0][0]
                                   / 1e9, 2),
            "filings": q("SELECT COUNT(*) FROM filings")[0][0],
            "harvest_windows": dict(q("SELECT status, COUNT(*) FROM harvest_log GROUP BY status")),
            "failed_windows_sample": [
                dict(zip(("symbol", "doc_type", "window_start", "error"), r))
                for r in q("SELECT symbol, doc_type, window_start, last_error FROM harvest_log"
                           " WHERE status='failed' LIMIT 5")],
        }
    finally:
        con.close()


def console_tail(p: Path, lines: int) -> list[str]:
    if not p.exists():
        return []
    with p.open("rb") as fh:
        fh.seek(max(0, p.stat().st_size - 20_000))
        return fh.read().decode("utf-8", errors="replace").splitlines()[-lines:]


def cmd_health(f: Files, args: argparse.Namespace) -> int:
    t = now()
    state = read_json(f.state) or {}
    hold = read_json(f.hold)
    running = is_running(state)
    stage = state.get("stage")
    started = parse_ts(state.get("started_at"))
    events = load_events(f.logs, started or t - timedelta(days=1)) if f.logs.exists() else []
    m = manifest_stats(f.manifest)
    files, windows, dead = m.get("files", {}), m.get("harvest_windows", {}), m.get("dead_by_kind", {})

    successes = [e["_ts"] for e in events if e["event"] in SUCCESS_EVENTS]
    last_success = max(successes) if successes else None
    ref = max((d for d in (last_success, started) if d), default=None)
    mins_since_success = round((t - ref).total_seconds() / 60) if ref else None
    blocks_since = [e for e in events if e["event"] in BLOCK_EVENTS and (ref is None or e["_ts"] >= ref)]
    aborts = [e for e in events if e["event"] in ABORT_EVENTS]
    crashes = [e for e in events if e["event"] == "crashed"]
    hour = [e for e in events if e["_ts"] >= t - timedelta(hours=1)]
    last_hour = {
        "successes": sum(e["event"] in SUCCESS_EVENTS for e in hour),
        "block_pauses": sum(e["event"] in BLOCK_EVENTS for e in hour),
        "window_failures": sum(e["event"] == "harvest.window_failed" for e in hour),
        "download_failures_by_kind": dict(Counter(e.get("kind") for e in hour if e["event"] == "download.failed")),
    }
    actions = read_jsonl(f.actions)
    # Only automatic restarts count against the budget; the monitor tags them "restart ..." / "gap-fill".
    restarts = sum(1 for a in actions if a.get("action") == "start"
                   and str(a.get("reason", "")).startswith(AUTO_RESTART_PREFIXES)
                   and (parse_ts(a.get("ts")) or t) >= t - timedelta(hours=RESTART_WINDOW_H))
    gap_fill_attempted = any(a.get("action") == "start" and "gap-fill" in str(a.get("reason")) for a in actions)
    queued = files.get("discovered", 0) + files.get("failed", 0) + files.get("downloading", 0)

    reasons: list[str] = []
    if hold:
        verdict = "HELD"
        reasons.append(f"on hold since {hold.get('at')}: {hold.get('reason')}")
    elif not state:
        verdict = "NOT_STARTED"
    elif aborts:
        verdict = "BLOCKED_GAVE_UP"
        reasons += [f"{e['event']} at {e['ts']}: {e.get('reason')}" for e in aborts]
    elif running and blocks_since and mins_since_success is not None and mins_since_success >= GIVE_UP_MIN:
        verdict = "BLOCKED_GAVE_UP"
        reasons.append(f"no successful request for {mins_since_success} min while NSE kept blocking")
    elif running and blocks_since:
        verdict = "BLOCKED_ESCALATING"
        last = blocks_since[-1]
        reasons.append(f"block streak: {len(blocks_since)} pauses since the last success {mins_since_success} min "
                       f"ago; latest pause {last.get('pause_s')} s at {last.get('ts')}")
    elif running and mins_since_success is not None and mins_since_success >= STALL_MIN:
        verdict = "STALLED"
        reasons.append(f"no successful request for {mins_since_success} min and no blocking logged")
    elif running:
        verdict = "OK"
    elif stage == "finished" and state.get("download_rc") == RC_OK:
        gaps = windows.get("failed", 0) + dead.get("other", 0) + queued
        verdict = "FINISHED_WITH_GAPS" if gaps else "FINISHED"
        if gaps:
            reasons.append(f"{windows.get('failed', 0)} failed harvest windows, {dead.get('other', 0)} dead files "
                           f"(not 404), {queued} still queued")
    elif stage == "stopped":
        verdict = "STOPPED"
        reasons.append(f"stopped at {state.get('stopped_at')}: {state.get('stop_reason')}")
    else:
        verdict = "CRASHED"
        reasons.append(f"not running; stage={stage} harvest_rc={state.get('harvest_rc')} "
                       f"download_rc={state.get('download_rc')}")
        reasons += [f"crashed event ({e.get('command')}) at {e['ts']}: {e.get('error')}" for e in crashes[-2:]]

    history = read_jsonl(f.history)
    prev = history[-1] if history else None
    snap = {"ts": iso(t), "verdict": verdict, "windows_done": windows.get("done", 0),
            "files_downloaded": files.get("downloaded", 0)}
    progress = None
    if prev and parse_ts(prev.get("ts")):
        progress = {"minutes": round((t - parse_ts(prev["ts"])).total_seconds() / 60),
                    "windows_done": snap["windows_done"] - prev.get("windows_done", 0),
                    "files_downloaded": snap["files_downloaded"] - prev.get("files_downloaded", 0)}
    if f.logs.exists() and (progress is None or progress["minutes"] >= HISTORY_MIN_GAP_MIN):
        append_jsonl(f.history, snap)

    report = {
        "verdict": verdict,
        "reasons": reasons,
        "checked_at": iso(t),
        "running": running,
        "stage": stage,
        "started_at": state.get("started_at"),
        "rps": state.get("rps"),
        "exit_codes": {"harvest": state.get("harvest_rc"), "download": state.get("download_rc")},
        "last_success_at": iso(last_success) if last_success else None,
        "minutes_since_success": mins_since_success,
        "last_hour": last_hour,
        "progress_since_last_check": progress,
        "manifest": m,
        "restarts_last_6h": restarts,
        "restart_budget_left": max(0, RESTART_LIMIT - restarts),
        "gap_fill_attempted": gap_fill_attempted,
        "disk_free_gb": round(shutil.disk_usage(f.data if f.data.exists() else REPO).free / 1e9, 1),
        "console_tail": console_tail(f.console, CONSOLE_TAIL_LINES),
    }
    print(json.dumps(report, indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="backfill", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", type=Path, default=Path(os.environ.get("NSE_INGEST_DATA_DIR", DEFAULT_DATA_DIR)))
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start", help="launch harvest -> download in the background")
    s.add_argument("--rps", type=float, default=1.0)
    s.add_argument("--skip-harvest", action="store_true")
    s.add_argument("--reason", default="manual")
    s.add_argument("--force", action="store_true", help="clear a HOLD (humans only)")
    st = sub.add_parser("stop", help="kill the whole job")
    st.add_argument("--hold", metavar="REASON", help="also block restarts until `start --force`")
    st.add_argument("--reason", default="manual")
    sub.add_parser("health", help="print a JSON health report with a verdict")
    r = sub.add_parser("run", help="internal: body of the detached job")
    r.add_argument("--rps", type=float, required=True)
    r.add_argument("--skip-harvest", action="store_true")
    args = p.parse_args(argv)
    f = Files(args.data_dir.resolve())
    return {"start": cmd_start, "stop": cmd_stop, "health": cmd_health, "run": cmd_run}[args.cmd](f, args)


if __name__ == "__main__":
    sys.exit(main())
