"""nse-ingest CLI: harvest | download | status | retry-failed | verify."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import traceback
from dataclasses import replace
from datetime import date
from pathlib import Path

from . import retry
from .config import DEFAULT_DATA_DIR, Config, DownloadConfig, HttpConfig, Paths
from .constituents import load_symbols
from .download import Downloader, content_path, sha256_file
from .harvest import HarvestAborted, harvest
from .logs import event, setup_logging
from .manifest import Manifest
from .nse_client import NseClient
from .records import DOC_TYPES
from .windows import calendar_windows, years_back

# Exit codes (scripts/backfill.py relies on these).
# 0 ok | 1 some harvest windows failed | 3 NSE blocked through every retry | 4 crashed | 130 interrupted
EXIT_ABORTED_BLOCKED = 3
EXIT_CRASHED = 4
EXIT_INTERRUPTED = 130


def _date(s: str) -> date:
    return date.fromisoformat(s)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="nse-ingest", description=__doc__)
    p.add_argument("--data-dir", type=Path,
                   default=Path(os.environ.get("NSE_INGEST_DATA_DIR", str(DEFAULT_DATA_DIR))))
    p.add_argument("--rps", type=float, default=HttpConfig.max_requests_per_second,
                   help="max sustained requests/sec to NSE (default %(default)s)")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    h = sub.add_parser("harvest", help="Stage A: list filings into the manifest")
    h.add_argument("--symbols", nargs="+", help="default: Nifty 50 from the latest constituents snapshot")
    h.add_argument("--refresh-constituents", action="store_true", help="fetch and snapshot today's Nifty 50 list")
    h.add_argument("--from", dest="from_date", type=_date, help="YYYY-MM-DD (default: 3 years before --to)")
    h.add_argument("--to", dest="to_date", type=_date, help="YYYY-MM-DD (default: today)")
    h.add_argument("--doc-type", nargs="+", choices=DOC_TYPES, default=list(DOC_TYPES))

    d = sub.add_parser("download", help="Stage B: fetch queued files")
    d.add_argument("--limit", type=int, help="stop after this many files")
    d.add_argument("--concurrency", type=int, default=DownloadConfig.concurrency)
    d.add_argument("--no-wait", action="store_true", help="exit instead of waiting for scheduled retries")

    sub.add_parser("status", help="counts by status, failures, disk usage")
    r = sub.add_parser("retry-failed", help="requeue failed (and dead) files with attempts reset")
    r.add_argument("--failed-only", action="store_true", help="leave dead rows alone")
    v = sub.add_parser("verify", help="re-hash files on disk vs manifest, find orphans")
    v.add_argument("--requeue", action="store_true", help="requeue rows whose file is missing or corrupt")
    return p


def _config(args: argparse.Namespace) -> Config:
    base = Config()
    return replace(base, http=replace(base.http, max_requests_per_second=args.rps), paths=Paths(args.data_dir))


async def _cmd_harvest(args: argparse.Namespace, cfg: Config, m: Manifest, log) -> int:
    today = date.today()
    end = args.to_date or today
    start = args.from_date or years_back(end, cfg.harvest.backfill_years)
    async with NseClient(cfg.http) as client:
        if args.symbols:
            symbols = [s.upper() for s in args.symbols]
        else:
            symbols, snap = await load_symbols(client, cfg.paths.constituents_dir, today, args.refresh_constituents)
            event(log, "harvest.constituents", snapshot=str(snap), count=len(symbols))
        windows = calendar_windows(start, end, cfg.harvest.window_months)
        event(log, "harvest.start", symbols=len(symbols), windows=len(windows), doc_types=",".join(args.doc_type),
              start=str(start), end=str(end))
        try:
            s = await harvest(m, client, cfg, symbols, windows, args.doc_type, log)
        except HarvestAborted as e:
            event(log, "harvest.aborted", logging.ERROR, reason=str(e))
            print(f"ABORTED: {e}. Re-run harvest later; completed windows are kept.", file=sys.stderr)
            return EXIT_ABORTED_BLOCKED
    event(log, "harvest.done", fetched=s.windows_fetched, skipped=s.windows_skipped, failed=s.windows_failed,
          rows=s.rows, new_filings=s.new_filings, new_files=s.new_files)
    for f in s.failures:
        print(f"FAILED {f}", file=sys.stderr)
    return 1 if s.windows_failed else 0


async def _cmd_download(args: argparse.Namespace, cfg: Config, m: Manifest, log) -> int:
    async with NseClient(cfg.http) as client:
        s = await Downloader(m, client, cfg, log).run(args.limit, args.concurrency, not args.no_wait)
    event(log, "download.done", downloaded=s.downloaded, deduped=s.deduped, mb=round(s.bytes / 1e6, 1),
          failed=s.failed, dead=s.dead, aborted=s.aborted)
    if s.aborted:
        print(f"ABORTED: {s.aborted}. Re-run download later; nothing downloaded is lost.", file=sys.stderr)
        return EXIT_ABORTED_BLOCKED
    return 0


def _cmd_status(cfg: Config, m: Manifest) -> int:
    print("filings by doc_type:")
    for k, v in sorted(m.filing_counts().items()):
        print(f"  {k:18s} {v:8d}")
    print("files by status:")
    for k in retry.ALL_STATUSES:
        print(f"  {k:18s} {m.file_status_counts().get(k, 0):8d}")
    n, uniq, b = m.downloaded_stats()
    dedupe = (1 - uniq / n) * 100 if n else 0.0
    print(f"downloaded: {n} urls -> {uniq} unique blobs ({dedupe:.1f}% dedupe), {b / 1e9:.2f} GB fetched")
    disk = sum(p.stat().st_size for p in cfg.paths.raw_dir.rglob("*") if p.is_file())
    print(f"disk usage (raw/): {disk / 1e9:.2f} GB")
    print("harvest windows:", m.harvest_counts())
    hf = m.harvest_failures()
    for r in hf[:10]:
        print(f"  HARVEST FAILED {r['symbol']} {r['doc_type']} {r['window_start']}..{r['window_end']}: {r['last_error']}")
    fails = m.recent_failures()
    if fails:
        print("failures (top 10):")
        for r in fails:
            print(f"  [{r['status']} x{r['attempts']}] {r['source_url']}\n      {r['last_error']}")
    return 0


def _cmd_verify(args: argparse.Namespace, cfg: Config, m: Manifest) -> int:
    expected: set[Path] = set()
    missing = corrupt = 0
    for r in list(m.iter_downloaded()):
        p = content_path(cfg.paths.raw_dir, r["sha256"], r["file_ext"])
        expected.add(p.resolve())
        if not p.exists():
            missing += 1
            print(f"MISSING {p} <- {r['source_url']}")
        elif sha256_file(p) != r["sha256"]:
            corrupt += 1
            print(f"CORRUPT {p} <- {r['source_url']}")
        else:
            continue
        if args.requeue:
            m.requeue(r["source_url"])
    orphans = [p for p in cfg.paths.raw_dir.rglob("*") if p.is_file() and p.resolve() not in expected]
    for p in orphans:
        print(f"ORPHAN {p}")
    print(f"verified {len(expected)} blobs: {missing} missing, {corrupt} corrupt, {len(orphans)} orphans")
    return 1 if (missing or corrupt) else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = _config(args)
    cfg.paths.ensure()
    log = setup_logging(cfg.paths.logs_dir, args.command, args.verbose)
    with Manifest(cfg.paths.manifest_path) as m:
        try:
            if args.command == "harvest":
                return asyncio.run(_cmd_harvest(args, cfg, m, log))
            if args.command == "download":
                return asyncio.run(_cmd_download(args, cfg, m, log))
            if args.command == "status":
                return _cmd_status(cfg, m)
            if args.command == "retry-failed":
                print(f"requeued {m.retry_failed(include_dead=not args.failed_only)} files")
                return 0
            if args.command == "verify":
                return _cmd_verify(args, cfg, m)
        except KeyboardInterrupt:
            event(log, "interrupted", command=args.command)
            return EXIT_INTERRUPTED
        except Exception as e:  # make crashes explicit in the JSON log, not just a console traceback
            event(log, "crashed", logging.CRITICAL, command=args.command, error=f"{type(e).__name__}: {e}",
                  traceback=traceback.format_exc())
            return EXIT_CRASHED
    return 2


if __name__ == "__main__":
    sys.exit(main())
