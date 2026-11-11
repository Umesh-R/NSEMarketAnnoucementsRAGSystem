"""Stage A: metadata harvest. Per (symbol, window, doc_type), list filings and record them."""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field

from .config import Config
from .logs import event
from .manifest import Manifest
from .nse_client import BlockedError, NotFoundError, NseClient, NseError
from .records import to_records
from .retry import BlockTracker, backoff_seconds
from .windows import Window


@dataclass
class HarvestSummary:
    windows_fetched: int = 0
    windows_skipped: int = 0
    windows_failed: int = 0
    rows: int = 0
    new_filings: int = 0
    new_files: int = 0
    failures: list[str] = field(default_factory=list)


class HarvestAborted(Exception):
    """NSE stayed blocked through every escalating pause; stop and resume on a later run."""


async def _fetch_window(client: NseClient, cfg: Config, doc_type: str, symbol: str, w: Window,
                        log: logging.Logger, blocks: BlockTracker) -> list[dict]:
    rc = cfg.retry
    attempt = 0
    while True:
        try:
            rows = await client.list_filings(doc_type, symbol, w.start, w.end)
            blocks.on_success()
            return rows
        except NotFoundError:
            raise  # endpoint moved: retrying won't help, surface it
        except BlockedError as e:
            # Not the window's fault: pause everything, don't consume an attempt.
            pause = blocks.on_block()
            if pause is None:
                raise HarvestAborted(f"NSE blocked continuously for {blocks.blocked_for_s / 3600:.1f} h after "
                                     f"{blocks.consecutive} escalating pauses; last error: {e}") from e
            event(log, "harvest.blocked", logging.WARNING, symbol=symbol, doc_type=doc_type, window=str(w.start),
                  pause_s=round(pause), consecutive=blocks.consecutive,
                  blocked_min=round(blocks.blocked_for_s / 60), error=str(e))
            await asyncio.sleep(pause)
        except NseError as e:
            attempt += 1
            if attempt >= rc.max_attempts:
                raise
            delay = backoff_seconds(attempt, rc.harvest_base_delay_s, rc.harvest_max_delay_s, rc.jitter_fraction,
                                    random.random())
            event(log, "harvest.retry", logging.WARNING, symbol=symbol, doc_type=doc_type, window=str(w.start),
                  attempt=attempt, delay_s=round(delay, 1), error=str(e))
            await asyncio.sleep(delay)


async def harvest(manifest: Manifest, client: NseClient, cfg: Config, symbols: list[str], windows: list[Window],
                  doc_types: list[str], log: logging.Logger) -> HarvestSummary:
    s = HarvestSummary()
    blocks = BlockTracker.from_config(cfg.retry)
    total = len(symbols) * len(windows) * len(doc_types)
    done = 0
    for symbol in symbols:
        for w in windows:
            for doc_type in doc_types:
                done += 1
                if manifest.is_window_done(symbol, w, doc_type, cfg.harvest.window_settle_days):
                    s.windows_skipped += 1
                    continue
                try:
                    rows = await _fetch_window(client, cfg, doc_type, symbol, w, log, blocks)
                    result = manifest.record_window(symbol, w, doc_type, to_records(doc_type, rows, symbol))
                except NseError as e:
                    s.windows_failed += 1
                    msg = f"{symbol} {doc_type} {w.start}..{w.end}: {type(e).__name__}: {e}"
                    s.failures.append(msg)
                    manifest.record_window_failure(symbol, w, doc_type, f"{type(e).__name__}: {e}")
                    event(log, "harvest.window_failed", logging.ERROR, symbol=symbol, doc_type=doc_type,
                          window_start=str(w.start), window_end=str(w.end), error=str(e))
                    continue
                s.windows_fetched += 1
                s.rows += result.rows
                s.new_filings += result.new_filings
                s.new_files += result.new_files
                event(log, "harvest.window", logging.DEBUG, symbol=symbol, doc_type=doc_type,
                      window_start=str(w.start), window_end=str(w.end), rows=result.rows,
                      new_filings=result.new_filings, new_files=result.new_files)
            event(log, "harvest.progress", symbol=symbol, window=f"{w.start}..{w.end}", units=f"{done}/{total}",
                  new_filings=s.new_filings, new_files=s.new_files, failed=s.windows_failed)
    return s
