"""Nifty 50 constituent list, snapshotted to a dated CSV so the corpus is reproducible."""
from __future__ import annotations

import csv
import io
from datetime import date
from pathlib import Path

from .nse_client import NseClient

SNAPSHOT_PREFIX = "nifty50_"


def parse_symbols(csv_text: str) -> list[str]:
    reader = csv.DictReader(io.StringIO(csv_text))
    col = next((c for c in (reader.fieldnames or []) if c.strip().lower() == "symbol"), None)
    if col is None:
        raise ValueError(f"no Symbol column in constituents CSV (columns: {reader.fieldnames})")
    symbols = [row[col].strip() for row in reader if row.get(col, "").strip()]
    if not symbols:
        raise ValueError("constituents CSV has no rows")
    return symbols


def latest_snapshot(dir_: Path) -> Path | None:
    snaps = sorted(dir_.glob(f"{SNAPSHOT_PREFIX}*.csv"))
    return snaps[-1] if snaps else None


async def load_symbols(client: NseClient, dir_: Path, today: date, refresh: bool) -> tuple[list[str], Path]:
    """Use the newest snapshot unless `refresh`, in which case fetch and write today's."""
    snap = None if refresh else latest_snapshot(dir_)
    if snap is None:
        text = await client.fetch_constituents_csv()
        parse_symbols(text)  # validate before writing
        snap = dir_ / f"{SNAPSHOT_PREFIX}{today.isoformat()}.csv"
        snap.write_text(text, encoding="utf-8")
    return parse_symbols(snap.read_text(encoding="utf-8")), snap
