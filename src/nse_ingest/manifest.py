"""SQLite manifest (WAL). The only module that knows the schema.

Schema deviates from the original sketch based on Step 0 evidence:
  filings      one row per API row; idempotent on filing_key
  files        one row per attachment URL; carries the download state machine
  filing_files many-to-many (one PDF can be filed under several categories)
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

from . import retry
from .records import XBRL, FilingRecord
from .windows import Window, is_window_settled

SCHEMA = """
CREATE TABLE IF NOT EXISTS filings (
    filing_key    TEXT PRIMARY KEY,
    doc_type      TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    category      TEXT,
    subject       TEXT,
    broadcast_dt  TEXT,
    seq_id        TEXT,
    raw_json      TEXT NOT NULL,
    discovered_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS filings_symbol_type ON filings(symbol, doc_type);

CREATE TABLE IF NOT EXISTS files (
    source_url    TEXT PRIMARY KEY,
    sha256        TEXT,
    symbol        TEXT NOT NULL,
    doc_type      TEXT NOT NULL,
    file_ext      TEXT,
    byte_size     INTEGER,
    etag          TEXT,
    status        TEXT NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_error    TEXT,
    discovered_at TEXT NOT NULL,
    downloaded_at TEXT
);
CREATE INDEX IF NOT EXISTS files_status ON files(status, next_retry_at);
CREATE INDEX IF NOT EXISTS files_sha ON files(sha256);

CREATE TABLE IF NOT EXISTS filing_files (
    filing_key TEXT NOT NULL REFERENCES filings(filing_key),
    source_url TEXT NOT NULL REFERENCES files(source_url),
    role       TEXT NOT NULL,
    PRIMARY KEY (filing_key, source_url)
);
CREATE INDEX IF NOT EXISTS filing_files_url ON filing_files(source_url);

CREATE TABLE IF NOT EXISTS harvest_log (
    symbol       TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end   TEXT NOT NULL,
    doc_type     TEXT NOT NULL,
    status       TEXT NOT NULL,
    row_count    INTEGER,
    new_filings  INTEGER,
    last_error   TEXT,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (symbol, window_start, window_end, doc_type)
);

CREATE TABLE IF NOT EXISTS categories (
    name            TEXT PRIMARY KEY,
    is_xbrl_variant INTEGER NOT NULL,
    first_seen      TEXT NOT NULL
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat(timespec="seconds") if dt else None


@dataclass(frozen=True)
class FileTask:
    source_url: str
    symbol: str
    doc_type: str
    attempts: int


@dataclass(frozen=True)
class WindowResult:
    rows: int
    new_filings: int
    new_files: int


class Manifest:
    def __init__(self, path: Path | str) -> None:
        self._conn = sqlite3.connect(str(path), isolation_level=None)  # explicit transactions
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Manifest:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- harvest ---------------------------------------------------------------------------
    def is_window_done(self, symbol: str, w: Window, doc_type: str, settle_days: int) -> bool:
        row = self._conn.execute(
            "SELECT status, fetched_at FROM harvest_log WHERE symbol=? AND window_start=? AND window_end=? AND doc_type=?",
            (symbol, w.start.isoformat(), w.end.isoformat(), doc_type),
        ).fetchone()
        if row is None or row["status"] != "done":
            return False
        fetched_on = datetime.fromisoformat(row["fetched_at"]).date()
        return is_window_settled(w, fetched_on, settle_days)

    def record_window(self, symbol: str, w: Window, doc_type: str, records: list[FilingRecord],
                      now: datetime | None = None) -> WindowResult:
        """Insert all filings/files for a window and mark it done, atomically."""
        ts = _iso(now or utcnow())
        c = self._conn
        new_filings = new_files = 0
        c.execute("BEGIN IMMEDIATE")
        try:
            for rec in records:
                cur = c.execute(
                    "INSERT OR IGNORE INTO filings(filing_key, doc_type, symbol, category, subject, broadcast_dt,"
                    " seq_id, raw_json, discovered_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (rec.filing_key, rec.doc_type, rec.symbol, rec.category, rec.subject,
                     _iso(rec.broadcast_dt), rec.seq_id, rec.raw_json, ts),
                )
                new_filings += cur.rowcount
                if rec.category:
                    c.execute("INSERT OR IGNORE INTO categories(name, is_xbrl_variant, first_seen) VALUES (?,?,?)",
                              (rec.category, int(rec.doc_type == XBRL), ts))
                for f in rec.files:
                    cur = c.execute(
                        "INSERT OR IGNORE INTO files(source_url, symbol, doc_type, status, discovered_at)"
                        " VALUES (?,?,?,?,?)", (f.url, rec.symbol, rec.doc_type, retry.DISCOVERED, ts))
                    new_files += cur.rowcount
                    c.execute("INSERT OR IGNORE INTO filing_files(filing_key, source_url, role) VALUES (?,?,?)",
                              (rec.filing_key, f.url, f.role))
            c.execute(
                "INSERT OR REPLACE INTO harvest_log(symbol, window_start, window_end, doc_type, status, row_count,"
                " new_filings, last_error, fetched_at) VALUES (?,?,?,?,'done',?,?,NULL,?)",
                (symbol, w.start.isoformat(), w.end.isoformat(), doc_type, len(records), new_filings, ts),
            )
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise
        return WindowResult(len(records), new_filings, new_files)

    def record_window_failure(self, symbol: str, w: Window, doc_type: str, error: str,
                              now: datetime | None = None) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO harvest_log(symbol, window_start, window_end, doc_type, status, row_count,"
            " new_filings, last_error, fetched_at) VALUES (?,?,?,?,'failed',NULL,NULL,?,?)",
            (symbol, w.start.isoformat(), w.end.isoformat(), doc_type, error[:1000], _iso(now or utcnow())),
        )

    # ---- download state machine ------------------------------------------------------------
    def reset_in_flight(self) -> int:
        """Crash recovery: anything left 'downloading' by a dead process goes back to the queue."""
        return self._conn.execute("UPDATE files SET status=? WHERE status=?",
                                  (retry.DISCOVERED, retry.DOWNLOADING)).rowcount

    def claim_next(self, now: datetime | None = None) -> FileTask | None:
        ts = _iso(now or utcnow())
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT source_url, symbol, doc_type, attempts FROM files"
                " WHERE status=? OR (status=? AND next_retry_at<=?)"
                " ORDER BY attempts, discovered_at LIMIT 1",
                (retry.DISCOVERED, retry.FAILED, ts),
            ).fetchone()
            if row is None:
                c.execute("COMMIT")
                return None
            c.execute("UPDATE files SET status=? WHERE source_url=?", (retry.DOWNLOADING, row["source_url"]))
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise
        return FileTask(row["source_url"], row["symbol"], row["doc_type"], row["attempts"])

    def next_retry_at(self) -> datetime | None:
        row = self._conn.execute("SELECT MIN(next_retry_at) AS t FROM files WHERE status=?",
                                 (retry.FAILED,)).fetchone()
        return datetime.fromisoformat(row["t"]) if row and row["t"] else None

    def mark_downloaded(self, url: str, sha256: str, ext: str, size: int, etag: str | None,
                        now: datetime | None = None) -> None:
        self._conn.execute(
            "UPDATE files SET status=?, sha256=?, file_ext=?, byte_size=?, etag=?, downloaded_at=?,"
            " last_error=NULL, next_retry_at=NULL WHERE source_url=?",
            (retry.DOWNLOADED, sha256, ext, size, etag, _iso(now or utcnow()), url),
        )

    def mark_failure(self, url: str, decision: retry.RetryDecision, error: str) -> None:
        self._conn.execute(
            "UPDATE files SET status=?, attempts=?, next_retry_at=?, last_error=? WHERE source_url=?",
            (decision.status, decision.attempts, _iso(decision.next_retry_at), error[:1000], url),
        )

    def release(self, url: str) -> None:
        """Put a claimed row back without counting an attempt (e.g. global pause, shutdown)."""
        self._conn.execute("UPDATE files SET status=? WHERE source_url=? AND status=?",
                           (retry.DISCOVERED, url, retry.DOWNLOADING))

    def retry_failed(self, include_dead: bool = True) -> int:
        statuses = (retry.FAILED, retry.DEAD) if include_dead else (retry.FAILED,)
        q = ",".join("?" * len(statuses))
        return self._conn.execute(
            f"UPDATE files SET status=?, attempts=0, next_retry_at=NULL WHERE status IN ({q})",
            (retry.DISCOVERED, *statuses),
        ).rowcount

    def requeue(self, url: str) -> None:
        self._conn.execute("UPDATE files SET status=?, sha256=NULL, attempts=0, downloaded_at=NULL WHERE source_url=?",
                           (retry.DISCOVERED, url))

    # ---- reporting -------------------------------------------------------------------------
    def file_status_counts(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in self._conn.execute("SELECT status, COUNT(*) n FROM files GROUP BY status")}

    def filing_counts(self) -> dict[str, int]:
        return {r["doc_type"]: r["n"] for r in
                self._conn.execute("SELECT doc_type, COUNT(*) n FROM filings GROUP BY doc_type")}

    def harvest_counts(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in
                self._conn.execute("SELECT status, COUNT(*) n FROM harvest_log GROUP BY status")}

    def downloaded_stats(self) -> tuple[int, int, int]:
        r = self._conn.execute("SELECT COUNT(*) n, COUNT(DISTINCT sha256) u, COALESCE(SUM(byte_size),0) b"
                               " FROM files WHERE status=?", (retry.DOWNLOADED,)).fetchone()
        return r["n"], r["u"], r["b"]

    def recent_failures(self, limit: int = 10) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT source_url, status, attempts, last_error FROM files WHERE status IN (?,?)"
            " ORDER BY status, attempts DESC LIMIT ?", (retry.FAILED, retry.DEAD, limit)).fetchall()

    def iter_downloaded(self) -> Iterator[sqlite3.Row]:
        yield from self._conn.execute("SELECT source_url, sha256, file_ext, byte_size FROM files WHERE status=?",
                                      (retry.DOWNLOADED,))

    def harvest_failures(self) -> list[sqlite3.Row]:
        return self._conn.execute("SELECT * FROM harvest_log WHERE status='failed'").fetchall()


def window_from_row(start: str, end: str) -> Window:
    return Window(date.fromisoformat(start), date.fromisoformat(end))
