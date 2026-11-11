"""Turn raw NSE API rows into normalized filing records (pure).

A *filing* is one API row (one announcement, one board-meeting intimation, ...).
A *file* is one attachment URL. One filing can reference 0..n files and one file can be
referenced by several filings (NSE files the same PDF under multiple categories), so they
are modelled separately. See docs/step0_nse_discovery.md for the evidence.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.parse import urlsplit

ANNOUNCEMENT = "announcement"
XBRL = "xbrl"
BOARD_MEETING = "board_meeting"
CORP_ACTION = "corp_action"
FINANCIAL_RESULT = "financial_result"
DOC_TYPES = (ANNOUNCEMENT, XBRL, BOARD_MEETING, CORP_ACTION, FINANCIAL_RESULT)

_NO_VALUE = {"", "-", "null", "none", "na", "n/a"}
_NSE_DT_FORMATS = ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y")
_CONTENT_TYPE_EXT = {
    "application/pdf": "pdf",
    "application/zip": "zip",
    "application/x-zip-compressed": "zip",
    "application/xml": "xml",
    "text/xml": "xml",
    "text/html": "html",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
}
_MAX_EXT_LEN = 5


@dataclass(frozen=True)
class FileRef:
    url: str
    role: str  # which API field it came from, e.g. attchmntFile, ixbrl


@dataclass(frozen=True)
class FilingRecord:
    filing_key: str
    doc_type: str
    symbol: str
    category: str | None
    subject: str | None
    broadcast_dt: datetime | None
    seq_id: str | None
    files: tuple[FileRef, ...]
    raw_json: str


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return None if s.lower() in _NO_VALUE else s


def parse_compact_dt(s: str | None) -> datetime | None:
    """Parse NSE's `dt` field, DDMMYYYYHHMMSS (e.g. 04112026170304)."""
    s = _clean(s)
    if s is None or len(s) != 14 or not s.isdigit():
        return None
    return datetime.strptime(s, "%d%m%Y%H%M%S")


def parse_nse_dt(s: str | None) -> datetime | None:
    """Parse NSE display dates like '04-Nov-2026 17:03:04' or '04-Aug-2026'."""
    s = _clean(s)
    if s is None:
        return None
    for fmt in _NSE_DT_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def clean_url(v: Any) -> str | None:
    """Return the attachment URL verbatim, or None for NSE's no-attachment sentinels ('-', '')."""
    s = _clean(v)
    if s is None or not s.lower().startswith(("http://", "https://")):
        return None
    return s


def file_ext(url: str, content_type: str | None = None) -> str:
    name = urlsplit(url).path.rsplit("/", 1)[-1]
    if "." in name:
        ext = name.rsplit(".", 1)[-1].lower()
        if ext.isalnum() and len(ext) <= _MAX_EXT_LEN:
            return ext
    if content_type:
        return _CONTENT_TYPE_EXT.get(content_type.split(";")[0].strip().lower(), "bin")
    return "bin"


def _key(doc_type: str, seq_id: str | None, *fallback_parts: Any) -> str:
    if seq_id:
        return f"{doc_type}:{seq_id}"
    digest = hashlib.sha1(json.dumps(fallback_parts, default=str, ensure_ascii=False).encode()).hexdigest()
    return f"{doc_type}:h:{digest[:20]}"


def _files(row: dict[str, Any], *fields: str) -> tuple[FileRef, ...]:
    seen: set[str] = set()
    out: list[FileRef] = []
    for f in fields:
        url = clean_url(row.get(f))
        if url and url not in seen:
            seen.add(url)
            out.append(FileRef(url, f))
    return tuple(out)


def _raw(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def from_announcement(row: dict[str, Any], symbol: str, doc_type: str = ANNOUNCEMENT) -> FilingRecord:
    seq = _clean(row.get("seq_id"))
    files = _files(row, "attchmntFile")
    bdt = parse_compact_dt(row.get("dt")) or parse_nse_dt(row.get("an_dt"))
    sym = _clean(row.get("symbol")) or symbol
    subject = _clean(row.get("attchmntText"))
    return FilingRecord(
        filing_key=_key(doc_type, seq, sym, row.get("an_dt"), row.get("desc"), subject, [f.url for f in files]),
        doc_type=doc_type, symbol=sym, category=_clean(row.get("desc")), subject=subject,
        broadcast_dt=bdt, seq_id=seq, files=files, raw_json=_raw(row),
    )


def from_board_meeting(row: dict[str, Any], symbol: str) -> FilingRecord:
    files = _files(row, "attachment", "ixbrl")
    sym = _clean(row.get("bm_symbol")) or symbol
    return FilingRecord(
        filing_key=_key(BOARD_MEETING, None, sym, row.get("bm_timestamp"), row.get("bm_date"),
                        row.get("bm_purpose"), row.get("bm_desc"), [f.url for f in files]),
        doc_type=BOARD_MEETING, symbol=sym, category=_clean(row.get("bm_purpose")),
        subject=_clean(row.get("bm_desc")), broadcast_dt=parse_nse_dt(row.get("bm_timestamp")),
        seq_id=None, files=files, raw_json=_raw(row),
    )


def from_corp_action(row: dict[str, Any], symbol: str) -> FilingRecord:
    sym = _clean(row.get("symbol")) or symbol
    return FilingRecord(
        filing_key=_key(CORP_ACTION, None, sym, row.get("subject"), row.get("exDate"), row.get("recDate"),
                        row.get("series"), row.get("bcStartDate"), row.get("bcEndDate")),
        doc_type=CORP_ACTION, symbol=sym, category="Corporate Action", subject=_clean(row.get("subject")),
        broadcast_dt=parse_nse_dt(row.get("caBroadcastDate")) or parse_nse_dt(row.get("exDate")),
        seq_id=None, files=(), raw_json=_raw(row),
    )


def from_financial_result(row: dict[str, Any], symbol: str) -> FilingRecord:
    """Rows from /api/corporates-financial-results (pre Integrated Filing, i.e. up to ~Feb 2025)."""
    seq = _clean(row.get("seqNumber"))
    files = _files(row, "xbrl")
    sym = _clean(row.get("symbol")) or symbol
    period = " ".join(p for p in (_clean(row.get("fromDate")), _clean(row.get("toDate"))) if p)
    return FilingRecord(
        filing_key=_key(FINANCIAL_RESULT, f"fr:{seq}" if seq else None, sym, row.get("params"),
                        row.get("broadCastDate"), [f.url for f in files]),
        doc_type=FINANCIAL_RESULT, symbol=sym, category=_clean(row.get("relatingTo")),
        subject=" | ".join(p for p in (_clean(row.get("consolidated")), _clean(row.get("audited")), period) if p) or None,
        broadcast_dt=parse_nse_dt(row.get("broadCastDate")), seq_id=seq, files=files, raw_json=_raw(row),
    )


def from_integrated_result(row: dict[str, Any], symbol: str) -> FilingRecord:
    """Rows from /api/integrated-filing-results (SEBI Integrated Filing, 2025+)."""
    seq = _clean(row.get("seq_Id"))
    files = _files(row, "pdf_attach", "xbrl", "ixbrl")
    sym = _clean(row.get("symbol")) or symbol
    return FilingRecord(
        filing_key=_key(FINANCIAL_RESULT, f"if:{seq}" if seq else None, sym, row.get("broadcast_Date"),
                        row.get("qe_Date"), [f.url for f in files]),
        doc_type=FINANCIAL_RESULT, symbol=sym, category=_clean(row.get("type")),
        subject=" | ".join(p for p in (_clean(row.get("type_Sub")), _clean(row.get("consolidated")),
                                       _clean(row.get("audited")), _clean(row.get("qe_Date"))) if p) or None,
        broadcast_dt=parse_nse_dt(row.get("broadcast_Date")), seq_id=seq, files=files, raw_json=_raw(row),
    )


def _financial(row: dict[str, Any], symbol: str) -> FilingRecord:
    # Both result endpoints feed doc_type financial_result; they are distinguishable by shape.
    return from_integrated_result(row, symbol) if "seq_Id" in row else from_financial_result(row, symbol)


_PARSERS: dict[str, Callable[[dict[str, Any], str], FilingRecord]] = {
    ANNOUNCEMENT: lambda r, s: from_announcement(r, s, ANNOUNCEMENT),
    XBRL: lambda r, s: from_announcement(r, s, XBRL),
    BOARD_MEETING: from_board_meeting,
    CORP_ACTION: from_corp_action,
    FINANCIAL_RESULT: _financial,
}


def to_records(doc_type: str, rows: list[dict[str, Any]], symbol: str) -> list[FilingRecord]:
    if doc_type not in _PARSERS:
        raise ValueError(f"unknown doc_type {doc_type!r}")
    parse = _PARSERS[doc_type]
    return [parse(r, symbol) for r in rows if isinstance(r, dict)]
