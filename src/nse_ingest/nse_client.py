"""The ONLY module that talks HTTP to NSE. Swap this file when NSE changes endpoints.

Narrow interface:
    NseClient.list_filings(doc_type, symbol, start, end) -> list[dict]   raw API rows
    NseClient.fetch_constituents_csv() -> str
    NseClient.download(url, dest) -> DownloadResult                      streams to dest, resumes
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable

import httpx

from .config import HttpConfig

NSE_DATE_FMT = "%d-%m-%Y"  # ISO dates silently return "no data found" (Step 0)


class NseError(Exception):
    """Base class; anything not more specific is treated as transient."""


class TransientError(NseError):
    pass


class BlockedError(NseError):
    """401/403/connection reset that persisted after a session refresh."""


class NotFoundError(NseError):
    pass


class ResponseFormatError(NseError):
    """Body was not what we expected (HTML error page, unexpected JSON shape, truncation)."""


# doc_type -> list of (path, fixed params). financial_result spans two endpoints.
_LIST_ENDPOINTS: dict[str, list[tuple[str, dict[str, str]]]] = {
    "announcement": [("/api/corporate-announcements", {"index": "equities"})],
    "xbrl": [("/api/corporate-announcements", {"index": "equities", "reqXbrl": "true"})],
    "board_meeting": [("/api/corporate-board-meetings", {"index": "equities"})],
    "corp_action": [("/api/corporates-corporateActions", {"index": "equities"})],
    "financial_result": [("/api/corporates-financial-results", {"index": "equities", "period": "Quarterly"})],
}
_INTEGRATED_PATH = "/api/integrated-filing-results"
_INTEGRATED_TYPE = "Integrated Filing- Financials"
_BLOCK_STATUSES = {401, 403}
_HTML_EXTS = (".html", ".htm")


def format_nse_date(d: date) -> str:
    return d.strftime(NSE_DATE_FMT)


def normalize_rows(payload: Any) -> list[dict[str, Any]]:
    """NSE returns a bare list, or {"data": [...]} (including {"data": [], "msg": "no data found"})."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    raise ResponseFormatError(f"unexpected JSON shape: {str(payload)[:200]}")


class RateLimiter:
    """Minimum spacing between request starts, shared by every coroutine using the client."""

    def __init__(self, max_per_second: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._interval = 1.0 / max_per_second if max_per_second > 0 else 0.0
        self._clock = clock
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = self._clock()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = self._clock()
            self._next = max(now, self._next) + self._interval


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    bytes_total: int
    resumed: bool
    content_type: str | None
    etag: str | None


class NseClient:
    def __init__(self, cfg: HttpConfig, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._cfg = cfg
        self._limiter = RateLimiter(cfg.max_requests_per_second)
        self._client = httpx.AsyncClient(
            headers={"User-Agent": cfg.user_agent, "Accept-Language": "en-US,en;q=0.9"},
            timeout=httpx.Timeout(cfg.read_timeout_s, connect=cfg.connect_timeout_s),
            follow_redirects=True,
            transport=transport,
        )
        self._primed_at: float | None = None
        self._session_lock = asyncio.Lock()

    async def __aenter__(self) -> NseClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._client.aclose()

    # ---- session -------------------------------------------------------------------------
    async def _prime(self) -> None:
        await self._limiter.acquire()
        try:
            await self._client.get(self._cfg.site_url + self._cfg.prime_path, headers={"Accept": "text/html,*/*"})
        except httpx.HTTPError:
            pass  # priming is best-effort; the real request decides
        self._primed_at = time.monotonic()

    async def _ensure_session(self) -> None:
        async with self._session_lock:
            if self._primed_at is None or time.monotonic() - self._primed_at > self._cfg.session_max_age_s:
                await self._prime()

    async def refresh_session(self) -> None:
        async with self._session_lock:
            self._client.cookies.clear()
            await self._prime()

    # ---- JSON listing ----------------------------------------------------------------------
    async def _get_json(self, path: str, params: dict[str, Any]) -> Any:
        url = self._cfg.site_url + path
        for attempt in (1, 2):
            await self._ensure_session()
            await self._limiter.acquire()
            try:
                r = await self._client.get(url, params=params, headers={"Accept": "application/json, text/plain, */*"})
            except httpx.RemoteProtocolError as e:  # Akamai resets the stream instead of a 403
                if attempt == 1:
                    await self.refresh_session()
                    continue
                raise BlockedError(f"stream reset: {e}") from e
            except httpx.TimeoutException as e:
                # NSE throttles by tarpitting: every request hangs for ~an hour. Retrying now only adds load.
                raise BlockedError(f"timeout: {type(e).__name__}") from e
            except httpx.HTTPError as e:
                raise TransientError(f"{type(e).__name__}: {e}") from e
            if r.status_code in _BLOCK_STATUSES:
                if attempt == 1:
                    await self.refresh_session()
                    continue
                raise BlockedError(f"HTTP {r.status_code} on {path}")
            if r.status_code == 404:
                raise NotFoundError(f"HTTP 404 on {path}")
            if r.status_code != 200:
                raise TransientError(f"HTTP {r.status_code} on {path}")
            try:
                return r.json()
            except json.JSONDecodeError as e:
                raise ResponseFormatError(f"non-JSON body on {path}: {r.text[:200]!r}") from e
        raise BlockedError(f"unreachable: {path}")

    async def list_filings(self, doc_type: str, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        if doc_type not in _LIST_ENDPOINTS:
            raise ValueError(f"unknown doc_type {doc_type!r}")
        dates = {"from_date": format_nse_date(start), "to_date": format_nse_date(end)}
        rows: list[dict[str, Any]] = []
        for path, fixed in _LIST_ENDPOINTS[doc_type]:
            rows += normalize_rows(await self._get_json(path, {**fixed, "symbol": symbol, **dates}))
        if doc_type == "financial_result":
            rows += await self._list_integrated(symbol, dates)
        return rows

    async def _list_integrated(self, symbol: str, dates: dict[str, str]) -> list[dict[str, Any]]:
        size = self._cfg.integrated_page_size
        out: list[dict[str, Any]] = []
        for page in range(1, self._cfg.integrated_max_pages + 1):  # request pages are 1-based
            payload = await self._get_json(_INTEGRATED_PATH, {
                "index": "equities", "symbol": symbol, "type": _INTEGRATED_TYPE, **dates, "page": page, "size": size,
            })
            batch = normalize_rows(payload)
            out += batch
            total = payload.get("totalCount", 0) if isinstance(payload, dict) else 0
            if not batch or len(out) >= total:
                return out
        raise ResponseFormatError(f"integrated filings for {symbol} exceeded {self._cfg.integrated_max_pages} pages")

    async def fetch_constituents_csv(self) -> str:
        await self._limiter.acquire()
        try:
            r = await self._client.get(self._cfg.archives_url + self._cfg.constituents_csv_path)
        except httpx.HTTPError as e:
            raise TransientError(f"{type(e).__name__}: {e}") from e
        if r.status_code != 200 or "symbol" not in r.text[:200].lower():
            raise ResponseFormatError(f"constituents CSV: HTTP {r.status_code} {r.text[:200]!r}")
        return r.text

    # ---- file download ---------------------------------------------------------------------
    async def download(self, url: str, dest: Path) -> DownloadResult:
        """Stream `url` (taken verbatim from the API) into `dest`, resuming with Range when a large
        partial exists. Raises NotFoundError / BlockedError / ResponseFormatError / TransientError."""
        for attempt in (1, 2):
            try:
                return await self._download_once(url, dest)
            except (httpx.RemoteProtocolError, _Blocked) as e:
                if attempt == 1:
                    await self.refresh_session()
                    continue
                raise BlockedError(str(e)) from e
            except httpx.TimeoutException as e:
                raise BlockedError(f"timeout: {type(e).__name__}") from e
            except httpx.HTTPError as e:
                raise TransientError(f"{type(e).__name__}: {e}") from e
        raise BlockedError(f"unreachable: {url}")

    async def _download_once(self, url: str, dest: Path) -> DownloadResult:
        existing = dest.stat().st_size if dest.exists() else 0
        if 0 < existing < self._cfg.range_resume_min_bytes:
            dest.unlink()
            existing = 0
        meta_path = dest.with_suffix(dest.suffix + ".meta")
        headers: dict[str, str] = {}
        if existing:
            headers["Range"] = f"bytes={existing}-"
            validator = _read_meta(meta_path).get("last_modified")
            if validator:
                headers["If-Range"] = validator  # server sends full 200 if the file changed

        await self._limiter.acquire()
        async with self._client.stream("GET", url, headers=headers) as r:
            if r.status_code in _BLOCK_STATUSES:
                raise _Blocked(f"HTTP {r.status_code}")
            if r.status_code == 404:
                raise NotFoundError("HTTP 404")
            if r.status_code == 416:
                dest.unlink(missing_ok=True)
                raise TransientError("HTTP 416 on resume; partial discarded")
            if r.status_code not in (200, 206):
                raise TransientError(f"HTTP {r.status_code}")
            ctype = r.headers.get("content-type")
            if ctype and ctype.lower().startswith("text/html") and not url.lower().endswith(_HTML_EXTS):
                raise ResponseFormatError(f"got HTML instead of a document (content-type {ctype})")

            resumed = r.status_code == 206
            if resumed and not r.headers.get("content-range", "").startswith(f"bytes {existing}-"):
                dest.unlink(missing_ok=True)
                raise TransientError(f"unexpected Content-Range {r.headers.get('content-range')!r}")
            expected = _expected_total(r, existing if resumed else 0)
            _write_meta(meta_path, {"last_modified": r.headers.get("last-modified"), "etag": r.headers.get("etag")})

            written = existing if resumed else 0
            with dest.open("ab" if resumed else "wb") as fh:
                async for chunk in r.aiter_bytes(self._cfg.chunk_size):
                    fh.write(chunk)
                    written += len(chunk)

        if expected is not None and written != expected:
            raise TransientError(f"truncated: got {written} of {expected} bytes")
        if written == 0:
            raise ResponseFormatError("empty body")
        meta_path.unlink(missing_ok=True)
        return DownloadResult(dest, written, resumed, ctype, r.headers.get("etag"))


class _Blocked(Exception):
    pass


def _expected_total(r: httpx.Response, offset: int) -> int | None:
    cl = r.headers.get("content-length")
    if cl is None or not cl.isdigit() or r.headers.get("content-encoding"):
        return None
    return offset + int(cl)


def _read_meta(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def _write_meta(p: Path, meta: dict[str, Any]) -> None:
    p.write_text(json.dumps(meta))
