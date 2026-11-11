"""Downloader + data layer against a mocked HTTP transport. Never touches NSE."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Callable

import httpx
import pytest

from nse_ingest.config import Config, DownloadConfig, HttpConfig, Paths, RetryConfig
from nse_ingest.download import Downloader, content_path
from nse_ingest.manifest import Manifest
from nse_ingest.nse_client import BlockedError, NseClient
from nse_ingest.records import FileRef, FilingRecord
from nse_ingest.windows import Window

PDF = b"%PDF-1.7 " + b"x" * 200
ARCH = "https://nsearchives.nseindia.com/corporate/"
LOG = logging.getLogger("test")


def cfg_for(tmp: Path, **http: object) -> Config:
    c = Config(http=HttpConfig(max_requests_per_second=1000, **http),
               retry=RetryConfig(blocked_cooldown_s=0), paths=Paths(tmp))
    c.paths.ensure()
    return c


def seed(m: Manifest, urls: list[str]) -> None:
    recs = [FilingRecord(f"announcement:{i}", "announcement", "X", "Updates", None, None, str(i),
                         (FileRef(u, "attchmntFile"),), "{}") for i, u in enumerate(urls)]
    m.record_window("X", Window(date(2024, 1, 1), date(2024, 3, 31)), "announcement", recs)


def run(cfg: Config, m: Manifest, handler: Callable[[httpx.Request], httpx.Response]) -> object:
    async def go():
        async with NseClient(cfg.http, transport=httpx.MockTransport(handler)) as client:
            return await Downloader(m, client, cfg, LOG).run(None, 2, wait_for_retries=False)
    return asyncio.run(go())


def pdf_response(body: bytes = PDF) -> httpx.Response:
    return httpx.Response(200, content=body, headers={"content-type": "application/pdf"})


def test_identical_content_is_stored_once(tmp_path: Path) -> None:
    cfg = cfg_for(tmp_path)
    with Manifest(tmp_path / "m.db") as m:
        seed(m, [ARCH + "a.pdf", ARCH + "b.pdf"])
        s = run(cfg, m, lambda r: pdf_response() if r.url.host.startswith("nsearchives") else httpx.Response(200))
        assert (s.downloaded, s.deduped) == (2, 1)
        sha = hashlib.sha256(PDF).hexdigest()
        assert content_path(cfg.paths.raw_dir, sha, "pdf").read_bytes() == PDF
        assert m.file_status_counts() == {"downloaded": 2}
        assert not any(cfg.paths.partial_dir.iterdir())


def test_404_dead_and_html_error_page_failed(tmp_path: Path) -> None:
    cfg = cfg_for(tmp_path)

    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path.endswith("gone.pdf"):
            return httpx.Response(404, text="<html>not found</html>", headers={"content-type": "text/html"})
        if r.url.path.endswith("html.pdf"):
            return httpx.Response(200, text="<html>oops</html>", headers={"content-type": "text/html"})
        return httpx.Response(200)

    with Manifest(tmp_path / "m.db") as m:
        seed(m, [ARCH + "gone.pdf", ARCH + "html.pdf"])
        s = run(cfg, m, handler)
        assert (s.dead, s.failed, s.downloaded) == (1, 1, 0)
        assert m.file_status_counts() == {"dead": 1, "failed": 1}


def test_ixbrl_html_is_accepted(tmp_path: Path) -> None:
    cfg = cfg_for(tmp_path)
    with Manifest(tmp_path / "m.db") as m:
        seed(m, [ARCH + "ixbrl/a_WEB.html"])
        s = run(cfg, m, lambda r: httpx.Response(200, text="<html>ixbrl</html>", headers={"content-type": "text/html"}))
        assert s.downloaded == 1


def test_403_refreshes_session_then_succeeds(tmp_path: Path) -> None:
    cfg = cfg_for(tmp_path)
    calls = {"doc": 0, "prime": 0}

    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.host == "www.nseindia.com":
            calls["prime"] += 1
            return httpx.Response(200)
        calls["doc"] += 1
        return httpx.Response(403) if calls["doc"] == 1 else pdf_response()

    with Manifest(tmp_path / "m.db") as m:
        seed(m, [ARCH + "a.pdf"])
        s = run(cfg, m, handler)
        assert s.downloaded == 1 and calls["prime"] >= 1
        assert m.file_status_counts() == {"downloaded": 1}


def test_range_resume_appends_partial(tmp_path: Path) -> None:
    cfg = cfg_for(tmp_path, range_resume_min_bytes=10)
    url = ARCH + "big.pdf"
    from nse_ingest.download import partial_path
    part = partial_path(cfg.paths.partial_dir, url)
    part.write_bytes(PDF[:50])
    seen_range: list[str | None] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen_range.append(r.headers.get("range"))
        start = int(r.headers["range"].split("=")[1].rstrip("-"))
        return httpx.Response(206, content=PDF[start:], headers={
            "content-type": "application/pdf", "content-range": f"bytes {start}-{len(PDF) - 1}/{len(PDF)}"})

    with Manifest(tmp_path / "m.db") as m:
        seed(m, [url])
        s = run(cfg, m, handler)
        assert s.downloaded == 1 and seen_range == ["bytes=50-"]
        assert content_path(cfg.paths.raw_dir, hashlib.sha256(PDF).hexdigest(), "pdf").read_bytes() == PDF


def test_persistent_block_aborts_download_and_releases_rows(tmp_path: Path) -> None:
    cfg = Config(http=HttpConfig(max_requests_per_second=1000),
                 retry=RetryConfig(base_delay_s=0.001, blocked_cooldown_s=0.01, blocked_cooldown_max_s=0.02,
                                   blocked_give_up_s=0.05),
                 download=DownloadConfig(idle_poll_s=0.01), paths=Paths(tmp_path))
    cfg.paths.ensure()

    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.host == "www.nseindia.com":
            return httpx.Response(200)
        raise httpx.ReadTimeout("tarpit", request=r)

    async def go():
        async with NseClient(cfg.http, transport=httpx.MockTransport(handler)) as client:
            return await Downloader(m, client, cfg, LOG).run(None, 2, wait_for_retries=True)

    with Manifest(tmp_path / "m.db") as m:
        seed(m, [ARCH + "a.pdf", ARCH + "b.pdf"])
        s = asyncio.run(go())
        assert s.aborted and "blocked continuously" in s.aborted and s.downloaded == 0
        counts = m.file_status_counts()
        assert counts.get("downloading", 0) == 0 and counts.get("dead", 0) == 0


def test_list_filings_normalizes_shapes_and_formats_dates() -> None:
    seen: list[httpx.URL] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r.url)
        if r.url.path == "/":
            return httpx.Response(200)
        return httpx.Response(200, json={"data": [], "msg": "no data found"})

    async def go():
        async with NseClient(HttpConfig(max_requests_per_second=1000), httpx.MockTransport(handler)) as c:
            return await c.list_filings("announcement", "RELIANCE", date(2026, 8, 1), date(2026, 8, 31))

    assert asyncio.run(go()) == []
    api = [u for u in seen if u.path == "/api/corporate-announcements"][0]
    assert api.params["from_date"] == "01-08-2026" and api.params["to_date"] == "31-08-2026"
    assert api.params["symbol"] == "RELIANCE" and api.params["index"] == "equities"


def test_persistent_stream_reset_is_blocked() -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/":
            return httpx.Response(200)
        raise httpx.RemoteProtocolError("StreamReset", request=r)

    async def go():
        async with NseClient(HttpConfig(max_requests_per_second=1000), httpx.MockTransport(handler)) as c:
            await c.list_filings("board_meeting", "X", date(2024, 1, 1), date(2024, 3, 31))

    with pytest.raises(BlockedError):
        asyncio.run(go())
