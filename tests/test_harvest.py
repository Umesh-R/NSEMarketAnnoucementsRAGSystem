"""Harvest behaviour under NSE blocking, with a mocked HTTP layer."""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path

import httpx
import pytest

from nse_ingest.config import Config, HttpConfig, Paths, RetryConfig
from nse_ingest.harvest import HarvestAborted, harvest
from nse_ingest.manifest import Manifest
from nse_ingest.nse_client import BlockedError, NseClient
from nse_ingest.windows import Window

W = [Window(date(2024, 1, 1), date(2024, 3, 31))]
LOG = logging.getLogger("test")
ROW = {"seq_id": "1", "symbol": "X", "desc": "Updates", "dt": "01012024100000", "an_dt": "01-Jan-2024 10:00:00",
       "attchmntFile": "https://nsearchives.nseindia.com/corporate/X_1.pdf", "attchmntText": "t"}


def cfg_for(tmp: Path, **retry: object) -> Config:
    return Config(http=HttpConfig(max_requests_per_second=1000),
                  retry=RetryConfig(blocked_cooldown_s=0.01, blocked_cooldown_max_s=0.02, max_attempts=2, **retry),
                  paths=Paths(tmp))


def run(cfg: Config, m: Manifest, handler) -> object:
    async def go():
        async with NseClient(cfg.http, transport=httpx.MockTransport(handler)) as c:
            return await harvest(m, c, cfg, ["X"], W, ["announcement"], LOG)
    return asyncio.run(go())


def test_timeout_is_blocked() -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=r)

    async def go():
        async with NseClient(HttpConfig(max_requests_per_second=1000), httpx.MockTransport(handler)) as c:
            await c.list_filings("announcement", "X", date(2024, 1, 1), date(2024, 3, 31))

    with pytest.raises(BlockedError):
        asyncio.run(go())


def test_blocks_pause_without_consuming_window_attempts(tmp_path: Path) -> None:
    calls = {"api": 0}

    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/":
            return httpx.Response(200)
        calls["api"] += 1
        if calls["api"] <= 4:  # more blocks than max_attempts=2 would allow
            raise httpx.ReadTimeout("tarpit", request=r)
        return httpx.Response(200, json=[ROW])

    with Manifest(tmp_path / "m.db") as m:
        s = run(cfg_for(tmp_path), m, handler)
        assert (s.windows_fetched, s.windows_failed, s.new_filings) == (1, 0, 1)


def test_persistent_block_aborts_run(tmp_path: Path) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/":
            return httpx.Response(200)
        raise httpx.ReadTimeout("tarpit", request=r)

    with Manifest(tmp_path / "m.db") as m, pytest.raises(HarvestAborted):
        run(cfg_for(tmp_path, blocked_give_up_s=0.05), m, handler)
