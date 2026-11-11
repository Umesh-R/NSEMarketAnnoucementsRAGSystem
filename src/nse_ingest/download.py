"""Stage B: file fetch. Workers claim queued files, download, hash, store content-addressed."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .logs import event
from .manifest import FileTask, Manifest, utcnow
from .nse_client import BlockedError, NotFoundError, NseClient, NseError, ResponseFormatError
from .records import file_ext
from .retry import BlockTracker, FailureKind, on_failure

_HASH_CHUNK = 1024 * 1024


@dataclass
class DownloadSummary:
    downloaded: int = 0
    deduped: int = 0
    bytes: int = 0
    failed: int = 0
    dead: int = 0
    aborted: str | None = None  # set when NSE blocked us through every retry


def content_path(raw_dir: Path, sha256: str, ext: str) -> Path:
    return raw_dir / sha256[:2] / f"{sha256}.{ext}"


def partial_path(partial_dir: Path, url: str) -> Path:
    return partial_dir / f"{hashlib.sha1(url.encode()).hexdigest()}.part"


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while chunk := fh.read(_HASH_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def classify(exc: BaseException) -> FailureKind:
    if isinstance(exc, NotFoundError):
        return FailureKind.NOT_FOUND
    if isinstance(exc, BlockedError):
        return FailureKind.BLOCKED
    if isinstance(exc, ResponseFormatError):
        return FailureKind.INVALID_CONTENT
    return FailureKind.TRANSIENT


class Downloader:
    def __init__(self, manifest: Manifest, client: NseClient, cfg: Config, log: logging.Logger) -> None:
        self._m = manifest
        self._client = client
        self._cfg = cfg
        self._log = log
        self._paused_until = 0.0
        self._blocks = BlockTracker.from_config(cfg.retry)
        self._stopping = False
        self.summary = DownloadSummary()

    async def run(self, limit: int | None, concurrency: int, wait_for_retries: bool = True) -> DownloadSummary:
        reset = self._m.reset_in_flight()
        if reset:
            event(self._log, "download.recovered_in_flight", count=reset)
        budget = [limit if limit is not None else -1]
        await asyncio.gather(*(self._worker(i, budget, wait_for_retries) for i in range(concurrency)))
        return self.summary

    async def _wait_if_paused(self) -> None:
        loop = asyncio.get_running_loop()
        while (delay := self._paused_until - loop.time()) > 0:
            await asyncio.sleep(delay)

    async def _worker(self, wid: int, budget: list[int], wait_for_retries: bool) -> None:
        while True:
            await self._wait_if_paused()
            if self._stopping or budget[0] == 0:
                return
            task = self._m.claim_next()
            if task is None:
                nxt = self._m.next_retry_at() if wait_for_retries else None
                if nxt is None:
                    return
                await asyncio.sleep(min(self._cfg.download.idle_poll_s, max(1.0, (nxt - utcnow()).total_seconds())))
                continue
            if budget[0] > 0:
                budget[0] -= 1
            try:
                await self._process(task)
            except asyncio.CancelledError:
                self._m.release(task.source_url)  # Ctrl-C: in-flight row goes back to the queue
                raise

    async def _process(self, task: FileTask) -> None:
        paths = self._cfg.paths
        part = partial_path(paths.partial_dir, task.source_url)
        try:
            result = await self._client.download(task.source_url, part)
            sha = await asyncio.to_thread(sha256_file, part)
        except (NseError, OSError) as e:
            self._fail(task, e)
            return
        ext = file_ext(task.source_url, result.content_type)
        final = content_path(paths.raw_dir, sha, ext)
        deduped = final.exists()
        if deduped:
            part.unlink()
            self.summary.deduped += 1
        else:
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(part, final)
        self._m.mark_downloaded(task.source_url, sha, ext, result.bytes_total, result.etag)
        self._blocks.on_success()
        self.summary.downloaded += 1
        self.summary.bytes += result.bytes_total
        event(self._log, "download.ok", logging.DEBUG, url=task.source_url, sha256=sha, bytes=result.bytes_total,
              resumed=result.resumed, deduped=deduped)
        if self.summary.downloaded % 100 == 0:
            event(self._log, "download.progress", downloaded=self.summary.downloaded, deduped=self.summary.deduped,
                  mb=round(self.summary.bytes / 1e6, 1), failed=self.summary.failed, dead=self.summary.dead)

    def _fail(self, task: FileTask, exc: BaseException) -> None:
        kind = classify(exc)
        decision = on_failure(task.attempts, kind, utcnow(), self._cfg.retry, random.random())
        self._m.mark_failure(task.source_url, decision, f"{type(exc).__name__}: {exc}")
        if decision.status == "dead":
            self.summary.dead += 1
        else:
            self.summary.failed += 1
        event(self._log, "download.failed", logging.WARNING, url=task.source_url, kind=kind.value,
              attempts=decision.attempts, status=decision.status, error=str(exc)[:300])
        if kind is FailureKind.BLOCKED and not self._stopping:
            loop = asyncio.get_running_loop()
            if self._paused_until > loop.time():
                return  # concurrent workers hitting the same block count once
            pause = self._blocks.on_block()
            if pause is None:
                self._stopping = True
                self.summary.aborted = (f"NSE blocked continuously for {self._blocks.blocked_for_s / 3600:.1f} h "
                                        f"after {self._blocks.consecutive} escalating pauses; last error: {exc}")
                event(self._log, "download.aborted", logging.ERROR, reason=self.summary.aborted)
                return
            self._paused_until = loop.time() + pause
            event(self._log, "download.blocked_pause", logging.WARNING, pause_s=round(pause),
                  consecutive=self._blocks.consecutive, blocked_min=round(self._blocks.blocked_for_s / 60))
