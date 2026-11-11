"""Every threshold and tunable lives here. No magic numbers elsewhere."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)
DEFAULT_DATA_DIR = Path("annoucement docs")


@dataclass(frozen=True)
class HttpConfig:
    site_url: str = "https://www.nseindia.com"
    archives_url: str = "https://nsearchives.nseindia.com"
    user_agent: str = DEFAULT_USER_AGENT
    # Sustained ceiling across ALL NSE hosts (API + archives) combined.
    max_requests_per_second: float = 2.0
    connect_timeout_s: float = 15.0
    read_timeout_s: float = 90.0
    # Cookies are not required today (Step 0) but priming is cheap insurance.
    session_max_age_s: float = 900.0
    prime_path: str = "/"
    # Keep partial downloads (and resume with Range) only once this many bytes exist.
    range_resume_min_bytes: int = 5 * 1024 * 1024
    chunk_size: int = 64 * 1024
    integrated_page_size: int = 100
    integrated_max_pages: int = 50
    constituents_csv_path: str = "/content/indices/ind_nifty50list.csv"


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    base_delay_s: float = 30.0
    max_delay_s: float = 3600.0
    jitter_fraction: float = 0.25
    # When NSE blocks us (401/403, stream reset, or timeouts, which is how its CDN throttles),
    # every worker pauses. Consecutive blocks double the pause up to the max; a success resets it.
    # Observed 2026-11-09: blocks lasting ~50 min after bursts of a few hundred requests.
    blocked_cooldown_s: float = 300.0
    blocked_cooldown_max_s: float = 3600.0
    # Blocked attempts are mostly NSE's fault, not the file's, so they get a larger budget before 'dead'.
    blocked_max_attempts: int = 12
    # All retries are spent once NSE has blocked us CONTINUOUSLY (no success at all) this long:
    # the stage stops with exit code 3 and resumes cleanly on a later run. With the pauses above
    # (5+10+20+40+60+60 min plus timeouts) this triggers after ~3.4 h, well past the ~50 min
    # blocks observed so far.
    blocked_give_up_s: float = 3 * 3600.0
    # Harvest retries a failing window in-process with a shorter schedule.
    harvest_base_delay_s: float = 10.0
    harvest_max_delay_s: float = 300.0


@dataclass(frozen=True)
class HarvestConfig:
    backfill_years: int = 3
    window_months: int = 3
    # A window is "closed" (skippable) once it was fetched this many days after it ended.
    window_settle_days: int = 1


@dataclass(frozen=True)
class DownloadConfig:
    concurrency: int = 2
    # Upper bound on how long an idle worker sleeps while waiting for scheduled retries.
    idle_poll_s: float = 60.0


@dataclass(frozen=True)
class Paths:
    data_dir: Path = DEFAULT_DATA_DIR

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def partial_dir(self) -> Path:
        return self.data_dir / "partial"

    @property
    def manifest_path(self) -> Path:
        return self.data_dir / "manifest.db"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def constituents_dir(self) -> Path:
        return self.data_dir / "constituents"

    def ensure(self) -> None:
        for d in (self.raw_dir, self.partial_dir, self.logs_dir, self.constituents_dir):
            d.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Config:
    http: HttpConfig = field(default_factory=HttpConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    harvest: HarvestConfig = field(default_factory=HarvestConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    paths: Paths = field(default_factory=Paths)
