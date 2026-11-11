# MarketAnnoucementsRAGSystem

A local RAG pipeline over NSE corporate filings for the Nifty 50.

**Phase 1** (this repo today) is `nse-ingest`, a resumable, rate-limited downloader. It puts every announcement, announcement XBRL, board meeting, corporate action and financial result onto local disk, and records each one in a SQLite manifest. Chunking, embeddings and retrieval are out of scope for this phase.

> NSE has no public API, and its terms of use prohibit automated scraping. This tool is for personal-scale research only: it is slow by design (2 requests/sec by default) and meant to run after market hours. Downloaded documents are git-ignored and must not be redistributed.

## Setup

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate    Windows: .venv\Scripts\activate
pip install -e ".[dev]"
python -m pytest          # tests mock the HTTP layer; they never hit NSE
```

Python 3.11+. The only runtime dependency is `httpx`.

## Usage

```bash
nse-ingest harvest  [--symbols SYM ...] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--doc-type T ...] [--refresh-constituents]
nse-ingest download [--limit N] [--concurrency N] [--no-wait]
nse-ingest status          # filings per doc_type, files per status, dedupe ratio, disk usage, failures
nse-ingest retry-failed    # requeue failed + dead files (attempts reset); --failed-only keeps dead
nse-ingest verify          # re-hash blobs vs manifest, report missing/corrupt/orphans; --requeue to fix
```

Global options: `--data-dir` (default `annoucement docs/`, or the `NSE_INGEST_DATA_DIR` env var), `--rps` (default 2.0, shared across all NSE hosts), and `-v`.

A typical 3-year backfill:

```bash
nse-ingest harvest --refresh-constituents   # ~3,250 list calls, ~30-40 min
nse-ingest download                         # hours; safe to stop and restart at any time
```

Without `--from`, the harvest covers the 3 years up to `--to` (default: today). Without `--symbols`, it uses the newest Nifty 50 snapshot in `annoucement docs/constituents/`. `--refresh-constituents` fetches today's list and snapshots it to `nifty50_<date>.csv`, so a corpus can always be traced to the index membership it was built from.

## Data layout

```
annoucement docs/
  manifest.db                       SQLite, WAL mode
  raw/<sha256[:2]>/<sha256>.<ext>   content-addressed blobs (NSE reposts identical files)
  partial/                          in-flight downloads (large partials are resumed with HTTP Range)
  constituents/nifty50_<date>.csv   index membership snapshots
  logs/<command>-<date>.jsonl       structured logs
```

### Manifest schema

The schema differs from the original design sketch because of what Step 0 found in the data:
- One attachment can be filed under several categories.
- Many filings have no attachment at all (`attchmntFile = "-"`).
- The API returns exact duplicate rows.

| table | key | purpose |
|---|---|---|
| `filings` | `filing_key` = `<doc_type>:<NSE seq id>`, or a content hash when NSE gives no id | One row per API row. Holds category (NSE's string, verbatim), subject, broadcast time and the full `raw_json`. |
| `files` | `source_url` (verbatim from the API, never constructed) | One row per attachment. Holds the download state, `sha256`, size and errors. |
| `filing_files` | (`filing_key`, `source_url`) | Many-to-many link with the source field (`attchmntFile`, `ixbrl`, ...). |
| `harvest_log` | (`symbol`, `window_start`, `window_end`, `doc_type`) | Makes Stage A resumable. |
| `categories` | `name` | Every category seen, plus whether it came from the XBRL variant. |

Re-harvesting is idempotent because filings and files are both inserted with `INSERT OR IGNORE`. A re-harvest never resets the download state of a file that's already known.

## Stages

**Stage A, harvest.** For each (symbol, calendar quarter, doc_type), call the list endpoint(s) and record every row, all in one transaction together with the `harvest_log` entry. A window is skipped on later runs only if it completed **and** was fetched at least a day after it ended. The current, still-open quarter is therefore re-listed on every run and picks up new filings. If a window fails, the harvest retries it with backoff, then records it as `failed` and moves on. `status` lists failed windows, and a later harvest retries them.

**Stage B, download.** Workers claim queued files one at a time, stream them to `partial/`, check status and content type, compute the SHA-256, and move the file to `raw/`. If the blob already exists, the duplicate is dropped. The rate limit is shared, so `--concurrency` only overlaps network latency; it never exceeds `--rps`.

### File status state machine

```
discovered ──claim──> downloading ──ok──> downloaded
                          │
                          ├─ transient / blocked / bad content ─> failed (next_retry_at = now + backoff)
                          │                                         └─ claimed again once due
                          └─ 404, or 5th attempt ──────────────────> dead
```

- **Backoff:** 30 s × 2^(attempt−1), capped at 1 h, with ±25% jitter. After 5 attempts a file is marked `dead`. A 404 from the archive host is permanent, so it goes straight to `dead`.
- **Blocking.** NSE's CDN throttles bursts. The first backfill (2026-11-09, at 2 req/s) ran in a cycle: a few hundred successful requests, then about 50 minutes in which every request **timed out**. A 401/403 or an HTTP/2 stream reset triggers one session refresh. If that doesn't help, or the request timed out, it counts as *blocked*:
  - Every worker pauses: 5 min, then 10, 20, 40, capped at 60 min. Any success resets the pause.
  - Blocked file attempts get a larger budget (12) before `dead`, and harvest windows don't spend attempts while blocked.
  - Once NSE has blocked **continuously** for 3 h (no success at all, about 3.4 h with the pause schedule), every retry is spent. The stage stops itself with exit code 3 and logs `harvest.aborted` / `download.aborted`. Nothing is lost, and a later run resumes.
  - Blocks happened at 1 req/s too. The managed launcher (below) still defaults to 1 req/s to keep the load low.
- **Retry waits:** `download` waits for scheduled retries before exiting. `--no-wait` exits as soon as nothing is due.
- All tunables live in `src/nse_ingest/config.py`.

### Resuming after an interruption

Just run the same command again.
- **Ctrl-C, a crash or a reboot** loses at most the files in flight. Rows left in `downloading` go back to the queue when the next `download` starts. Partials over 5 MB resume with `Range` + `If-Range`, and smaller ones start over.
- **The harvest resumes per window,** since completed, settled windows are skipped.
- **Don't run two `download` processes against the same data dir at once.**

### Running it unattended

`scripts/backfill.py` wraps the long job. Run it with the venv's Python, from any directory:

```bash
python scripts/backfill.py start [--rps 1] [--skip-harvest] [--reason TEXT]   # harvest -> download, detached
python scripts/backfill.py stop  [--hold REASON]   # kill the whole job; --hold blocks restarts
python scripts/backfill.py health                  # JSON report with a verdict
```

- **`start`** skips the download if the harvest aborted (blocked), crashed or was interrupted.
- **`stop --hold`** writes `logs/HOLD.json`. After that, `start` refuses until a human passes `--force`.
- **`health`** reports one of these verdicts: `OK`, `BLOCKED_ESCALATING`, `BLOCKED_GAVE_UP`, `STALLED`, `CRASHED`, `FINISHED`, `FINISHED_WITH_GAPS`, `HELD`, `STOPPED` or `NOT_STARTED`.
- **Files it keeps:** job state, actions and health history live in `annoucement docs/logs/` (`backfill.json`, `backfill-actions.jsonl`, `health-history.jsonl`, `backfill-console.txt`).

`nse-ingest` exit codes:

| code | meaning |
|---|---|
| 0 | ok |
| 1 | some harvest windows failed |
| 3 | NSE blocked through every retry |
| 4 | crashed (the traceback is in the JSONL log as a `crashed` event) |
| 130 | interrupted |

**Hourly monitor.** `.claude/agents/backfill-monitor.md` defines a Claude Code sub agent that runs `health` and acts on the verdict:
- `BLOCKED_GAVE_UP`: it stops the job with a hold and reports back.
- Stalls and crashes: it restarts the job, or makes a minimal code fix with tests first. At most 3 automatic restarts per 6 h.
- `FINISHED_WITH_GAPS`: it runs one gap-fill pass.
- Blocks still in progress: it leaves the job alone.

Schedule it from a Claude Code session, for example by having `/loop 1h` spawn that agent. Session schedules stop when the session ends and expire after 7 days.

## NSE endpoints (Step 0 findings)

The full evidence is in [docs/step0_nse_discovery.md](docs/step0_nse_discovery.md). In short:
- The live corporate-filings pages call `/api/corporate-announcements` (`reqXbrl=true` gives the XBRL variant), `/api/corporate-board-meetings`, `/api/corporates-corporateActions`, `/api/corporates-financial-results` and `/api/integrated-filing-results` (2025+ results, paginated).
- Dates must be `DD-MM-YYYY`. ISO dates silently return "no data found".
- The Next.js dispatcher is `/api/NextApi/apiClient/GetQuoteApi?functionName=...`, not `/api/GetQuoteApi`. It only exposes announcements (`getCorporateAnnouncement&marketApiType=equities`), so it's the fallback.
- `equity-stockIndices` is gone. The Nifty 50 list comes from `nsearchives.nseindia.com/content/indices/ind_nifty50list.csv`.
- **Headers:** a browser User-Agent is the only header needed. Cookie priming is done anyway, as cheap insurance.
- **Attachments** are on `nsearchives.nseindia.com`. They need no Referer and support Range.

**Row cap: none.** A single call returned 798 rows (RELIANCE, 3 years) and another returned 55,570 rows (all equities, 3 months). Windowed results match slices of larger queries exactly, and the `[0 … 99]` in the UI is page-by-page display. The no-date "full history" call is *less* complete: it dropped rows and truncated timestamps. That's why the harvest always uses explicit quarterly windows.

All of this logic lives in `src/nse_ingest/nse_client.py`. When NSE changes something, re-run `scripts/probe_nse.py` and update that one module.

## Layout

```
src/nse_ingest/
  nse_client.py    all NSE HTTP (endpoints, session, rate limit, streaming download)
  records.py       raw API rows -> FilingRecord (pure)
  windows.py       calendar window generation (pure)
  retry.py         backoff + status state machine (pure)
  manifest.py      SQLite schema and queries
  harvest.py       Stage A
  download.py      Stage B
  constituents.py  Nifty 50 snapshot
  cli.py, config.py, logs.py
scripts/probe_nse.py   throwaway Step 0 probe
```
