# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Follow `AGENTS.md` (project rules). `README.md` has the user-facing details.

## Commands

```bash
pip install -e ".[dev]"                                        # into .venv
python -m pytest                                               # all tests (HTTP is mocked; never hits NSE)
python -m pytest tests/test_manifest.py::test_reharvest_is_idempotent   # single test
nse-ingest status                                              # inspect a data dir (default "annoucement docs/")
nse-ingest --data-dir <scratch dir> harvest --symbols BHARTIARTL --from 2026-07-01 --to 2026-09-30   # live smoke test
python scripts/probe_nse.py <step>                             # re-probe NSE endpoints when they change
python scripts/backfill.py health|start|stop                    # run/inspect the long backfill (always use this, never two jobs)
```

The `backfill-monitor` sub agent (`.claude/agents/backfill-monitor.md`) acts on `backfill.py health` verdicts. A hold (`logs/HOLD.json`) means the job was stopped on purpose, by the monitor after NSE blocked through every retry or by a human. Only a human clears it, with `start --force`.

## Architecture

Phase 1 is `src/nse_ingest`: a two-stage, resumable NSE filings downloader. Phases for chunking, embeddings and retrieval come later and must not be added to this package.

- **Data layer isolation:** `nse_client.py` is the only module that does HTTP. It owns the endpoint table, session priming, the shared rate limiter and streaming download with Range resume. It raises `NotFoundError`, `BlockedError`, `ResponseFormatError` and `TransientError`, and everything above it reacts to those types only. When NSE changes, edit this file (evidence in `docs/step0_nse_discovery.md`).
- **Pure logic:**
  - `records.py` turns raw rows into `FilingRecord`s, one parser per doc_type. `financial_result` merges two endpoints, told apart by row shape.
  - `windows.py` builds calendar-aligned windows.
  - `retry.py` is the backoff and status state machine.
- **I/O edges:** `manifest.py` holds all SQL. `harvest.py` (Stage A) and `download.py` (Stage B) are wired by `cli.py`. Every tunable is in `config.py`.
- **Manifest model:**
  - A filing (one API row) and a file (one attachment URL) are many-to-many through `filing_files`, because NSE files one PDF under several categories and many filings have no attachment (`"-"`).
  - Blobs are content-addressed in `raw/<sha[:2]>/<sha>.<ext>`.
  - Idempotency comes from `INSERT OR IGNORE` on `filing_key` and `source_url`.
  - Harvest resumability comes from `harvest_log`. Windows are skipped only once settled, i.e. fetched after they ended.

## Invariants

- Never construct attachment URLs; use the API field verbatim.
- NSE dates must be `DD-MM-YYYY`; ISO dates silently return empty results.
- The rate limit is global across hosts (default 2 req/s); concurrency must not bypass it.
- `annoucement docs/` (downloaded data, manifest, logs) is git-ignored and must never be committed.
