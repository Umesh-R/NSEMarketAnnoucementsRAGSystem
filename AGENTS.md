# AGENTS.md

This repository is currently a blank or early-stage workspace for a stock-market announcement RAG system. Keep work intentionally small, reproducible, and easy to review.

## Core guidance

- Prefer focused, incremental changes over broad rewrites.
- Preserve the existing structure and conventions of any file you touch; do not introduce unrelated refactors.
- Keep modules and responsibilities clear, especially for data ingestion, retrieval, ranking, prompt orchestration, and evaluation.
- Add or update tests for behavior changes whenever a test suite exists.
- Do not commit secrets, API keys, or environment-specific values. Use environment variables and sample config files instead.

## Project expectations

- Treat this as a Python-first project unless a later repo structure clearly indicates otherwise.
- Prefer explicit dependency management in requirements, pyproject, or equivalent project config when adding packages.
- Keep notebooks exploratory and avoid committing generated artifacts, large datasets, or cached model outputs unless the repo explicitly expects them.
- For market and announcement data work, document provenance, assumptions, and processing steps clearly.

## Documentation and validation

- If a new feature or setup step is introduced, document it in the repo README or the most relevant docs file rather than duplicating details here.
- Run the smallest relevant validation command first; in a Python project that is usually `pytest` or `python -m pytest`.
- If there is no existing test coverage for the changed behavior, prefer a focused smoke check or a minimal validation script that exercises the exact workflow.

## When adding new code

- Keep functions and classes narrowly scoped.
- Favor clear names and straightforward data flow.
- Make failures explicit and user-visible when dealing with external data sources, model calls, or retrieval pipelines.
- Write code that is easy to test in isolation when possible.

This file is intentionally minimal so the agent remains aligned with the project as it grows while avoiding unnecessary duplication of existing docs.
