# AGENTS.md

## Repo Facts

- Trust `pyproject.toml`, `src/flowlet/__init__.py`, `tests/test_pipeline.py`, and `README.md`.
- Python version is pinned to `3.13` in both `pyproject.toml` and `.python-version`.
- This repo is a single-package library. The entire public API currently lives in `src/flowlet/__init__.py` (`pipe`, `flow`, `Pipeline`, `Flow`, `Stage`, `map_`, `flat_map`, `filter_`).
- Tests are all in `tests/test_pipeline.py`; there is no CI, pre-commit config, or other repo-local instruction file to consult.

## Commands

- Install deps: `uv sync`
- Lint: `uv run ruff check .`
- Format: `uv run ruff check --fix-only . && uv run ruff format .`
- Type-check: `uv run mypy .`
- Test all: `uv run pytest`
- Test one case: `uv run pytest tests/test_pipeline.py::TestPipelineApi::test_method_chain_collects_results`

## Makefile Shortcuts

- `make lint` runs `uv run ruff check .`.
- `make test` runs `uv run pytest`.
- `make fformat` runs strict formatting: `uv run ruff check --fix-only . && uv run ruff format .`.
- `make format` is looser and ignores `F401` during fix-only import cleanup before formatting.

## Verified Quirks

- `pytest` currently passes on `HEAD`.
- `ruff` currently passes on `HEAD`.
- `mypy` is expected to pass on `HEAD`.
- `pytest` runs with `-s` from `pyproject.toml`, so print output is not captured.

## Working Notes

- Keep edits small and centralized: most library changes only touch `src/flowlet/__init__.py`, `tests/test_pipeline.py`, and README examples.
- The codebase already uses PEP 695 generics (`class Pipeline[T]`, `class Flow[T, U]`) and `from __future__ import annotations`; match that style when editing types.
- The pipeline API is immutable and async-iterable: `pipe(source).map(...).flat_map(...).filter(...).collect()` is the primary style; `|` with `map_`/`flat_map`/`filter_` is the operator alternative.
- `None` is valid data. Do not reintroduce sentinel-value stream termination.
- When changing pipeline behavior, verify focused async tests first, then run the full test suite.
