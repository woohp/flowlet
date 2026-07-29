# AGENTS.md

## Repo Facts

- Trust `pyproject.toml`, `src/flowlet/__init__.py`, `src/flowlet/_flow.py`, `src/flowlet/functional.py`, `src/flowlet/op.py`, `tests/test_pipeline.py`, and `README.md`.
- Python version is pinned to `3.13` in both `pyproject.toml` and `.python-version`.
- This repo is a single-package library. The public API currently lives in `src/flowlet/` (`pipe`, `Pipeline`, `Flow`, `op`, and `flowlet.functional`).
- Tests live in `tests/test_pipeline.py` (behavior and API) and `tests/test_lifecycle.py` (the iterator-ownership specification). There is no CI, pre-commit config, or other repo-local instruction file to consult.

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

- Keep edits small and centralized: most library changes only touch `src/flowlet/__init__.py`, `src/flowlet/_flow.py`, `src/flowlet/functional.py`, `src/flowlet/op.py`, `tests/test_pipeline.py`, and README examples.
- The codebase already uses PEP 695 generics (`class Pipeline[T]`, `class Flow[T, U]`); match existing styles when editing types.
- The pipeline API is immutable and async-iterable: `pipe(source).map(...).flat_map(...).filter(...).collect()` is the primary style; `|` with `op.map`/`op.flat_map`/`op.filter` is mainly for reusable sourceless `Flow` fragments.
- For typed sourceless method-chaining, start with `Flow[T]()`; bare `Flow()` produces `Flow[Any, ...]` because there is no source to infer the input type from.
- Concurrent stages emit values in completion order. Do not reintroduce ordered-output options unless explicitly requested.
- `flowlet.functional` is the execution core; fluent `Pipeline`/`Flow` and `op` should wrap its curried operators rather than duplicating execution logic.
- Concurrent `_map`/`_flat_map` deliberately hand the source to a feeder task and read results off a queue. Do not "simplify" this back to awaiting `anext(source)` inside the driver loop: that makes a stage withhold finished results while the source is slow, which stalls live sources. Racing the pull as a per-item task instead was measured 17-22x slower, because it can only pull one item per event-loop turn.
- Worker and expansion tasks must publish every failure to the queue, `BaseException` included, and additionally re-raise when `_is_cancelling()` — during teardown the driver has stopped reading, so publishing alone loses the exception and the task looks successful. The drivers count outstanding work rather than awaiting each task, so an exception left on a task object is silently dropped and the driver can hang.
- `tests/test_lifecycle.py` is the specification for iterator ownership, not just a test file: one rule table applied to every ownership boundary, stage shape, and sync/async half. Adding a stage or changing cleanup means conforming to it, and a change that is genuinely intended shows up as an edited rule rather than an edited assertion. It pins exception identity, that the close was attempted once, and that no shape hangs; it deliberately does not pin `__context__` structure or exact value counts once a failure is in flight, both of which are implementation detail. It fails 216/216 against `master`, 44 against `1e1b957`, and exactly the four fail-fast cells against `20c640c`.
- Cleanup failures must surface from `aclose()` but must never mask an in-flight error. `_teardown(..., surface=)` encodes that: `GeneratorExit` permits surfacing, a real exception does not. Surfacing unconditionally *erases* the root cause rather than demoting it.
- Inside the queue-based drivers, an owned iterator's close failure is *recorded and also queued* (`_report_cleanup`), never raised at the point of closing. Recording alone covers abandonment but loses fail-fast: against an endless source the consumer is never told. Queuing is suppressed only when an iteration error or cancellation is already propagating. Raising there both masks the read failure and strands the cleanup failure in a queue the consumer may have stopped reading. Only cleanup failures are retained this way — retaining ordinary stage errors would make an abandoned pipeline report work the consumer chose to walk away from.
- Resource ownership is symmetric — every fix has an async and a sync half (`to_async_iter`'s two branches, `_close_async_iter`/`_close_sync_iter`, source and expansion iterators). Three separate review rounds caught a missed half; check the counterpart before calling one done.
- An operator's `apply` must `return` its driver generator, never `async for ... yield` from it. `async for` does not close its iterator when `GeneratorExit` passes through, so a wrapper swallows `aclose()` and defers the driver's cleanup (cancelling tasks, closing the source) to garbage collection. Same reason `to_async_iter` closes the source it wraps in a `finally`, and why `batch` and the `concurrency == 1` fast path own their cleanup. Tests assert this with no `sleep` and no `gc.collect()`; a probe that uses either will pass against broken code.
- `None` is valid data. Do not reintroduce sentinel-value stream termination.
- When changing pipeline behavior, verify focused async tests first, then run the full test suite.
