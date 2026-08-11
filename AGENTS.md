# AGENTS.md

## Repo Facts

- Trust `pyproject.toml`, `src/flowlet/__init__.py`, `src/flowlet/_flow.py`, `src/flowlet/functional.py`, `src/flowlet/op.py`, `tests/test_pipeline.py`, `tests/test_lifecycle.py`, and `README.md`.
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
- Concurrent stages emit values in completion order by default. `ordered=True` on `map`/`flat_map`/`filter` opts into input order and must stay opt-in: it is what `concurrency=1` would produce, including holding an *ordinary* failure until its input position, which trades a concurrent stage's liveness for determinism. Cancellation is exempt by design (`_bypasses_ordering`): it bypasses positional ordering and begins teardown immediately rather than waiting for an earlier item, because cancellation is teardown and not an item's outcome. That is not the same as reaching the consumer at once -- `_Stage` still gathers supervised tasks, so a task that swallows its cancellation or an `aclose()` that hangs delays teardown in either mode (measured, identically for both). Every other `BaseException` a stage raises *is* that item's outcome and stays positional. `map(concurrency=1)` must stay on its sequential fast path whichever mode is asked for; `flat_map` has no such path, so ordered at `concurrency=1` is a real configuration of the concurrent driver and the ownership matrix covers it explicitly.
- Ordering rides on a sequence tag: the feeder stamps each item's position, producers publish it on every message, and the ordered driver holds results until their turn. The holding is one shared `_Reorder` (`push`/`drain` message replay) used by both drivers; delivery -- yields, semaphore releases, raising positional failures -- stays in the drivers on purpose, because a reorder layer wrapped *around* a stage's output sits outside the slot/capacity loop (unbounded buffering) and outside `aclose` propagation. Replay measured 3-5% slower on ordered `flat_map`/`filter` than inline drains (a drain generator per message, each message dispatched on receipt and on replay); accepted for a single shared implementation over two that must stay identical. `map` needs no new bound, because a slot is not returned until its result is delivered. Ordered `flat_map` needs two changes that are not optional -- a per-expansion value allowance, since one shared allowance deadlocks (an out-of-turn expansion fills it, and the in-turn one then cannot publish; reproduced before choosing this), and a slot returned on *delivery* rather than on termination, or the feeder starts expansions past the bound (measured: an ordered stage holds up to `concurrency * (buffer + 1)` values, an unordered one `buffer + concurrency`, the `+ 1` being the value an expansion has pulled but not yet published). An expansion's cleanup failure is positional too, which is why the owner carries a position: tagged positionless it is never anyone's turn and falls back to teardown, losing fail-fast. The per-item allowance allocation is not worth optimizing: ordered mode measured 2-9% slower than unordered across `filter`/`flat_map`/`map` at `concurrency=4`.
- `flowlet.functional` is the execution core; fluent `Pipeline`/`Flow` and `op` should wrap its curried operators rather than duplicating execution logic.
- Concurrent `_map`/`_flat_map` deliberately hand the source to a feeder task and read results off a queue. Do not "simplify" this back to awaiting `anext(source)` inside the driver loop: that makes a stage withhold finished results while the source is slow, which stalls live sources. Racing the pull as a per-item task instead was measured 17-22x slower, because it can only pull one item per event-loop turn.
- Worker and expansion tasks must publish every failure to the queue, `BaseException` included, and additionally re-raise when `_is_cancelling()` — during teardown the driver has stopped reading, so publishing alone loses the exception and the task looks successful. The drivers count outstanding work rather than awaiting each task, so an exception left on a task object is silently dropped and the driver can hang.
- `tests/test_lifecycle.py` is the specification for iterator ownership, not just a test file: one rule table applied to every ownership boundary, stage shape, and sync/async half. Adding a stage or changing cleanup means conforming to it, and a change that is genuinely intended shows up as an edited rule rather than an edited assertion. It pins exception identity, that the close was attempted once, and that no shape hangs; it deliberately does not pin `__context__` structure or exact value counts once a failure is in flight, both of which are implementation detail. It fails 216/216 against `master`, 44 against `1e1b957`, and exactly the four fail-fast cells against `20c640c`.
- Cleanup-failure policy is two independent facts, and collapsing them into one flag is what made it hard to keep correct across six sites. `_Role` is *how a boundary delivers* a failure (static, fixed at construction: raise directly / retain for teardown / retain and notify a live driver). `_Exit` is *why the body ended* (dynamic, per exit), and `_Exit.may_surface` is the precedence rule: a cleanup failure is reported only on a normal exit or a plain close, never over a real error or a cancellation. Surfacing unconditionally *erases* the root cause rather than demoting it.
- An ownership boundary must be a context manager around the code that reads the iterator, never an async generator that classifies its own iteration. `__aexit__` is handed the reason the *body* ended; a generator only sees the `GeneratorExit` that `aclose()` sent it, reads ordinary abandonment, and so lets a cleanup failure mask the body's error or send a live notification that cannot be retracted. Both were reproduced before this shape was chosen. `_is_cancelling()` is therefore *not* part of cleanup precedence — it remains only for re-raising a producer task's exception that publishing alone would lose.
- Resource ownership is symmetric — every fix has an async and a sync half (`_OwnedAsync`/`_OwnedSync`, source and expansion iterators). Three separate review rounds caught a missed half; check the counterpart before calling one done. `_OwnedSync` is a *synchronous* context manager on purpose: nothing it does awaits, and the async protocol costs two coroutine allocations per owner, which `filter` pays once per item.
- An operator's `apply` must `return` its driver generator, never `async for ... yield` from it. `async for` does not close its iterator when `GeneratorExit` passes through, so a wrapper swallows `aclose()` and defers the driver's cleanup (cancelling tasks, closing the source) to garbage collection. Tests assert this with no `sleep` and no `gc.collect()`; a probe that uses either will pass against broken code.
- `None` is valid data. Do not reintroduce sentinel-value stream termination.
- When changing pipeline behavior, verify focused async tests first, then run the full test suite.
