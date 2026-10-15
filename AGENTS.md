# flowlet agent guide

flowlet is a small, dependency-free async pipeline library: one package, Python 3.13 (pinned in `pyproject.toml` and `.python-version`). Most of its complexity is deliberate and concentrated in iterator ownership, concurrency, and failure delivery; read the relevant docstrings in `functional.py` before simplifying any of it.

## Navigation

- `src/flowlet/functional.py` is the execution core: curried operators, the stage drivers, and ownership. `_flow.py` (`pipe`, `Pipeline`, `Flow`) and `op.py` wrap its operators and must not duplicate execution logic. `_threading.py` (`in_thread`, `thread_local`) and `_processing.py` (`in_process`) move blocking work off the event loop and manage per-thread resources.
- The `map`/`flat_map`/`filter`/`batch` signatures repeat across `Pipeline`, `Flow`, `op`, and `functional` because each returns a differently typed object; the typing makes this duplication unavoidable, so do not abstract it away.
- `tests/test_pipeline.py` covers behavior and API. `tests/test_lifecycle.py` is the specification for iterator ownership (see Testing).
- Keep `README.md` in sync when public behavior, parameters, or examples change.
- CI (`.github/workflows/ci.yml`) runs `ruff check`, `ruff format --check`, `mypy`, and `pytest`.

## API conventions

- Use PEP 695 generics (`class Pipeline[T]`, `class Flow[T, U = T]`). For typed sourceless chains start with `Flow[T]()`; bare `Flow()` is `Flow[Any, ...]`.
- Pipelines are immutable and lazy. `pipe(source).map(...).collect()` is the primary style; `|` with `op.*` is mainly for reusable sourceless `Flow` fragments.
- `None` is valid data. Never terminate streams with a sentinel value.

## Stage architecture

- Concurrent `map`/`flat_map` hand the source to a feeder task and read results off a queue. Do not move `anext(source)` back into the driver loop: a slow source would then withhold finished results and stall live sources. Racing the pull as a per-item task is far slower, since it pulls one item per event-loop turn.
- Worker, expansion, and feeder tasks publish every failure, `BaseException` included, and also re-raise while `_is_cancelling()`: during teardown the driver has stopped reading, so publishing alone would lose it. Drivers count outstanding work instead of awaiting tasks, so an exception left on a task object is dropped and the driver can hang.
- An operator's `apply` must `return` its driver generator, never `async for ... yield` from it. `async for` does not close its iterator when `GeneratorExit` passes through, so the driver's cleanup (cancelling tasks, closing the source) would wait for garbage collection.
- `map(concurrency=1)` keeps its sequential fast path whatever `ordered` says. `flat_map` has no sequential path, so ordered `flat_map` at `concurrency=1` is a real configuration of the concurrent driver.

## Ordering

- Concurrent stages emit in completion order by default. `ordered=True` is opt-in and must produce what `concurrency=1` would, including holding an ordinary failure until its input position. Cancellation alone bypasses ordering (`_bypasses_ordering`): it is teardown, not an item's outcome. Every other `BaseException` is positional. Bypassing starts teardown at once but does not make it instant: `_Stage` still awaits supervised tasks, so a task that swallows its cancellation or an `aclose()` that hangs delays teardown in either mode.
- Each message carries its input position. The delivery policy (`_Ordered` / `_Unordered`) is chosen at construction, so each driver has a single delivery path; only the `_Fed` hold-or-raise and the capacity lookup stay mode-aware, because they genuinely differ per mode. Delivery itself (yields, semaphore releases, raising positional failures) stays in the drivers: an ordering layer wrapped around a stage's output sits outside the slot/capacity loop (unbounded buffering) and outside `aclose` propagation. Unordered `flat_map`'s `_Value` arm is a deliberate fast path.
- Ordered `flat_map` needs both a per-expansion value allowance (one shared allowance deadlocks: an out-of-turn expansion fills it and the in-turn one cannot publish) and a slot returned on *delivery*, not termination (otherwise the feeder starts expansions past the bound). Its memory bound is `concurrency * (buffer + 1)` against `buffer + concurrency` unordered.
- A source failure has no position of its own; an ordered driver holds it until every started item is delivered. An expansion's cleanup failure is positional, which is why expansion owners carry `seq`.

## Iterator ownership and cleanup failures

- An ownership boundary is a context manager around the code that reads the iterator (`_OwnedAsync`, `_OwnedSync`, `_OwnedSource`), never an async generator that classifies its own iteration: a generator sees only the `GeneratorExit` its close sent and so masks the body's real error.
- Cleanup-failure policy is two independent facts; do not merge them into one flag. `_Role` is how a boundary delivers (static: raise directly, retain for teardown, or retain and notify a live driver). `_may_surface(error)` is the per-exit precedence rule: a cleanup failure is reported only after a normal exit or a plain close, never over a real error or a cancellation, because that would erase the root cause. `_is_cancelling()` is not part of this precedence.
- Feeders own their source with `_Role.RETAIN`, so in-flight results are delivered before teardown reports a source's close failure. Do not route a feeder's source through `to_async_iter`, which closes with direct delivery and would end the input early.
- Every ownership fix has an async and a sync half (`_OwnedAsync`/`_OwnedSync`, source and expansion iterators); change both. `_OwnedSync` is synchronous on purpose: nothing in it awaits, and `filter` builds one per item.

## Testing

- `tests/test_lifecycle.py` is one rule table applied to every ownership boundary, stage shape, and sync/async half. A new stage or cleanup change must conform to it, and an intended behavior change shows up as an edited rule, not an edited assertion. It pins exception identity, a single close attempt, and that nothing hangs. It deliberately does not pin `__context__` or exact output once a failure is in flight, so guarantees about values delivered around a failure need focused tests in `test_pipeline.py`.
- Prove cleanup without `sleep` or `gc.collect()`; a probe using either passes against broken code. Prefer plain-class iterators over generators when asserting closes, since the cyclic collector can close a generator on its own.
- Synchronize with events or bounded `asyncio.sleep(0)` turns (`settle()`) instead of wall-clock sleeps and timing-dependent counts.
- When changing pipeline behavior, run the focused async tests first, then the full suite. `pytest` runs with `-s`, so print output is not captured.
- Measure performance changes interleaved in one process; drift between sessions on one machine is often larger than the effect being measured. `benchmark/` has the harnesses (see its `README.md`).

## Commands

- Install: `uv sync`
- Lint: `uv run ruff check .` (`make lint`)
- Format: `uv run ruff check --fix-only . && uv run ruff format .` (`make fformat`; `make format` also skips F401 import cleanup)
- Type-check: `uv run mypy .`
- Test: `uv run pytest` (`make test`); one case: `uv run pytest tests/test_pipeline.py::TestPipelineApi::test_method_chain_collects_results`

When done with code changes, run lint, format check, mypy, and the tests, as CI does.
