# flowlet agent guide

flowlet is a small async pipeline library with no dependencies: one package, on Python 3.14 (pinned in `pyproject.toml` and `.python-version`). Most of its complexity is deliberate, and it lives in iterator ownership, concurrency, and failure delivery. Read the relevant docstrings in `functional.py` before simplifying any of it.

## Navigation

- `src/flowlet/functional.py` is the execution core: the curried operators, the stage drivers, and iterator ownership. `_flow.py` (`pipe`, `Pipeline`, `Flow`) and `op.py` wrap those operators and must not duplicate execution logic. `_threading.py` (`in_thread`, `thread_local`) and `_processing.py` (`in_process`) move blocking work off the event loop and manage per-thread resources.
- The `map`/`flat_map`/`filter`/`batch` signatures repeat across `Pipeline`, `Flow`, `op`, and `functional` because each returns a differently typed object. The type system leaves no way around that, so don't abstract it away.
- `tests/test_pipeline.py` covers behavior and the API. `tests/test_lifecycle.py` is the spec for iterator ownership (see Testing).
- Keep `README.md` in sync when public behavior, parameters, or examples change.
- CI (`.github/workflows/ci.yml`) runs `ruff check`, `ruff format --check`, `mypy`, and `pytest`.

## Conventions

- Use PEP 695 generics (`class Pipeline[T]`, `class Flow[T, U = T]`). Start typed sourceless chains with `Flow[T]()`; bare `Flow()` is `Flow[Any, ...]`.
- Pipelines are immutable and lazy. `pipe(source).map(...).collect()` is the main style; `|` with `op.*` is mostly for reusable sourceless `Flow` fragments.
- `None` is ordinary data. Never end a stream with a sentinel value.
- Lines are ruff's default 88 columns. Wrap comments and docstrings to fit, breaking at a punctuation mark when one falls near the end of a line. Keep prose concise and plain-spoken.

## Stage architecture

- Concurrent `map`/`flat_map` hand the source to a feeder task and read results off a queue. Don't move `anext(source)` back into the driver loop: a slow source would then hold back finished results and stall live sources. Pulling each item in its own task is far slower, since it pulls only one item per event-loop turn.
- Worker, expansion, and feeder tasks publish every failure, `BaseException` included, and also re-raise while `_is_cancelling()`. During teardown the driver has stopped reading, so publishing alone would lose the failure. Drivers count outstanding work instead of awaiting tasks, so an exception left on a task object is dropped, and the driver can hang.
- An operator's `apply` must `return` its driver generator, never `async for ... yield` from it. `async for` doesn't close its iterator when `GeneratorExit` passes through, so the driver's cleanup (cancelling tasks, closing the source) would wait for garbage collection.
- Worker and expansion tasks start eagerly (`create_task(..., eager_start=True)`), so work that finishes without suspending costs no extra event-loop turn per item. That was the biggest overhead win measured. It needs event loops and task factories that accept `eager_start` (uvloop 0.23+); target current dependency versions rather than working around old ones.
- `map(concurrency=1)` keeps its sequential fast path whatever `ordered` says. `flat_map` has no sequential path, so ordered `flat_map` at `concurrency=1` really runs the concurrent driver.

## Ordering

- Concurrent stages emit in completion order by default. `ordered=True` is opt-in and must produce what `concurrency=1` would, including holding an ordinary failure until its input position. Only cancellation skips the line (`_bypasses_ordering`), because it's teardown, not an item's outcome; every other `BaseException` waits for its position. Skipping the line starts teardown at once, but teardown still awaits the stage's tasks, so a task that swallows its cancellation, or an `aclose()` that hangs, delays it in either mode.
- Every message carries its input position. The delivery policy (`_Ordered` or `_Unordered`) is picked at construction, so each driver has a single delivery path. Only the `_Fed` hold-or-raise and the capacity lookup check the mode, because they really do differ per mode. Delivery itself (yielding, releasing semaphores, raising positional failures) stays in the drivers: an ordering layer wrapped around a stage's output would sit outside the slot and capacity loop, buffering without bound, and outside `aclose` propagation. Unordered `flat_map`'s `_Value` arm is a deliberate fast path.
- Ordered `flat_map` needs two things. Each expansion gets its own value allowance, because one shared allowance deadlocks: an out-of-turn expansion fills it, and the in-turn one can't publish. And a slot comes back on *delivery*, not when the expansion finishes, or the feeder would start expansions past the bound. Its memory bound is `concurrency * (buffer + 1)`, against `buffer + concurrency` unordered.
- A source failure has no position of its own, so an ordered driver holds it until every started item is delivered. An expansion's cleanup failure does have a position, which is why expansion owners carry `seq`.

## Iterator ownership and cleanup failures

- An ownership boundary is a context manager around the code that reads the iterator (`_OwnedAsync`, `_OwnedSync`, `_OwnedSource`), never an async generator that judges its own iteration. A generator only sees the `GeneratorExit` its close sent, so it would mask the body's real error.
- Cleanup-failure policy is two separate facts; don't merge them into one flag. `_Role` is how a boundary delivers a failure, fixed per boundary: raise it directly, keep it for teardown, or keep it and notify a live driver. `_may_surface(error)` is the per-exit rule: report a cleanup failure only after a normal exit or a plain close, never over a real error or a cancellation, since that would erase the root cause. `_is_cancelling()` plays no part in this.
- Feeders own their source with `_Role.RETAIN`, so in-flight results get delivered before teardown reports the source's close failure. Don't route a feeder's source through `to_async_iter`, which raises close failures directly and would end the input early.
- Every ownership fix has an async half and a sync half (`_OwnedAsync`/`_OwnedSync`, for both source and expansion iterators). Change both. `_OwnedSync` is synchronous on purpose: nothing in it awaits, and `filter` builds one per item.

## Testing

- `tests/test_lifecycle.py` applies one rule table to every ownership boundary, stage shape, and sync/async half. A new stage or cleanup change has to conform to it; an intended behavior change shows up as an edited rule, not an edited assertion. It pins which exception surfaces, a single close attempt, and that nothing hangs. It deliberately leaves `__context__` and the exact output once a failure is in flight unpinned, so guarantees about values delivered around a failure need focused tests in `test_pipeline.py`.
- Prove cleanup without `sleep` or `gc.collect()`; a probe that uses either passes against broken code. Assert closes on plain-class iterators rather than generators, since the cyclic collector can close a generator by itself.
- Synchronize with events or a bounded number of `asyncio.sleep(0)` turns (`settle()`), not wall-clock sleeps or timing-dependent counts.
- When changing pipeline behavior, run the focused async tests first, then the full suite. `pytest` runs with `-s`, so print output isn't captured.
- Measure performance changes interleaved in one process; drift between sessions on the same machine is often bigger than the effect. `benchmark/` has the harnesses (see its `README.md`).

## Commands

- Install: `uv sync`
- Lint: `uv run ruff check .` (`make lint`)
- Format: `uv run ruff check --fix-only . && uv run ruff format .` (`make fformat`; `make format` also skips F401 import cleanup)
- Type-check: `uv run mypy .`
- Test: `uv run pytest` (`make test`); one case: `uv run pytest tests/test_pipeline.py::TestPipelineApi::test_method_chain_collects_results`

When you're done with code changes, run lint, the format check, mypy, and the tests, as CI does.
