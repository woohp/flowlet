# flowlet

`flowlet` is a small async pipeline library for transforming streams with bounded stage-level parallelism.

## Requirements

`flowlet` requires Python 3.13 or newer.

## Installation

```bash
uv add flowlet
```

or:

```bash
pip install flowlet
```

## Pipeline API

The pipeline API is the default interface. It is method chaining over a lazy async stream; nothing runs until the pipeline is consumed with `async for` or a terminal method such as `.collect()`, or `.drain()`.

```python
from flowlet import pipe

results = await (
    pipe(urls)
    .map(fetch, concurrency=20)
    .flat_map(extract_links)
    .filter(is_valid)
    .map(normalize)
    .collect()
)
```

- `pipe(source)` accepts an `Iterable[T]` or `AsyncIterable[T]`.
- `.map(fn)` transforms one input into one output.
- `.flat_map(fn)` transforms one input into zero or more outputs.
- `.filter(pred)` keeps or drops each input.
- `.batch(size)` collects items into lists of up to `size`.
- `.through(flow)` appends a reusable `Flow` fragment.
- `.collect()` consumes the pipeline into a list.
- `.drain()` consumes the pipeline when outputs are intentionally ignored.

Functions may be sync or async.

Use `in_thread(...)` for blocking synchronous work that should not run on the event loop:

```python
from flowlet import in_thread, pipe

items = await pipe(keys).map(in_thread(load_s3), concurrency=64).collect()
```

`in_thread(fn, limit=16)` adds a wrapper-level throttle. `concurrency` still controls how many items the pipeline stage may have in flight. With `.map(in_thread(fn, limit=16), concurrency=64)`, up to 64 items may be active in the stage while at most 16 wrapped calls are submitted to the executor at once. That `limit` is enforced per wrapped callable per event loop, so the same wrapper can be reused safely across separate `asyncio.run(...)` calls.

Cancellation stops waiting for a threaded call's result, but it does not interrupt synchronous code that is already running in a worker thread.

If you pass both `executor=pool` and `limit=16`, both bounds apply: `limit` throttles this wrapper, while `pool.max_workers` remains the actual thread-pool-wide cap.

If multiple stages should share one thread pool, pass the same executor to each wrapper:

```python
from concurrent.futures import ThreadPoolExecutor
from flowlet import in_thread, pipe

with ThreadPoolExecutor(max_workers=16) as pool:
    items = await (
        pipe(keys)
        .map(in_thread(load_s3, executor=pool, limit=8), concurrency=32)
        .map(in_thread(parse_blob, executor=pool, limit=4), concurrency=32)
        .collect()
    )
```

Pass a `ThreadPoolExecutor` when using `executor=...`. `in_thread(...)` is for blocking thread-based work, not process pools.

Use `in_process(...)` for CPU-bound synchronous work that should run in a process pool:

```python
from concurrent.futures import ProcessPoolExecutor
from flowlet import in_process, pipe

with ProcessPoolExecutor(max_workers=8) as pool:
    items = await pipe(keys).map(in_process(crunch, executor=pool), concurrency=8).collect()
```

Unlike `in_thread(...)`, `in_process(...)` has no default executor. `in_thread(...)` can use asyncio's default thread pool, but process pools must be explicitly started and shut down, so `in_process(fn, executor=pool, limit=...)` always requires a `ProcessPoolExecutor`. For the usual case, set `concurrency` to the process pool size and omit `limit`; use `limit` only when this wrapper should submit fewer calls than the stage or executor would otherwise allow.

The wrapped function, its arguments, and its return values must be pickleable for cross-platform process-pool code. The default start method varies by platform and Python version: `fork` on Linux before Python 3.14 (where local functions and lambdas work), `forkserver` on Linux from Python 3.14 onwards, and `spawn` on macOS and Windows — both `forkserver` and `spawn` require importable module-level functions. Use module-level functions for portable in_process(...) pipelines.

`in_process(...)` does not propagate `contextvars` into workers. `in_thread(...)` can copy context into another thread in the same process, but `contextvars.Context` is not pickleable and cannot be sent to worker processes. Cancellation stops waiting for the process-pool result, but a task that is already running in a `ProcessPoolExecutor` cannot be cancelled individually; use `concurrency`, `limit`, or the executor's worker count as the practical throttles.

## Reusable Flows

`Flow` is the reusable sourceless pipeline fragment type. Use it when you want to name and reuse a transform.

```python
from flowlet import Flow, pipe

extract: Flow[Page, str] = (
    Flow[Page]()
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
)

links = await pipe(pages).through(extract).collect()
```

`Flow[T]()` starts a fragment whose input and current output type are both `T`. Starting from bare `Flow()` is allowed, but type checkers cannot infer the fragment input type from no source. Prefer `Flow[T]()` for typed reusable fragments.

The `|` syntax is optional sugar for `.through(...)`:

```python
links = await (pipe(pages) | extract).collect()
```

## Async Iteration

Pipelines are async iterables. Use `async for` when you do not want to collect every item, especially for unbounded streams.

```python
async for link in pipe(events).map(parse).filter(is_interesting):
    await handle(link)
```

Stop the loop normally when you have enough items:

```python
items = []

async for item in pipe(events).map(parse):
    items.append(item)
    if len(items) == 100:
        break
```

## Operator Syntax

The `op` namespace constructs single-step `Flow`s for compact reusable composition. It is secondary to method chaining and does not change execution behavior.

```python
from flowlet import op, pipe

items = await (
    pipe(pages)
    | op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
).collect()
```

This is equivalent to:

```python
items = await (
    pipe(pages)
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
    .collect()
)
```

`pipe(source) | flow` is equivalent to `pipe(source).through(flow)`.

Use `op` when you want compact sourceless flow composition:

```python
from flowlet import op, pipe

extract = (
    op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
)

links = await (pipe(pages) | extract).collect()
```

## Functional API

`flowlet.functional` is a lower-level API. It exposes the curried stream operators that power `Pipeline`, `Flow`, and `op`.

```python
import flowlet.functional as F

pipeline = F.chain(
    F.map(fetch, concurrency=20),
    F.flat_map(extract_links),
    F.filter(is_valid),
    F.map(normalize),
)

items = await F.collect(pipeline(urls))
```

Each functional operator returns a reusable stream transformer:

```python
fetch_pages = F.map(fetch, concurrency=20)
pages = fetch_pages(urls)
```

Most users should prefer the pipeline API. Use `flowlet.functional` when you specifically want to build or pass around stream-transformer functions.

## Error Behavior

The default error policy is fail-fast. Exceptions from sources or stages propagate to the caller.

In a concurrent stage, if one in-flight item raises, the pipeline raises and pending sibling tasks in that stage are cancelled. There is no skip-or-recover API yet; use explicit `try`/`except` inside your stage function if you want to convert failures into values or filter them with `.flat_map(...)`.

## Concurrency

Concurrency is configured per stage. A pipeline with two `concurrency=20` stages can have work in flight in both stages at the same time as downstream consumption allows.

Concurrent stages emit values in completion order. If you need input order, use `concurrency=1`.

```python
items = await pipe(urls).map(fetch, concurrency=20).collect()
```

With `concurrency > 1`, a faster later item can be yielded before a slower earlier item. Source items are pulled lazily, up to the stage concurrency and downstream demand.

This applies to every concurrent stage, including filters:

```python
items = await pipe([1, 2, 3]).filter(async_pred, concurrency=3).collect()
# May return [3, 1, 2], not necessarily [1, 2, 3].
```

## Cardinality

Use the method that matches the stage cardinality.

```python
pipe(items).map(fn)       # one input -> one output
pipe(items).filter(pred)  # one input -> zero or one output
pipe(items).flat_map(fn)  # one input -> zero or more outputs
pipe(items).batch(size)   # many inputs -> one output (list)
```

`flat_map(fn)` accepts a sync or async function returning an `Iterable[U]` or `AsyncIterable[U]`. It streams each returned iterable; an async expansion can yield values without first finishing the whole expansion. It expects each input to expand into a finite, reasonably small iterable or async iterable. Outputs are buffered internally; very large or infinite expansions may consume unbounded memory. It does not accept a single scalar output; use `.map(fn)` for one-to-one transforms.

With `flat_map(in_thread(fn))`, prefer returning a materialized collection such as `list` or `tuple`. If `fn` returns a lazy generator, the generator object is created in the worker thread but iterated later on the event-loop thread.

`batch(size)` collects items into lists of up to `size`. The last emitted list may be shorter when the source is exhausted with a partial group. This is useful for batching API calls, database inserts, or any operation that benefits from processing items in chunks:

```python
await pipe(records).batch(100).map(bulk_insert).drain()
```

`None` is treated as normal data. Filtering is explicit.

## Source Contract

`pipe(source)` and `F.collect(source)` accept iterables and async iterables. They consume the source lazily. One-shot iterators and generators remain one-shot if reused across multiple pipeline runs.

`.collect()` on an infinite source never completes because it waits to build a complete list. Use async iteration or `.drain()` for unbounded streams.

Use `.drain()` when side effects are inside the pipeline stages and no per-item action is needed at the terminal - the pipeline is just drained to completion.

```python
await pipe(events).map(write_to_log, concurrency=20).drain()
```
