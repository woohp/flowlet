# flowlet

`flowlet` is an async pipeline library for transforming streams with bounded, per-stage concurrency.

Give it some data, add a few steps, and consume the results. Slow steps can run concurrently without turning the code into a tangle of queues and tasks.

## Requirements

`flowlet` requires Python 3.13 or newer.

## Installation

```bash
pip install flowlet
```

## Quick start

```python
import asyncio
from flowlet import pipe


async def main():
    result = await (
        pipe([1, 2, 3, 4, 5])
        .map(lambda n: n * 2)
        .filter(lambda n: n > 5)
        .collect()
    )

    print(result)


asyncio.run(main())
```

Output:

```python
[6, 8, 10]
```

That is the main API: `pipe(source)`, chain stages like `.map(...)`, then consume with `.collect()`, `.drain()`, or `async for`.

**Pipelines are lazy.** Nothing runs until you consume the results with a collector or an `async for` loop.

## Concurrency and order

Concurrency is configured per stage.

```python
items = await pipe(urls).map(fetch, concurrency=20).collect()
```

- **`concurrency=1` (Default)**: Stages process items one at a time. The next item is pulled from the source only after the current one is yielded downstream.
- **`concurrency > 1`**: The stage eagerly starts up to `concurrency` calls at once and yields results as they complete. Backpressure is implicit: if the downstream consumer is slow, the stage pauses and stops pulling from the source.

**Concurrent stages emit values in completion order.** A faster later item can be yielded before a slower earlier item. If you need to preserve input order, use `concurrency=1`.

```python
# May return [3, 1, 2], not necessarily [1, 2, 3]
items = await pipe([1, 2, 3]).filter(async_pred, concurrency=3).collect()
```

## Pipeline stages

### Transform items with `map`

Use `.map(...)` when each input produces exactly one output.

```python
names = await pipe(users).map(lambda user: user.name).collect()
```

The function may be async:

```python
pages = await pipe(urls).map(fetch_page, concurrency=20).collect()
```

### Filter items with `filter`

Use `.filter(...)` when each input should be kept or dropped.

```python
active = await pipe(users).filter(lambda user: user.is_active).collect()
```

### Expand items with `flat_map`

Use `.flat_map(...)` when each input can produce zero, one, or many outputs.

```python
links = await pipe(pages).flat_map(extract_links).collect()
```

`flat_map(fn)` accepts a sync or async function returning an iterable or async iterable. Each returned iterable is streamed into the pipeline; an async expansion can yield values before the whole expansion has finished.

Use `.map(fn)` instead if `fn` returns a single scalar value.

`None` is treated as normal data. Filtering is always explicit.

### Batch items with `batch`

Use `.batch(size)` to group items into lists of up to `size`.

```python
await pipe(records).batch(100).map(bulk_insert).drain()
```

```python
await pipe([1, 2, 3, 4, 5]).batch(2).collect()
# [[1, 2], [3, 4], [5]]
```

The last emitted list may be shorter when the source is exhausted with a partial group. Batching is useful for API calls, database inserts, and any operation that benefits from processing items in chunks.

### Consume with `async for`

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

### Drain for side effects

Use `.drain()` when the pipeline stages do the useful work and the final output is not needed.

```python
await pipe(events).map(write_to_log, concurrency=20).drain()
```

## API reference

| API | Purpose | Cardinality |
| :--- | :--- | :--- |
| `pipe(source)` | Start a pipeline | — |
| `.map(fn)` | Transform each item | 1 → 1 |
| `.filter(pred)` | Keep or drop items | 1 → 0..1 |
| `.flat_map(fn)` | Expand into multiple items | 1 → 0..N |
| `.batch(size)` | Group items into lists | N → 1 |
| `.through(flow)` | Apply a reusable `Flow` | Varies |
| `.collect()` | Consume into a `list` | Consumer |
| `.drain()` | Consume and discard output | Consumer |

`flat_map(fn)` holds at most `buffer` expanded values (default 256) for a consumer that has not taken them yet. Once that buffer is full, expansions pause until the consumer catches up, so a large expansion streams rather than materializing in memory.

```python
# Wide expansions go faster with a larger buffer, at the cost of holding
# more values in memory; a smaller buffer bounds memory more tightly.
rows = pipe(files).flat_map(parse_rows, concurrency=8, buffer=2048)
```

With `flat_map(in_thread(fn))`, prefer returning a materialized collection such as `list` or `tuple`. If `fn` returns a lazy generator, the generator object is created in the worker thread but iterated later on the event-loop thread.

## Composition and reuse

`Flow` is a sourceless pipeline fragment. Use it to name and reuse transformations.

```python
from flowlet import Flow, pipe

# Define a reusable flow
# Flow[InputType]() starts a fragment with the given input type
extract: Flow[Page, str] = (
    Flow[Page]()
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
)

links = await pipe(pages).through(extract).collect()
```

### Type hinting

- `Flow[T]()` starts a fragment where the input is type `T`.
- `Flow[T, U]` is the type of a fragment that takes `T` and produces `U`.
- Bare `Flow()` produces `Flow[Any, Any]`. Prefer `Flow[T]()` so type checkers can verify your pipeline.

## Blocking synchronous work

Do not run blocking synchronous work directly on the event loop. Wrap it with `in_thread(...)` or `in_process(...)` depending on the kind of work.

### Blocking I/O with `in_thread`

Use `in_thread(...)` for blocking synchronous work that should run in a thread, such as filesystem calls, blocking SDKs, or blocking network clients.

```python
from flowlet import in_thread, pipe

items = await pipe(keys).map(in_thread(load_s3), concurrency=64).collect()
```

### Concurrency and executors

When using `in_thread` or `in_process`, the number of active calls is bounded by:

- **`concurrency`**: How many items the pipeline stage pulls from the source.
- **`executor`**: The capacity of the underlying thread or process pool.

The effective amount of blocking work is capped by the smaller of the stage concurrency and the executor's worker capacity.

Cancellation stops waiting for a threaded call's result, but it does not interrupt synchronous code that is already running in a worker thread.

If multiple stages should share one thread pool, pass the same executor to each wrapper:

```python
from concurrent.futures import ThreadPoolExecutor
from flowlet import in_thread, pipe

with ThreadPoolExecutor(max_workers=16) as pool:
    items = await (
        pipe(keys)
        .map(in_thread(load_s3, executor=pool), concurrency=8)
        .map(in_thread(parse_blob, executor=pool), concurrency=4)
        .collect()
    )
```

Pass a `ThreadPoolExecutor` when using `executor=...`. `in_thread(...)` is for blocking thread-based work, not process pools.

### Per-thread resources with `thread_local`

Use `thread_local` for blocking clients or sessions that should be created once per worker thread and reused by later tasks on that same thread.

```python
from flowlet import in_thread, pipe, thread_local

@thread_local
def s3():
    client = boto3.client("s3", region_name="us-east-1")
    try:
        yield client
    finally:
        client.close()

def load_s3(key):
    return s3().get_object(Bucket="my-bucket", Key=key)

items = await pipe(keys).map(in_thread(load_s3), concurrency=64).collect()
```

Plain factories are also supported when no cleanup is needed. For generator factories, the first yielded value is cached per calling thread; the generator is never resumed and is closed during teardown. `s3.close()` explicitly closes only the current thread's resource and clears that thread's cache.

To clean up worker resources at a defined point, use an explicit `ThreadPoolExecutor` context manager. When the context exits, the pool shuts down, its worker threads exit, and the per-thread generators created on those workers are closed:

```python
from concurrent.futures import ThreadPoolExecutor
from flowlet import in_thread, pipe, thread_local

@thread_local
def session():
    client = HttpClient()
    try:
        yield client
    finally:
        # Runs once per worker thread as the pool shuts down.
        client.close()

def fetch(url):
    return session().get(url)

with ThreadPoolExecutor(max_workers=16) as pool:
    pages = await pipe(urls).map(in_thread(fetch, executor=pool), concurrency=64).collect()
# after the with block, pool threads have exited and their sessions are closed
```

This only cleans up resources initialized on that executor's worker threads. Resources initialized on other threads, such as asyncio's default thread pool, are unaffected until those threads exit or call `.close()` themselves.

### CPU-bound work with `in_process`

Use `in_process(...)` for CPU-bound synchronous work that should run in a process pool.

```python
from concurrent.futures import ProcessPoolExecutor
from flowlet import in_process, pipe

with ProcessPoolExecutor(max_workers=8) as pool:
    items = await pipe(keys).map(in_process(crunch, executor=pool), concurrency=8).collect()
```

Unlike `in_thread(...)`, which can fall back to asyncio's default thread pool, `in_process(...)` always requires an explicit `ProcessPoolExecutor` because process pools must be created and shut down explicitly.

The wrapped function, its arguments, and its return values must be pickleable for cross-platform process-pool code. The default start method varies by platform and Python version:

- Linux before Python 3.14: `fork`, where local functions and lambdas work.
- Linux from Python 3.14 onward: `forkserver`.
- macOS and Windows: `spawn`.

Both `forkserver` and `spawn` require importable module-level functions. Use module-level functions for portable `in_process(...)` pipelines.

`in_process(...)` does not propagate `contextvars` into workers. `in_thread(...)` can copy context into another thread in the same process, but `contextvars.Context` is not pickleable and cannot be sent to worker processes.

Cancellation stops waiting for the process-pool result, but a task that is already running in a `ProcessPoolExecutor` cannot be cancelled individually. Use `concurrency` and the executor's worker count as the practical throttles.

## Pipe operator syntax

If you prefer an operator-based style, you can use the `|` operator with the `op` namespace. This is a stylistic alternative to fluent method chaining and does not change execution behavior.

```python
from flowlet import op, pipe

items = await (
    pipe(pages)
    | op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
).collect()
```

`pipe(source) | flow` is equivalent to `pipe(source).through(flow)`.

The `op` namespace constructs single-step `Flow`s. Composing them with `|` produces a multi-step `Flow` that can be named and reused:

```python
extract = (
    op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
)

links = await (pipe(pages) | extract).collect()
```

## Functional API

For users who prefer a functional style over fluent method chaining, `flowlet.functional` provides curried operators that can be composed using `chain`.

```python
import flowlet.functional as F

# Build a reusable transformer function
process = F.chain(
    F.map(fetch, concurrency=20),
    F.flat_map(extract_links),
    F.filter(is_valid),
)

# Apply it to a source
items = await F.collect(process(urls))
```

Each functional operator is a standalone transformer function:

```python
fetch_pages = F.map(fetch, concurrency=20)
pages = fetch_pages(urls)  # Returns an AsyncIterator
```

## Behavior reference

### Errors

The default policy is **fail-fast**. If a stage raises an exception, the pipeline raises immediately and pending tasks are cancelled.

To handle errors without stopping the pipeline, use `try`/`except` inside your stage function. Use `.flat_map(...)` so failures can produce zero outputs:

```python
async def safe_fetch(url):
    try:
        return [await fetch(url)]
    except Exception:
        return []

items = await pipe(urls).flat_map(safe_fetch, concurrency=20).collect()
```

### Sources

`pipe(source)` and `F.collect(source)` accept iterables and async iterables. They consume the source lazily.

One-shot iterators and generators remain one-shot if reused across multiple pipeline runs.

`.collect()` on an infinite source never completes because it waits to build a complete list. Use async iteration or `.drain()` for unbounded streams.
