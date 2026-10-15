# flowlet

`flowlet` is an async pipeline library for transforming streams with bounded, per-stage concurrency.

Give it some data, add a few steps, and consume the results. Slow steps can run concurrently without turning your code into a tangle of queues and tasks.

## Requirements

`flowlet` requires Python 3.14 or newer. Concurrent stages create their tasks with `eager_start=True`, so a custom event loop or task factory has to accept that argument. For uvloop, that means 0.23 or newer.

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

That's the core API: start with `pipe(source)`, chain stages like `.map(...)`, then consume with `.collect()`, `.drain()`, or `async for`.

**Pipelines are lazy.** Nothing runs until you consume the results.

## Concurrency and order

Concurrency is set per stage.

```python
items = await pipe(urls).map(fetch, concurrency=20).collect()
```

- **`concurrency=1` (the default)**: the stage handles one item at a time, and pulls the next item from the source only after the current one has gone downstream.
- **`concurrency > 1`**: the stage runs up to `concurrency` calls at once and yields results as they finish. Backpressure is built in: if the consumer is slow, the stage pauses and stops pulling from the source.

**Concurrent stages emit values in completion order.** A fast later item can come out before a slow earlier one.

```python
# May return [3, 1, 2], not necessarily [1, 2, 3]
items = await pipe([1, 2, 3]).filter(async_pred, concurrency=3).collect()
```

Pass `ordered=True` to `map`, `flat_map`, or `filter` to get input order back while still running concurrently. The output is then exactly what `concurrency=1` would produce: same values, same order, and any ordinary failure at the same point. Only the timing differs. Cancellation is the one exception. A `CancelledError` doesn't wait its turn, because it means the stage is shutting down, not that an item finished.

```python
# Always [1, 2, 3], with three predicates still running at once
items = await pipe([1, 2, 3]).filter(async_pred, concurrency=3, ordered=True).collect()
```

Two consequences are worth knowing before you opt in:

- **A failure waits its turn.** If item 1 is slow and item 2 fails, an ordered stage keeps waiting on item 1, where an unordered one would raise right away. Cancellation doesn't wait. Either way, shutdown waits for the stage's tasks, so a task that ignores cancellation delays it in both modes.
- **Ordered `flat_map` holds more.** Each running expansion gets its own buffer, so the stage can hold up to `concurrency * (buffer + 1)` values instead of `buffer + concurrency`. And an expansion that never ends blocks every item behind it, so ordered mode suits expansions that finish.

## Pipeline stages

### Transform items with `map`

Use `.map(...)` when each input produces exactly one output.

```python
names = await pipe(users).map(lambda user: user.name).collect()
```

The function can be async:

```python
pages = await pipe(urls).map(fetch_page, concurrency=20).collect()
```

### Filter items with `filter`

Use `.filter(...)` to keep or drop each input.

```python
active = await pipe(users).filter(lambda user: user.is_active).collect()
```

### Expand items with `flat_map`

Use `.flat_map(...)` when each input can produce zero, one, or many outputs.

```python
links = await pipe(pages).flat_map(extract_links).collect()
```

`flat_map(fn)` takes a sync or async function that returns an iterable or async iterable. Each one streams into the pipeline, so an async expansion can yield values before it finishes.

If `fn` returns a single value, use `.map(fn)` instead.

`None` is ordinary data; to drop items, filter them explicitly.

### Batch items with `batch`

Use `.batch(size)` to group items into lists of up to `size`.

```python
await pipe(records).batch(100).map(bulk_insert).drain()
```

```python
await pipe([1, 2, 3, 4, 5]).batch(2).collect()
# [[1, 2], [3, 4], [5]]
```

The last list can be shorter if the source runs out mid-group. Batching helps with API calls, database inserts, and anything else that's cheaper in chunks.

### Consume with `async for`

Pipelines are async iterables. Use `async for` when you don't want to collect everything, especially for unbounded streams.

```python
async for link in pipe(events).map(parse).filter(is_interesting):
    await handle(link)
```

To stop early, just break out of the loop:

```python
items = []

async for item in pipe(events).map(parse):
    items.append(item)
    if len(items) == 100:
        break
```

### Drain for side effects

Use `.drain()` when the stages do the useful work and you don't need the output.

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

`flat_map(fn)` holds at most `buffer` expanded values (256 by default) that the consumer hasn't taken yet. Once that's full, expansions pause until the consumer catches up, so a large expansion streams instead of piling up in memory.

```python
# A larger buffer speeds up wide expansions but holds more values in memory;
# a smaller one keeps memory tighter.
rows = pipe(files).flat_map(parse_rows, concurrency=8, buffer=2048)
```

With `flat_map(in_thread(fn))`, have `fn` return a materialized collection like a `list` or `tuple`. If it returns a lazy generator, the generator is created in the worker thread but iterated later, on the event loop.

## Composition and reuse

A `Flow` is a pipeline fragment without a source. Use it to name and reuse a series of steps.

```python
from flowlet import Flow, pipe

# Flow[Page]() starts a fragment that takes Page items.
extract: Flow[Page, str] = (
    Flow[Page]()
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
)

links = await pipe(pages).through(extract).collect()
```

### Type hinting

- `Flow[T]()` starts a fragment whose input is `T`.
- `Flow[T, U]` is the type of a fragment that takes `T` and produces `U`.
- Bare `Flow()` is `Flow[Any, Any]`. Prefer `Flow[T]()` so type checkers can check your pipeline.

## Blocking synchronous work

Don't run blocking sync code directly on the event loop. Wrap it with `in_thread(...)` or `in_process(...)`, depending on the kind of work.

### Blocking I/O with `in_thread`

Use `in_thread(...)` for blocking calls that should run in a thread, such as filesystem access, blocking SDKs, or blocking network clients.

```python
from flowlet import in_thread, pipe

items = await pipe(keys).map(in_thread(load_s3), concurrency=64).collect()
```

### Concurrency and executors

With `in_thread` or `in_process`, two things limit how many calls run at once:

- **`concurrency`**: how many items the stage works on at a time.
- **`executor`**: how many workers the thread or process pool has.

Whichever is smaller wins.

Cancelling stops waiting for a threaded call, but it can't interrupt sync code that's already running in a worker thread.

To share one thread pool across stages, pass the same executor to each wrapper:

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

`executor=...` takes a `ThreadPoolExecutor`; `in_thread(...)` is for threads, not process pools.

### Per-thread resources with `thread_local`

Use `thread_local` for blocking clients or sessions that should be created once per worker thread and reused by later calls on that thread.

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

Plain factories work too, when no cleanup is needed. With a generator factory, the first value it yields is cached per thread. The generator is never resumed; it's closed by `.close()` or when its thread exits. A cleanup error at thread exit has no caller to go to, so Python reports it through `sys.unraisablehook`. `s3.close()` closes only the current thread's resource and clears that thread's cache.

To clean up worker resources at a known point, manage the `ThreadPoolExecutor` with a `with` block. When the block exits, the pool shuts down, its threads exit, and the per-thread generators they created are closed:

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
# After the with block, the pool's threads have exited and their sessions are closed.
```

This only covers resources created on that executor's threads. Resources created on other threads, such as asyncio's default pool, stay open until those threads exit or call `.close()` themselves.

### CPU-bound work with `in_process`

Use `in_process(...)` for CPU-bound sync work that should run in a process pool.

```python
from concurrent.futures import ProcessPoolExecutor
from flowlet import in_process, pipe

with ProcessPoolExecutor(max_workers=8) as pool:
    items = await pipe(keys).map(in_process(crunch, executor=pool), concurrency=8).collect()
```

Unlike `in_thread(...)`, which can fall back to asyncio's default thread pool, `in_process(...)` always needs an explicit `ProcessPoolExecutor`, since process pools have to be created and shut down explicitly.

The default start method is `forkserver` on Linux and `spawn` on macOS and Windows, and neither can run lambdas or locally defined functions. So the function must be defined at module level, and it, its arguments, and its return value must be picklable.

`in_process(...)` doesn't carry `contextvars` into workers. `in_thread(...)` can copy the context into another thread, but a `contextvars.Context` can't be pickled, so it can't reach another process.

Cancelling stops waiting for the result, but a call already running in a `ProcessPoolExecutor` can't be cancelled on its own. Use `concurrency` and the pool's worker count to throttle.

## Pipe operator syntax

If you prefer operators, use `|` with the `op` namespace. It's purely a style choice; execution is the same as with method chaining.

```python
from flowlet import op, pipe

items = await (
    pipe(pages)
    | op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
).collect()
```

`pipe(source) | flow` is the same as `pipe(source).through(flow)`.

Each `op` function builds a one-step `Flow`. Chaining them with `|` gives a multi-step `Flow` you can name and reuse:

```python
extract = (
    op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
)

links = await (pipe(pages) | extract).collect()
```

## Functional API

If you'd rather use plain functions than method chaining, `flowlet.functional` provides curried operators that compose with `chain`.

```python
import flowlet.functional as F

# Build a reusable transform
process = F.chain(
    F.map(fetch, concurrency=20),
    F.flat_map(extract_links),
    F.filter(is_valid),
)

# Apply it to a source
items = await F.collect(process(urls))
```

Each functional operator is a standalone transform:

```python
fetch_pages = F.map(fetch, concurrency=20)
pages = fetch_pages(urls)  # an AsyncIterator
```

## Behavior reference

### Errors

Pipelines **fail fast**. If a stage raises, the pipeline raises right away and cancels any pending work.

To handle errors without stopping the pipeline, catch them inside your stage function. With `.flat_map(...)`, a failure can produce zero outputs:

```python
async def safe_fetch(url):
    try:
        return [await fetch(url)]
    except Exception:
        return []

items = await pipe(urls).flat_map(safe_fetch, concurrency=20).collect()
```

### Sources

`pipe(source)` and `F.collect(source)` accept iterables and async iterables, and read them lazily.

A one-shot iterator or generator stays one-shot: run a pipeline over it twice, and the second run sees nothing.

`.collect()` never finishes on an infinite source, since it waits to build the whole list. Use `async for` or `.drain()` for unbounded streams.
