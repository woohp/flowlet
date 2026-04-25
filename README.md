# flowlet

`flowlet` is a small async pipeline library for transforming streams with bounded stage-level parallelism.

## Pipeline API

The pipeline API is the default interface. It is method chaining over a lazy async stream; nothing runs until the pipeline is consumed with `async for` or a terminal method such as `.collect()`, `.for_each()`, or `.drain()`.

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
- `.through(flow)` appends a reusable `Flow` fragment.
- `.collect()` consumes the pipeline into a list.
- `.for_each(fn, concurrency=...)` runs a terminal side effect for each item.
- `.drain()` consumes the pipeline when outputs are intentionally ignored.

Functions may be sync or async.

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

Concurrent stages emit values in completion order.

```python
items = await pipe(urls).map(fetch, concurrency=20).collect()
```

With `concurrency > 1`, a faster later item can be yielded before a slower earlier item. Source items are pulled lazily, up to the stage concurrency and downstream demand.

`.for_each(...)` is implemented like `.map(fn, concurrency=...).drain()`, so terminal callbacks also run concurrently when `concurrency > 1`. If the callback mutates external state and that side effect must happen strictly in input order, use `concurrency=1`.

## Cardinality

Use the method that matches the stage cardinality.

```python
pipe(items).map(fn)       # one input -> one output
pipe(items).filter(pred)  # one input -> zero or one output
pipe(items).flat_map(fn)  # one input -> zero or more outputs
```

`flat_map(fn)` accepts a sync or async function returning an `Iterable[U]` or `AsyncIterable[U]`. It streams each returned iterable; an async expansion can yield values without first finishing the whole expansion. It does not accept a single scalar output; use `.map(fn)` for one-to-one transforms.

`None` is treated as normal data. Filtering is explicit.

## Source Contract

`pipe(source)` and `F.collect(source)` accept iterables and async iterables. They consume the source lazily. One-shot iterators and generators remain one-shot if reused across multiple pipeline runs.

`.collect()` on an infinite source never completes because it waits to build a complete list. Use async iteration, `.for_each(...)`, or `.drain()` for unbounded streams.

Use `.for_each(fn, concurrency=...)` when the terminal action is a side effect for each output item. Use `.drain()` when side effects are inside the pipeline stages and no per-item action is needed at the terminal - the pipeline is just drained to completion.

```python
await pipe(events).map(write_to_log, concurrency=20).drain()
```
