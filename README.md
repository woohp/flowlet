# flowlet

`flowlet` is a small async pipeline library for transforming streams with bounded parallelism.

## Pipeline API

The pipeline API is the default interface. It is method chaining over a lazy async stream; nothing runs until a terminal method such as `.collect()`, `.for_each()`, or `.run()` is awaited.

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
- `.then(flowlet)` appends a reusable `Flowlet` fragment.
- `.collect()` consumes the pipeline into a list.
- `.for_each(fn)` runs a terminal side effect for each item.
- `.run()` drains the pipeline when outputs are intentionally ignored.

Functions may be sync or async.

## Operator Syntax

The `|` syntax is a pure syntactic alternative to `.then(...)` and method chaining. It does not change execution behavior.

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

`pipe(source) | fragment` is equivalent to `pipe(source).then(fragment)`.

## Reusable Flowlets

`Flowlet` is the reusable sourceless pipeline fragment type. The `op` namespace constructs single-step `Flowlet`s for `|` composition.

Use `op` when you want compact sourceless composition:

```python
from flowlet import op, pipe

extract = (
    op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
)

links = await (pipe(pages) | extract).collect()
```

Use `Flowlet()` when you prefer the same method-chaining style as `Pipeline` or want an explicit annotation:

```python
from flowlet import Flowlet, pipe

extract: Flowlet[Page, str] = (
    Flowlet()
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
)

links = await pipe(pages).then(extract).collect()
```

For simple one-to-one chains, `chain(...)` is map-only sugar:

```python
from flowlet import chain, pipe

transform = chain(fetch, parse, normalize)
items = await pipe(urls).then(transform).collect()
```

`chain(fetch, parse, normalize)` means `.map(fetch).map(parse).map(normalize)`. Use `op.flat_map(...)`, `op.filter(...)`, or `Flowlet()` when any step changes cardinality. `None` returned by a chain step is normal data, not a dropped item.

## Functional API

`flowlet.functional` is an alternative lower-level API. It exposes the curried stream operators that power the pipeline API.

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

Top-level `chain(...)` and `F.chain(...)` live in different namespaces and compose different things: `chain(...)` creates a map-only `Flowlet`, while `F.chain(...)` composes functional stream operators.

## Error Behavior

The default error policy is fail-fast. Exceptions from sources or stages propagate to the caller.

In a concurrent stage, if one in-flight item raises, the pipeline raises and pending sibling tasks in that stage are cancelled. There is no skip-or-recover API yet; use explicit `try`/`except` inside your stage function if you want to convert failures into values or filter them with `.flat_map(...)`.

## Ordering

Concurrent stages preserve input order by default.

```python
items = await pipe(urls).map(fetch, concurrency=20).collect()
```

Use `preserve_order=False` when completion order is preferred.

```python
items = await pipe(urls).map(fetch, concurrency=20, preserve_order=False).collect()
```

## Cardinality

Use the method that matches the stage cardinality.

```python
pipe(items).map(fn)       # one input -> one output
pipe(items).filter(pred)  # one input -> zero or one output
pipe(items).flat_map(fn)  # one input -> zero or more outputs
```

`None` is treated as normal data. Filtering is explicit.

## Source Contract

`pipe(source)` and `F.collect(source)` accept iterables and async iterables. They consume the source lazily. One-shot iterators and generators remain one-shot if reused across multiple pipeline runs.

`.collect()` on an infinite source never completes because it waits to build a complete list. Use async iteration, `.for_each(...)`, or `.run()` for unbounded streams.

Use `.for_each(fn)` when the terminal action is a side effect for each output item. Use `.run()` when side effects are inside the pipeline stages and no per-item action is needed at the terminal - the pipeline is just drained to completion.

```python
await pipe(events).map(write_to_log, concurrency=20).run()
```
