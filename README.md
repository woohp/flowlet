# flowlet

`flowlet` is a small async pipeline library for transforming streams with bounded parallelism.

```python
from flowlet import pipe

results = await (
    pipe([1, 2, 3])
    .map(fetch, concurrency=20)
    .flat_map(extract_links)
    .filter(is_valid)
    .map(normalize)
    .collect()
)
```

## Core API

- `pipe(source)` binds an iterable or async iterable source.
- `.map(fn)` transforms one input into one output.
- `.flat_map(fn)` transforms one input into zero or more outputs.
- `.filter(pred)` keeps or drops each input.
- `.then(flow)` appends a reusable flow.
- `.collect()` consumes the pipeline into a list.
- `.run()` drains the pipeline when outputs are intentionally ignored.
- `.for_each(fn)` runs a terminal side effect for each item.

Functions may be sync or async.

## Reusable Flows

Use `Flow` when you want to define a reusable pipeline fragment without binding a source.

```python
from flowlet import Flow, pipe

extract = (
    Flow[Page, Page]()
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
)

links = await pipe(pages).then(extract).collect()
```

There is also constructor sugar for simple one-to-one chains:

```python
from flowlet import flow

transform = flow(fetch, parse, normalize)
items = await pipe(urls).then(transform).collect()
```

## Operator Syntax

`|` is available as an alternative composition syntax.

```python
from flowlet import filter_, flat_map, map_, pipe

items = await (
    pipe(pages)
    | flat_map(find_links)
    | filter_(is_internal)
    | map_(normalize_url)
).collect()
```

## Ordering

Concurrent stages preserve input order by default.

```python
items = await pipe(urls).map(fetch, concurrency=20).collect()
```

Use `ordered=False` when completion order is preferred.

```python
items = await pipe(urls).map(fetch, concurrency=20, ordered=False).collect()
```

## Cardinality

Use the method that matches the stage cardinality.

```python
pipe(items).map(fn)       # one input -> one output
pipe(items).filter(pred)  # one input -> zero or one output
pipe(items).flat_map(fn)  # one input -> zero or more outputs
```

`None` is treated as normal data. Filtering is explicit.
