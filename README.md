# flowlet

`flowlet` is a small async pipeline library for transforming streams with bounded parallelism.

## Fluent API

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

- `pipe(source)` binds an iterable or async iterable source.
- `.map(fn)` transforms one input into one output.
- `.flat_map(fn)` transforms one input into zero or more outputs.
- `.filter(pred)` keeps or drops each input.
- `.then(flowlet)` appends a reusable flowlet.
- `.collect()` consumes the pipeline into a list.
- `.run()` drains the pipeline when outputs are intentionally ignored.
- `.for_each(fn)` runs a terminal side effect for each item.

Functions may be sync or async.

## Reusable Flowlets

Use `Flowlet` to define a reusable pipeline fragment without binding a source.

```python
from flowlet import Flowlet, pipe

extract = (
    Flowlet[Page, Page]()
    .flat_map(find_links)
    .filter(is_internal)
    .map(normalize_url)
)

links = await pipe(pages).then(extract).collect()
```

There is constructor sugar for simple one-to-one chains:

```python
from flowlet import flowlet, pipe

transform = flowlet(fetch, parse, normalize)
items = await pipe(urls).then(transform).collect()
```

## Operator Syntax

`|` is available as an alternative fluent composition syntax through `flowlet.op`.

```python
from flowlet import op, pipe

items = await (
    pipe(pages)
    | op.flat_map(find_links)
    | op.filter(is_internal)
    | op.map(normalize_url)
).collect()
```

## Functional API

`flowlet.functional` exposes the curried stream operators that power the fluent API.

```python
import flowlet.functional as F

pipeline = F.compose(
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
