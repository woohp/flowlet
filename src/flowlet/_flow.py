from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, overload

import flowlet.functional as functional
from flowlet.functional import Expander, Operator, Predicate, Source


@dataclass(frozen=True)
class Flow[T, U = T]:
    """Reusable, sourceless pipeline fragment.

    A `Flow` stores one or more lazy stream transforms and can be applied to a
    source with `pipe(...).through(flow)` or composed with another flow using `|`.
    """

    _operators: tuple[Operator[Any, Any], ...] = ()

    @overload
    def map[V](
        self,
        fn: Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
    ) -> Flow[T, V]: ...

    @overload
    def map[V](
        self,
        fn: Callable[[U], V],
        *,
        concurrency: int = 1,
    ) -> Flow[T, V]: ...

    def map[V](
        self,
        fn: Callable[[U], V] | Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
    ) -> Flow[T, V]:
        """Return a flow that applies `fn` to each item.

        `fn` may be sync or async. When `concurrency` is greater than one,
        results are emitted as calls complete rather than in input order.
        """
        return self | Flow._from_operator(functional.map(fn, concurrency=concurrency))

    def flat_map[V](
        self,
        fn: Expander[U, V],
        *,
        concurrency: int = 1,
    ) -> Flow[T, V]:
        """Return a flow that expands each item into zero or more outputs.

        `fn` may return an iterable, async iterable, or an awaitable resolving to
        either. Outputs from concurrent expansions are emitted as they arrive.
        """
        return self | Flow._from_operator(functional.flat_map(fn, concurrency=concurrency))

    def filter(
        self,
        pred: Predicate[U],
        *,
        concurrency: int = 1,
    ) -> Flow[T, U]:
        """Return a flow that keeps items where `pred` returns true.

        `pred` may be sync or async. With concurrent predicates, kept items are
        emitted in completion order.
        """
        return self | Flow._from_operator(functional.filter(pred, concurrency=concurrency))

    def batch(self, size: int) -> Flow[T, list[U]]:
        """Return a flow that collects items into lists of up to `size`."""
        return self | Flow._from_operator(functional.batch(size))

    def apply(self, source: Source[T]) -> AsyncIterator[U]:
        """Apply this flow to a source and return an async iterator."""
        return functional.chain(*self._operators)(source)

    def through[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        """Return a new flow with `flow` appended after this one."""
        return Flow((*self._operators, *flow._operators))

    def __or__[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        """Compose two flows using `left | right` syntax."""
        return self.through(flow)

    @staticmethod
    def _from_operator[V, W](operator: Operator[V, W]) -> Flow[V, W]:
        return Flow((operator,))


@dataclass(frozen=True)
class Pipeline[T]:
    """Lazy async pipeline bound to a source.

    Pipeline methods return new pipelines; execution starts only when the
    pipeline is iterated or consumed with a terminal method.
    """

    _source: Source[Any]
    _operators: tuple[Operator[Any, Any], ...] = ()

    @overload
    def map[U](
        self,
        fn: Callable[[T], Awaitable[U]],
        *,
        concurrency: int = 1,
    ) -> Pipeline[U]: ...

    @overload
    def map[U](
        self,
        fn: Callable[[T], U],
        *,
        concurrency: int = 1,
    ) -> Pipeline[U]: ...

    def map[U](
        self,
        fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
        *,
        concurrency: int = 1,
    ) -> Pipeline[U]:
        """Return a pipeline that applies `fn` to each item.

        `fn` may be sync or async. When `concurrency` is greater than one,
        results are emitted as calls complete rather than in input order.
        """
        return self | Flow._from_operator(functional.map(fn, concurrency=concurrency))

    def flat_map[U](
        self,
        fn: Expander[T, U],
        *,
        concurrency: int = 1,
    ) -> Pipeline[U]:
        """Return a pipeline that expands each item into zero or more outputs.

        `fn` may return an iterable, async iterable, or an awaitable resolving to
        either. Outputs from concurrent expansions are emitted as they arrive.
        """
        return self | Flow._from_operator(functional.flat_map(fn, concurrency=concurrency))

    def filter(
        self,
        pred: Predicate[T],
        *,
        concurrency: int = 1,
    ) -> Pipeline[T]:
        """Return a pipeline that keeps items where `pred` returns true.

        `pred` may be sync or async. With concurrent predicates, kept items are
        emitted in completion order.
        """
        return self | Flow._from_operator(functional.filter(pred, concurrency=concurrency))

    def batch(self, size: int) -> Pipeline[list[T]]:
        """Return a pipeline that collects items into lists of up to `size`."""
        return self | Flow._from_operator(functional.batch(size))

    def __aiter__(self) -> AsyncIterator[T]:
        """Iterate over the pipeline results asynchronously."""
        return functional.chain(*self._operators)(self._source)

    async def collect(self) -> list[T]:
        """Consume the pipeline and return all results as a list."""
        return await functional.collect(self)

    async def drain(self) -> None:
        """Consume the pipeline, discarding any yielded values."""
        await functional.drain(self)

    async def for_each(
        self,
        fn: Callable[[T], object] | Callable[[T], Awaitable[object]],
        *,
        concurrency: int = 1,
    ) -> None:
        """Run `fn` for each item and consume the pipeline.

        Use this for terminal side effects. `fn` may be sync or async and runs
        with the requested stage-level concurrency.
        """
        await functional.for_each(self, fn, concurrency=concurrency)

    def through[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        """Return a new pipeline with `flow` appended after current steps."""
        return Pipeline(self._source, (*self._operators, *flow._operators))

    def __or__[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        """Append a flow using `pipeline | flow` syntax."""
        return self.through(flow)


def pipe[T](source: Source[T]) -> Pipeline[T]:
    """Create a lazy pipeline from an iterable or async iterable source."""
    return Pipeline(source)


__all__ = ["Flow", "Pipeline", "pipe"]
