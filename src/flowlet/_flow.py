from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, overload

import flowlet.functional as functional
from flowlet.functional import Expander, Operator, Predicate, Source


@dataclass(frozen=True)
class Flow[T, U = T]:
    """A reusable pipeline fragment that has no source yet.

    Apply it with `pipe(source).through(flow)`, or chain flows with `|`.
    """

    _operators: tuple[Operator[Any, Any], ...] = ()

    @overload
    def map[V](
        self,
        fn: Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Flow[T, V]: ...

    @overload
    def map[V](
        self,
        fn: Callable[[U], V],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Flow[T, V]: ...

    def map[V](
        self,
        fn: Callable[[U], V] | Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Flow[T, V]:
        """Apply `fn` to each item.

        `fn` can be sync or async. With `concurrency > 1`, results arrive as calls
        finish. Use `ordered=True` to deliver them in input order instead.
        """
        return self | Flow._from_operator(
            functional.map(fn, concurrency=concurrency, ordered=ordered)
        )

    def flat_map[V](
        self,
        fn: Expander[U, V],
        *,
        concurrency: int = 1,
        buffer: int = functional.DEFAULT_BUFFER,
        ordered: bool = False,
    ) -> Flow[T, V]:
        """Expand each item into zero or more items.

        `fn` can return an iterable, an async iterable, or an awaitable of either.
        Concurrent expansions interleave their values as they arrive.

        `buffer` limits values waiting for the consumer. With `ordered=True`,
        expansions are delivered in input order, each with its own buffer.
        The stage can then hold up to `concurrency * (buffer + 1)` values,
        compared with `buffer + concurrency` unordered.
        """
        operator = functional.flat_map(
            fn, concurrency=concurrency, buffer=buffer, ordered=ordered
        )
        return self | Flow._from_operator(operator)

    def filter(
        self,
        pred: Predicate[U],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Flow[T, U]:
        """Keep the items `pred` accepts.

        `pred` can be sync or async. Concurrent predicates emit kept items as they
        finish, or in input order with `ordered=True`.
        """
        return self | Flow._from_operator(
            functional.filter(pred, concurrency=concurrency, ordered=ordered)
        )

    def batch(self, size: int) -> Flow[T, list[U]]:
        """Group items into lists of up to `size`."""
        return self | Flow._from_operator(functional.batch(size))

    def apply(self, source: Source[T]) -> AsyncIterator[U]:
        """Run this flow over `source`."""
        return functional.chain(*self._operators)(source)

    def through[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        """Append the steps of `flow`."""
        return Flow((*self._operators, *flow._operators))

    def __or__[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        """`left | right` is `left.through(right)`."""
        return self.through(flow)

    @staticmethod
    def _from_operator[V, W](operator: Operator[V, W]) -> Flow[V, W]:
        return Flow((operator,))


@dataclass(frozen=True)
class Pipeline[T]:
    """A lazy async pipeline bound to a source.

    Adding a stage returns a new pipeline. Nothing runs until you iterate it
    or consume it with `collect()` or `drain()`.
    """

    _source: Source[Any]
    _operators: tuple[Operator[Any, Any], ...] = ()

    @overload
    def map[U](
        self,
        fn: Callable[[T], Awaitable[U]],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Pipeline[U]: ...

    @overload
    def map[U](
        self,
        fn: Callable[[T], U],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Pipeline[U]: ...

    def map[U](
        self,
        fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Pipeline[U]:
        """Apply `fn` to each item.

        `fn` can be sync or async. With `concurrency > 1`, results arrive as calls
        finish. Use `ordered=True` to deliver them in input order instead.
        """
        return self | Flow._from_operator(
            functional.map(fn, concurrency=concurrency, ordered=ordered)
        )

    def flat_map[U](
        self,
        fn: Expander[T, U],
        *,
        concurrency: int = 1,
        buffer: int = functional.DEFAULT_BUFFER,
        ordered: bool = False,
    ) -> Pipeline[U]:
        """Expand each item into zero or more items.

        `fn` can return an iterable, an async iterable, or an awaitable of either.
        Concurrent expansions interleave their values as they arrive.

        `buffer` limits values waiting for the consumer. With `ordered=True`,
        expansions are delivered in input order, each with its own buffer.
        The stage can then hold up to `concurrency * (buffer + 1)` values,
        compared with `buffer + concurrency` unordered.
        """
        operator = functional.flat_map(
            fn, concurrency=concurrency, buffer=buffer, ordered=ordered
        )
        return self | Flow._from_operator(operator)

    def filter(
        self,
        pred: Predicate[T],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> Pipeline[T]:
        """Keep the items `pred` accepts.

        `pred` can be sync or async. Concurrent predicates emit kept items as they
        finish, or in input order with `ordered=True`.
        """
        return self | Flow._from_operator(
            functional.filter(pred, concurrency=concurrency, ordered=ordered)
        )

    def batch(self, size: int) -> Pipeline[list[T]]:
        """Group items into lists of up to `size`."""
        return self | Flow._from_operator(functional.batch(size))

    def __aiter__(self) -> AsyncIterator[T]:
        """Iterate over the results."""
        return functional.chain(*self._operators)(self._source)

    async def collect(self) -> list[T]:
        """Run the pipeline and return every result in a list."""
        return await functional.collect(self)

    async def drain(self) -> None:
        """Run the pipeline for its side effects, discarding the results."""
        await functional.drain(self)

    def through[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        """Append the steps of `flow`."""
        return Pipeline(self._source, (*self._operators, *flow._operators))

    def __or__[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        """`pipeline | flow` is `pipeline.through(flow)`."""
        return self.through(flow)


def pipe[T](source: Source[T]) -> Pipeline[T]:
    """Start a lazy pipeline from an iterable or async iterable."""
    return Pipeline(source)


__all__ = ["Flow", "Pipeline", "pipe"]
