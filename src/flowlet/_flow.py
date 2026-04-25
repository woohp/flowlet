from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, overload

import flowlet.functional as functional
from flowlet.functional import Expander, Operator, Predicate, Source


@dataclass(frozen=True)
class Flow[T, U = T]:
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
        return self | Flow._from_operator(functional.map(fn, concurrency=concurrency))

    def flat_map[V](
        self,
        fn: Expander[U, V],
        *,
        concurrency: int = 1,
    ) -> Flow[T, V]:
        return self | Flow._from_operator(functional.flat_map(fn, concurrency=concurrency))

    def filter(
        self,
        pred: Predicate[U],
        *,
        concurrency: int = 1,
    ) -> Flow[T, U]:
        return self | Flow._from_operator(functional.filter(pred, concurrency=concurrency))

    def through[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        return Flow((*self._operators, *flow._operators))

    def __or__[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        return self.through(flow)

    def apply(self, source: Source[T]) -> AsyncIterator[U]:
        current: Source[Any] = source
        for operator in self._operators:
            current = operator(current)
        return functional.to_async_iter(current)

    @staticmethod
    def _from_operator[V, W](operator: Operator[V, W]) -> Flow[V, W]:
        return Flow((operator,))


@dataclass(frozen=True)
class Pipeline[T]:
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
        return self | Flow._from_operator(functional.map(fn, concurrency=concurrency))

    def flat_map[U](
        self,
        fn: Expander[T, U],
        *,
        concurrency: int = 1,
    ) -> Pipeline[U]:
        return self | Flow._from_operator(functional.flat_map(fn, concurrency=concurrency))

    def filter(
        self,
        pred: Predicate[T],
        *,
        concurrency: int = 1,
    ) -> Pipeline[T]:
        return self | Flow._from_operator(functional.filter(pred, concurrency=concurrency))

    def through[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        return Pipeline(self._source, (*self._operators, *flow._operators))

    def __or__[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        return self.through(flow)

    def __aiter__(self) -> AsyncIterator[T]:
        current: Source[Any] = self._source
        for operator in self._operators:
            current = operator(current)
        return functional.to_async_iter(current)

    async def collect(self) -> list[T]:
        return await functional.collect(self)

    async def drain(self) -> None:
        await functional.drain(self)

    async def for_each(
        self,
        fn: Callable[[T], object] | Callable[[T], Awaitable[object]],
        *,
        concurrency: int = 1,
    ) -> None:
        await functional.for_each(self, fn, concurrency=concurrency)


def pipe[T](source: Source[T]) -> Pipeline[T]:
    return Pipeline(source)


__all__ = ["Flow", "Pipeline", "pipe"]
