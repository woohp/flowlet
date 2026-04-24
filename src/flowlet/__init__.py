from __future__ import annotations

import importlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, overload

from flowlet import functional
from flowlet.functional import Expander, Operator, Predicate, Source


@dataclass(frozen=True)
class Flowlet[T, U]:
    _operators: tuple[Operator[Any, Any], ...] = ()

    @overload
    def map[V](
        self,
        fn: Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flowlet[T, V]: ...

    @overload
    def map[V](
        self,
        fn: Callable[[U], V],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flowlet[T, V]: ...

    def map[V](
        self,
        fn: Callable[[U], V] | Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flowlet[T, V]:
        return self._append(functional.map(fn, concurrency=concurrency, ordered=ordered))

    def flat_map[V](
        self,
        fn: Expander[U, V],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flowlet[T, V]:
        return self._append(functional.flat_map(fn, concurrency=concurrency, ordered=ordered))

    def filter(
        self,
        pred: Predicate[U],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flowlet[T, U]:
        return self._append(functional.filter(pred, concurrency=concurrency, ordered=ordered))

    def then[V](self, flowlet: Flowlet[U, V]) -> Flowlet[T, V]:
        return Flowlet((*self._operators, *flowlet._operators))

    def __or__[V](self, flowlet: Flowlet[U, V]) -> Flowlet[T, V]:
        return self.then(flowlet)

    def apply(self, source: Source[T]) -> AsyncIterator[U]:
        current: Source[Any] = source
        for operator in self._operators:
            current = operator(current)
        return functional.to_async_iter(current)

    def _append[V](self, operator: Operator[U, V]) -> Flowlet[T, V]:
        return Flowlet((*self._operators, operator))


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
        ordered: bool = True,
    ) -> Pipeline[U]: ...

    @overload
    def map[U](
        self,
        fn: Callable[[T], U],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Pipeline[U]: ...

    def map[U](
        self,
        fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Pipeline[U]:
        return self.then(Flowlet[T, T]().map(fn, concurrency=concurrency, ordered=ordered))

    def flat_map[U](
        self,
        fn: Expander[T, U],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Pipeline[U]:
        return self.then(Flowlet[T, T]().flat_map(fn, concurrency=concurrency, ordered=ordered))

    def filter(
        self,
        pred: Predicate[T],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Pipeline[T]:
        return self.then(Flowlet[T, T]().filter(pred, concurrency=concurrency, ordered=ordered))

    def then[U](self, flowlet: Flowlet[T, U]) -> Pipeline[U]:
        return Pipeline(self._source, (*self._operators, *flowlet._operators))

    def __or__[U](self, flowlet: Flowlet[T, U]) -> Pipeline[U]:
        return self.then(flowlet)

    def __aiter__(self) -> AsyncIterator[T]:
        current: Source[Any] = self._source
        for operator in self._operators:
            current = operator(current)
        return functional.to_async_iter(current)

    async def collect(self) -> list[T]:
        return await functional.collect(self)

    async def run(self) -> None:
        await functional.drain(self)

    async def for_each(
        self,
        fn: Callable[[T], object] | Callable[[T], Awaitable[object]],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> None:
        await functional.for_each(self, fn, concurrency=concurrency, ordered=ordered)


def pipe[T](source: Source[T]) -> Pipeline[T]:
    return Pipeline(source)


@overload
def flowlet[T, U](fn1: Callable[[T], U], /) -> Flowlet[T, U]: ...


@overload
def flowlet[T, A, U](fn1: Callable[[T], A], fn2: Callable[[A], U], /) -> Flowlet[T, U]: ...


@overload
def flowlet[T, A, B, U](fn1: Callable[[T], A], fn2: Callable[[A], B], fn3: Callable[[B], U], /) -> Flowlet[T, U]: ...


@overload
def flowlet[T, A, B, C, U](
    fn1: Callable[[T], A],
    fn2: Callable[[A], B],
    fn3: Callable[[B], C],
    fn4: Callable[[C], U],
    /,
) -> Flowlet[T, U]: ...


@overload
def flowlet() -> Flowlet[Any, Any]: ...


def flowlet(*functions: Callable[[Any], Any]) -> Flowlet[Any, Any]:
    result: Flowlet[Any, Any] = Flowlet()
    for fn in functions:
        result = result.map(fn)
    return result


Stage = Flowlet
op = importlib.import_module("flowlet.op")

__all__ = ["Flowlet", "Pipeline", "Stage", "flowlet", "functional", "op", "pipe"]
