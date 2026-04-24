from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol, cast, overload

type Source[T] = Iterable[T] | AsyncIterable[T]
type Expander[T, U] = Callable[[T], Iterable[U] | AsyncIterable[U] | Awaitable[Iterable[U] | AsyncIterable[U]]]
type Predicate[T] = Callable[[T], bool | Awaitable[bool]]


class _Step[T, U](Protocol):
    def apply(self, source: AsyncIterable[T]) -> AsyncIterator[U]: ...


def _validate_concurrency(concurrency: int) -> None:
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")


async def _maybe_await[T](value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value
    return value


async def _iterate_source[T](source: Source[T]) -> AsyncIterator[T]:
    if isinstance(source, AsyncIterable):
        async for item in source:
            yield item
        return

    for item in source:
        yield item


async def _collect_expansion[T, U](fn: Expander[T, U], item: T) -> list[U]:
    expanded = await _maybe_await(fn(item))
    output: list[U] = []

    if isinstance(expanded, AsyncIterable):
        async for value in expanded:
            output.append(value)
    else:
        output.extend(cast(Iterable[U], expanded))

    return output


@dataclass(frozen=True)
class _FlatMapStep[T, U]:
    fn: Expander[T, U]
    concurrency: int = 1
    ordered: bool = True

    def __post_init__(self) -> None:
        _validate_concurrency(self.concurrency)

    async def apply(self, source: AsyncIterable[T]) -> AsyncIterator[U]:
        if self.ordered:
            async for value in self._apply_ordered(source):
                yield value
        else:
            async for value in self._apply_unordered(source):
                yield value

    async def _apply_ordered(self, source: AsyncIterable[T]) -> AsyncIterator[U]:
        pending: list[asyncio.Task[list[U]]] = []

        try:
            async for item in source:
                pending.append(asyncio.create_task(_collect_expansion(self.fn, item)))

                if len(pending) >= self.concurrency:
                    done = pending.pop(0)
                    for value in await done:
                        yield value

            for task in pending:
                for value in await task:
                    yield value
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()

    async def _apply_unordered(self, source: AsyncIterable[T]) -> AsyncIterator[U]:
        pending: set[asyncio.Task[list[U]]] = set()

        async def emit_finished() -> AsyncIterator[U]:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            pending.difference_update(done)
            for task in done:
                for value in await task:
                    yield value

        try:
            async for item in source:
                pending.add(asyncio.create_task(_collect_expansion(self.fn, item)))

                while len(pending) >= self.concurrency:
                    async for value in emit_finished():
                        yield value

            while pending:
                async for value in emit_finished():
                    yield value
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()


@dataclass(frozen=True)
class Flow[T, U]:
    _steps: tuple[_Step[Any, Any], ...] = ()

    @overload
    def map[V](
        self,
        fn: Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flow[T, V]: ...

    @overload
    def map[V](
        self,
        fn: Callable[[U], V],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flow[T, V]: ...

    def map[V](
        self,
        fn: Callable[[U], V] | Callable[[U], Awaitable[V]],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flow[T, V]:
        async def expand(item: U) -> list[V]:
            return [await _maybe_await(fn(item))]

        return Flow((*self._steps, _FlatMapStep(expand, concurrency, ordered)))

    def flat_map[V](
        self,
        fn: Expander[U, V],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flow[T, V]:
        return Flow((*self._steps, _FlatMapStep(fn, concurrency, ordered)))

    def filter(
        self,
        pred: Predicate[U],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Flow[T, U]:
        async def expand(item: U) -> list[U]:
            return [item] if await _maybe_await(pred(item)) else []

        return Flow((*self._steps, _FlatMapStep(expand, concurrency, ordered)))

    def then[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        return Flow((*self._steps, *flow._steps))

    def __or__[V](self, flow: Flow[U, V]) -> Flow[T, V]:
        return self.then(flow)

    def apply(self, source: AsyncIterable[T]) -> AsyncIterator[U]:
        current: AsyncIterable[Any] = source
        for step in self._steps:
            current = step.apply(current)
        return current  # type: ignore[return-value]


@dataclass(frozen=True)
class Pipeline[T]:
    _source: Source[Any]
    _steps: tuple[_Step[Any, Any], ...] = ()

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
        return self.then(Flow[T, T]().map(fn, concurrency=concurrency, ordered=ordered))

    def flat_map[U](
        self,
        fn: Expander[T, U],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Pipeline[U]:
        return self.then(Flow[T, T]().flat_map(fn, concurrency=concurrency, ordered=ordered))

    def filter(
        self,
        pred: Predicate[T],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Pipeline[T]:
        return self.then(Flow[T, T]().filter(pred, concurrency=concurrency, ordered=ordered))

    def then[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        return Pipeline(self._source, (*self._steps, *flow._steps))

    def __or__[U](self, flow: Flow[T, U]) -> Pipeline[U]:
        return self.then(flow)

    def __aiter__(self) -> AsyncIterator[T]:
        current: AsyncIterable[Any] = _iterate_source(self._source)
        for step in self._steps:
            current = step.apply(current)
        return current  # type: ignore[return-value]

    async def collect(self) -> list[T]:
        return [item async for item in self]

    async def run(self) -> None:
        async for _ in self:
            pass

    async def for_each(
        self,
        fn: Callable[[T], object] | Callable[[T], Awaitable[object]],
        *,
        concurrency: int = 1,
        ordered: bool = False,
    ) -> None:
        await self.map(fn, concurrency=concurrency, ordered=ordered).run()


def pipe[T](source: Source[T]) -> Pipeline[T]:
    return Pipeline(source)


@overload
def flow[T, U](fn1: Callable[[T], U], /) -> Flow[T, U]: ...


@overload
def flow[T, A, U](fn1: Callable[[T], A], fn2: Callable[[A], U], /) -> Flow[T, U]: ...


@overload
def flow[T, A, B, U](fn1: Callable[[T], A], fn2: Callable[[A], B], fn3: Callable[[B], U], /) -> Flow[T, U]: ...


@overload
def flow[T, A, B, C, U](
    fn1: Callable[[T], A],
    fn2: Callable[[A], B],
    fn3: Callable[[B], C],
    fn4: Callable[[C], U],
    /,
) -> Flow[T, U]: ...


@overload
def flow() -> Flow[Any, Any]: ...


def flow(*functions: Callable[[Any], Any]) -> Flow[Any, Any]:
    result: Flow[Any, Any] = Flow()
    for fn in functions:
        result = result.map(fn)
    return result


@overload
def map_[T, U](fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = True) -> Flow[T, U]: ...


@overload
def map_[T, U](fn: Callable[[T], U], *, concurrency: int = 1, ordered: bool = True) -> Flow[T, U]: ...


def map_[T, U](
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = True
) -> Flow[T, U]:
    return Flow[T, T]().map(fn, concurrency=concurrency, ordered=ordered)


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1, ordered: bool = True) -> Flow[T, U]:
    return Flow[T, T]().flat_map(fn, concurrency=concurrency, ordered=ordered)


def filter_[T](pred: Predicate[T], *, concurrency: int = 1, ordered: bool = True) -> Flow[T, T]:
    return Flow[T, T]().filter(pred, concurrency=concurrency, ordered=ordered)


Stage = Flow

__all__ = ["Flow", "Pipeline", "Stage", "filter_", "flat_map", "flow", "map_", "pipe"]
