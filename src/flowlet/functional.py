from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, Protocol, cast, overload

type Source[T] = Iterable[T] | AsyncIterable[T]
type Expander[T, U] = Callable[[T], Iterable[U] | AsyncIterable[U] | Awaitable[Iterable[U] | AsyncIterable[U]]]
type Predicate[T] = Callable[[T], bool | Awaitable[bool]]


class Operator[T, U](Protocol):
    def __call__(self, source: Source[T], /) -> AsyncIterator[U]: ...


def _validate_concurrency(concurrency: int) -> None:
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")


async def _maybe_await[T](value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value
    return value


async def to_async_iter[T](source: Source[T]) -> AsyncIterator[T]:
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


async def _ordered_flat_map[T, U](source: Source[T], fn: Expander[T, U], concurrency: int) -> AsyncIterator[U]:
    pending: list[asyncio.Task[list[U]]] = []

    try:
        async for item in to_async_iter(source):
            pending.append(asyncio.create_task(_collect_expansion(fn, item)))

            if len(pending) >= concurrency:
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


async def _unordered_flat_map[T, U](source: Source[T], fn: Expander[T, U], concurrency: int) -> AsyncIterator[U]:
    pending: set[asyncio.Task[list[U]]] = set()

    async def emit_finished() -> AsyncIterator[U]:
        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        pending.difference_update(done)
        for task in done:
            for value in await task:
                yield value

    try:
        async for item in to_async_iter(source):
            pending.add(asyncio.create_task(_collect_expansion(fn, item)))

            while len(pending) >= concurrency:
                async for value in emit_finished():
                    yield value

        while pending:
            async for value in emit_finished():
                yield value
    finally:
        for task in pending:
            if not task.done():
                task.cancel()


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = True
) -> Operator[T, U]: ...


@overload
def map[T, U](fn: Callable[[T], U], *, concurrency: int = 1, ordered: bool = True) -> Operator[T, U]: ...  # noqa: A001


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = True
) -> Operator[T, U]:
    async def expand(item: T) -> list[U]:
        return [await _maybe_await(fn(item))]

    return flat_map(expand, concurrency=concurrency, ordered=ordered)


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1, ordered: bool = True) -> Operator[T, U]:
    _validate_concurrency(concurrency)

    async def apply(source: Source[T]) -> AsyncIterator[U]:
        if ordered:
            async for value in _ordered_flat_map(source, fn, concurrency):
                yield value
        else:
            async for value in _unordered_flat_map(source, fn, concurrency):
                yield value

    return apply


def filter[T](pred: Predicate[T], *, concurrency: int = 1, ordered: bool = True) -> Operator[T, T]:  # noqa: A001
    async def expand(item: T) -> list[T]:
        return [item] if await _maybe_await(pred(item)) else []

    return flat_map(expand, concurrency=concurrency, ordered=ordered)


@overload
def compose[T, U](op1: Operator[T, U], /) -> Operator[T, U]: ...


@overload
def compose[T, A, U](op1: Operator[T, A], op2: Operator[A, U], /) -> Operator[T, U]: ...


@overload
def compose[T, A, B, U](op1: Operator[T, A], op2: Operator[A, B], op3: Operator[B, U], /) -> Operator[T, U]: ...


@overload
def compose[T, A, B, C, U](
    op1: Operator[T, A], op2: Operator[A, B], op3: Operator[B, C], op4: Operator[C, U], /
) -> Operator[T, U]: ...


def compose(*operators: Operator[Any, Any]) -> Operator[Any, Any]:
    def apply(source: Source[Any]) -> AsyncIterator[Any]:
        current: Source[Any] = source
        for operator in operators:
            current = operator(current)
        return to_async_iter(current)

    return apply


async def collect[T](source: Source[T]) -> list[T]:
    return [item async for item in to_async_iter(source)]


async def drain[T](source: Source[T]) -> None:
    async for _ in to_async_iter(source):
        pass


async def for_each[T](
    source: Source[T],
    fn: Callable[[T], object] | Callable[[T], Awaitable[object]],
    *,
    concurrency: int = 1,
    ordered: bool = False,
) -> None:
    await drain(map(fn, concurrency=concurrency, ordered=ordered)(source))


__all__ = [
    "Expander",
    "Operator",
    "Predicate",
    "Source",
    "collect",
    "compose",
    "drain",
    "filter",
    "flat_map",
    "for_each",
    "map",
    "to_async_iter",
]
