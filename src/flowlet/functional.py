from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
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


@dataclass(frozen=True)
class _Value[T]:
    value: T


@dataclass(frozen=True)
class _Error:
    error: Exception


@dataclass(frozen=True)
class _Done:
    token: int


type _QueueItem[T] = _Value[T] | _Error | _Done


async def _emit_expansion[T, U](fn: Expander[T, U], item: T, queue: asyncio.Queue[_QueueItem[U]], token: int) -> None:
    try:
        expanded = await _maybe_await(fn(item))
        if isinstance(expanded, AsyncIterable):
            async for value in expanded:
                await queue.put(_Value(value))
        else:
            for value in cast(Iterable[U], expanded):
                await queue.put(_Value(value))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await queue.put(_Error(exc))
    finally:
        task = asyncio.current_task()
        if task is None or not task.cancelling():
            await queue.put(_Done(token))


async def _cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    task_list = list(tasks)
    for task in task_list:
        task.cancel()
    if task_list:
        await asyncio.gather(*task_list, return_exceptions=True)


async def _close_async_iter(source: AsyncIterator[Any]) -> None:
    aclose = getattr(source, "aclose", None)
    if aclose is not None:
        await aclose()


async def _flat_map[T, U](source: Source[T], fn: Expander[T, U], concurrency: int) -> AsyncIterator[U]:
    source_iter = to_async_iter(source)
    queue: asyncio.Queue[_QueueItem[U]] = asyncio.Queue(maxsize=concurrency)
    pending: dict[int, asyncio.Task[None]] = {}
    source_done = False
    next_token = 0

    async def start_next() -> None:
        nonlocal next_token, source_done

        if source_done:
            return

        try:
            item = await anext(source_iter)
        except StopAsyncIteration:
            source_done = True
            return

        pending[next_token] = asyncio.create_task(_emit_expansion(fn, item, queue, next_token))
        next_token += 1

    try:
        while len(pending) < concurrency and not source_done:
            await start_next()

        while pending:
            match await queue.get():
                case _Value(value):
                    yield value
                case _Error(error):
                    raise error
                case _Done(token):
                    task = pending.pop(token)
                    await task
                    while len(pending) < concurrency and not source_done:
                        await start_next()
    finally:
        await _cancel_tasks(pending.values())
        await _close_async_iter(source_iter)


async def _apply_map[T, U](fn: Callable[[T], U] | Callable[[T], Awaitable[U]], item: T) -> U:
    return await _maybe_await(fn(item))


async def _map[T, U](
    source: Source[T], fn: Callable[[T], U] | Callable[[T], Awaitable[U]], concurrency: int
) -> AsyncIterator[U]:
    source_iter = to_async_iter(source)
    pending: set[asyncio.Task[U]] = set()
    source_done = False

    async def start_next() -> None:
        nonlocal source_done

        if source_done:
            return

        try:
            item = await anext(source_iter)
        except StopAsyncIteration:
            source_done = True
            return

        pending.add(asyncio.create_task(_apply_map(fn, item)))

    try:
        while len(pending) < concurrency and not source_done:
            await start_next()

        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            task = done.pop()
            pending.remove(task)
            yield await task

            while len(pending) < concurrency and not source_done:
                await start_next()
    finally:
        await _cancel_tasks(pending)
        await _close_async_iter(source_iter)


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1
) -> Operator[T, U]: ...


@overload
def map[T, U](fn: Callable[[T], U], *, concurrency: int = 1) -> Operator[T, U]: ...  # noqa: A001


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1
) -> Operator[T, U]:
    _validate_concurrency(concurrency)

    async def apply(source: Source[T]) -> AsyncIterator[U]:
        async for value in _map(source, fn, concurrency):
            yield value

    return apply


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1) -> Operator[T, U]:
    _validate_concurrency(concurrency)

    async def apply(source: Source[T]) -> AsyncIterator[U]:
        async for value in _flat_map(source, fn, concurrency):
            yield value

    return apply


def filter[T](pred: Predicate[T], *, concurrency: int = 1) -> Operator[T, T]:  # noqa: A001
    async def expand(item: T) -> list[T]:
        return [item] if await _maybe_await(pred(item)) else []

    return flat_map(expand, concurrency=concurrency)


@overload
def chain[T]() -> Operator[T, T]: ...


@overload
def chain[T, U](op1: Operator[T, U], /) -> Operator[T, U]: ...


@overload
def chain[T, A, U](op1: Operator[T, A], op2: Operator[A, U], /) -> Operator[T, U]: ...


@overload
def chain[T, A, B, U](op1: Operator[T, A], op2: Operator[A, B], op3: Operator[B, U], /) -> Operator[T, U]: ...


@overload
def chain[T, A, B, C, U](
    op1: Operator[T, A], op2: Operator[A, B], op3: Operator[B, C], op4: Operator[C, U], /
) -> Operator[T, U]: ...


def chain(*operators: Operator[Any, Any]) -> Operator[Any, Any]:
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
) -> None:
    await drain(map(fn, concurrency=concurrency)(source))


__all__ = [
    "Expander",
    "Operator",
    "Predicate",
    "Source",
    "chain",
    "collect",
    "drain",
    "filter",
    "flat_map",
    "for_each",
    "map",
    "to_async_iter",
]
