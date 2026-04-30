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
    """Reusable stream transform from a source of `T` to async outputs of `U`."""

    def __call__(self, source: Source[T], /) -> AsyncIterator[U]: ...


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


async def to_async_iter[T](source: Source[T]) -> AsyncIterator[T]:
    """Yield items from an iterable or async iterable as an async iterator."""
    if isinstance(source, AsyncIterable):
        async for item in source:
            yield item
        return

    for item in source:
        yield item


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1
) -> Operator[T, U]: ...


@overload
def map[T, U](fn: Callable[[T], U], *, concurrency: int = 1) -> Operator[T, U]: ...  # noqa: A001


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1
) -> Operator[T, U]:
    """Return an operator that applies `fn` to each source item.

    `fn` may be sync or async. With `concurrency > 1`, outputs are yielded in
    completion order.
    """
    _validate_concurrency(concurrency)

    async def apply(source: Source[T]) -> AsyncIterator[U]:
        async for value in _map(source, fn, concurrency):
            yield value

    return apply


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1) -> Operator[T, U]:
    """Return an operator that expands each source item into zero or more outputs.

    `fn` may return an iterable, async iterable, or an awaitable resolving to
    either. Concurrent expansions yield values as they become available.
    """
    _validate_concurrency(concurrency)

    async def apply(source: Source[T]) -> AsyncIterator[U]:
        async for value in _flat_map(source, fn, concurrency):
            yield value

    return apply


def filter[T](pred: Predicate[T], *, concurrency: int = 1) -> Operator[T, T]:  # noqa: A001
    """Return an operator that keeps items where `pred` returns true.

    `pred` may be sync or async. With `concurrency > 1`, kept items are yielded
    in completion order.
    """

    async def expand(item: T) -> list[T]:
        return [item] if await _maybe_await(pred(item)) else []

    return flat_map(expand, concurrency=concurrency)


def batch[T](size: int) -> Operator[T, list[T]]:
    """Return an operator that collects items into lists of up to `size`.

    The last emitted list may contain fewer than `size` items when the source
    is exhausted with a partial group.
    """
    if size < 1:
        raise ValueError("size must be >= 1")

    async def apply(source: Source[T]) -> AsyncIterator[list[T]]:
        chunk: list[T] = []
        async for item in to_async_iter(source):
            chunk.append(item)
            if len(chunk) == size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    return apply


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


@overload
def chain(*operators: Operator[Any, Any]) -> Operator[Any, Any]: ...


def chain(*operators: Operator[Any, Any]) -> Operator[Any, Any]:
    """Compose operators into one reusable stream transform.

    With no operators, the returned transform is the identity operation.
    """

    def apply(source: Source[Any]) -> AsyncIterator[Any]:
        if not operators:
            return to_async_iter(source)

        current: Source[Any] = source
        for operator in operators:
            current = operator(current)
        return cast(AsyncIterator[Any], current)

    return apply


async def collect[T](source: Source[T]) -> list[T]:
    """Consume a source into a list."""
    return [item async for item in to_async_iter(source)]


async def drain[T](source: Source[T]) -> None:
    """Consume a source and discard all yielded values."""
    async for _ in to_async_iter(source):
        pass


def _validate_concurrency(concurrency: int) -> None:
    """Reject invalid stage concurrency before an operator is built."""
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")


async def _map[T, U](
    source: Source[T], fn: Callable[[T], U] | Callable[[T], Awaitable[U]], concurrency: int
) -> AsyncIterator[U]:
    """Map a source lazily with bounded in-flight calls.

    The sequential path avoids task scheduling overhead. The concurrent path
    starts up to `concurrency` tasks and yields each result as soon as its task
    completes, cancelling unfinished work if iteration stops or an error occurs.
    """
    if concurrency == 1:
        async for item in to_async_iter(source):
            yield await _apply_map(fn, item)
        return

    source_iter = to_async_iter(source)
    pending: set[asyncio.Task[U]] = set()
    remaining_done: set[asyncio.Task[U]] = set()
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
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            remaining_done = set(done)
            for task in done:
                remaining_done.remove(task)
                yield await task

            while len(pending) < concurrency and not source_done:
                await start_next()
    except BaseException:
        await asyncio.gather(*remaining_done, return_exceptions=True)
        raise
    finally:
        await _cancel_tasks(pending)
        await _close_async_iter(source_iter)


async def _apply_map[T, U](fn: Callable[[T], U] | Callable[[T], Awaitable[U]], item: T) -> U:
    """Apply a map function and await the result only when needed."""
    return await _maybe_await(fn(item))


async def _flat_map[T, U](source: Source[T], fn: Expander[T, U], concurrency: int) -> AsyncIterator[U]:
    """Flat-map a source with bounded concurrent expansions.

    Each input gets an expansion task that streams values into a shared queue.
    Queue messages distinguish yielded values, raised errors, and task
    termination so downstream consumers can receive values as they arrive while
    the driver still knows when to start more input work.
    """
    source_iter = to_async_iter(source)
    queue: asyncio.Queue[_QueueItem[U]] = asyncio.Queue()
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


async def _emit_expansion[T, U](fn: Expander[T, U], item: T, queue: asyncio.Queue[_QueueItem[U]], token: int) -> None:
    """Run one flat-map expansion and publish its values or error to `queue`.

    A final `_Done` message is always sent so `_flat_map` can retire this
    expansion task even when the expansion raises.
    """
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
        queue.put_nowait(_Error(exc))
    finally:
        # _Done means the task terminated, not that it completed successfully.
        # The flat_map queue is intentionally unbounded so termination can be
        # signaled without awaiting during cancellation cleanup.
        queue.put_nowait(_Done(token))


async def _cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """Cancel tasks and wait for cancellation cleanup to finish."""
    task_list = list(tasks)
    for task in task_list:
        task.cancel()
    if task_list:
        await asyncio.gather(*task_list, return_exceptions=True)


async def _close_async_iter(source: AsyncIterator[Any]) -> None:
    """Close an async iterator when it exposes `aclose`.

    This lets pipeline stages release upstream generators promptly when a
    downstream consumer stops early or an error interrupts iteration.
    """
    aclose = getattr(source, "aclose", None)
    if aclose is not None:
        await aclose()


async def _maybe_await[T](value: T | Awaitable[T]) -> T:
    """Return plain values unchanged and await awaitable values."""
    if inspect.isawaitable(value):
        return await value
    return value


__all__ = [
    "Expander",
    "Operator",
    "Predicate",
    "Source",
    "batch",
    "chain",
    "collect",
    "drain",
    "filter",
    "flat_map",
    "map",
    "to_async_iter",
]
