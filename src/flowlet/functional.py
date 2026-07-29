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
    error: BaseException


@dataclass(frozen=True)
class _Done:
    """One expansion task terminated, successfully or not."""


@dataclass(frozen=True)
class _Fed:
    started: int


type _MapItem[T] = _Value[T] | _Error | _Fed
type _QueueItem[T] = _Value[T] | _Error | _Done | _Fed

DEFAULT_BUFFER = 256
"""Default number of expanded values `flat_map` holds for a lagging consumer.

Bounding this is what gives the stage backpressure. Raising it trades memory for
throughput on wide expansions, because every value produced past the bound needs
an event-loop round trip to reclaim a slot; lowering it does the reverse.
"""


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


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1, buffer: int = DEFAULT_BUFFER) -> Operator[T, U]:
    """Return an operator that expands each source item into zero or more outputs.

    `fn` may return an iterable, async iterable, or an awaitable resolving to
    either. Concurrent expansions yield values as they become available.

    `buffer` caps how many expanded values may wait for the consumer. Once it is
    full, expansions pause instead of buffering, so large expansions stream. Wide
    expansions go faster with a larger `buffer` at the cost of holding more
    values in memory.
    """
    _validate_concurrency(concurrency)
    _validate_buffer(buffer)

    async def apply(source: Source[T]) -> AsyncIterator[U]:
        async for value in _flat_map(source, fn, concurrency, buffer):
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


def _validate_buffer(buffer: int) -> None:
    """Reject an unusable value buffer before an operator is built."""
    if buffer < 1:
        raise ValueError("buffer must be >= 1")


async def _map[T, U](
    source: Source[T], fn: Callable[[T], U] | Callable[[T], Awaitable[U]], concurrency: int
) -> AsyncIterator[U]:
    """Map a source lazily with bounded in-flight calls.

    The sequential path avoids task scheduling overhead. On the concurrent path a
    single feeder task owns the source and starts one worker per item, while this
    driver only ever reads finished results from a queue. Keeping the source off
    the driver's await path is what lets a finished result be yielded while the
    source is still blocked producing the next item.

    A slot, returned once a value is yielded, bounds started-but-unyielded work at
    `concurrency` and gives the stage backpressure.
    """
    if concurrency == 1:
        async for item in to_async_iter(source):
            yield await _apply_map(fn, item)
        return

    source_iter = to_async_iter(source)
    results: asyncio.Queue[_MapItem[U]] = asyncio.Queue()
    slots = asyncio.Semaphore(concurrency)
    workers: set[asyncio.Task[None]] = set()

    async def run(item: T) -> None:
        # Every exit publishes exactly one message, cancellation included, so the
        # driver's count of outstanding work can never leave it stranded on `get`.
        try:
            value = await _apply_map(fn, item)
        except BaseException as exc:
            results.put_nowait(_Error(exc))
            if isinstance(exc, asyncio.CancelledError):
                raise
        else:
            results.put_nowait(_Value(value))

    async def feed() -> None:
        started = 0
        try:
            while True:
                await slots.acquire()
                try:
                    item = await anext(source_iter)
                except StopAsyncIteration:
                    return
                task = asyncio.create_task(run(item))
                workers.add(task)
                task.add_done_callback(workers.discard)
                started += 1
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            results.put_nowait(_Error(exc))
        finally:
            # Reporting the count is what makes termination race-free: the driver
            # never has to inspect a worker set that done callbacks mutate.
            results.put_nowait(_Fed(started))

    feeder = asyncio.create_task(feed())
    started_total: int | None = None
    yielded = 0

    try:
        while started_total is None or yielded < started_total:
            match await results.get():
                case _Value(value):
                    yielded += 1
                    yield value
                    slots.release()
                case _Error(error):
                    raise error
                case _Fed(started):
                    started_total = started
    finally:
        await _cancel_tasks([feeder, *workers])
        await _close_async_iter(source_iter)


async def _apply_map[T, U](fn: Callable[[T], U] | Callable[[T], Awaitable[U]], item: T) -> U:
    """Apply a map function and await the result only when needed."""
    return await _maybe_await(fn(item))


async def _flat_map[T, U](source: Source[T], fn: Expander[T, U], concurrency: int, buffer: int) -> AsyncIterator[U]:
    """Flat-map a source with bounded concurrent expansions.

    A feeder task owns the source and starts one expansion task per item, while
    this driver only ever reads from the queue. Keeping the source off the
    driver's await path is what lets a ready value be yielded while the source is
    still blocked producing the next item.

    Two semaphores do separate jobs. `slots`, returned when an expansion
    terminates, bounds concurrent expansions. `capacity`, returned after a value
    is yielded, bounds unconsumed values at `buffer` so a slow consumer stalls
    production instead of letting expansions buffer without limit.
    """
    source_iter = to_async_iter(source)
    queue: asyncio.Queue[_QueueItem[U]] = asyncio.Queue()
    capacity = asyncio.Semaphore(buffer)
    slots = asyncio.Semaphore(concurrency)
    expansions: set[asyncio.Task[None]] = set()

    async def feed() -> None:
        started = 0
        try:
            while True:
                await slots.acquire()
                try:
                    item = await anext(source_iter)
                except StopAsyncIteration:
                    return
                task = asyncio.create_task(_emit_expansion(fn, item, queue, capacity))
                expansions.add(task)
                task.add_done_callback(expansions.discard)
                started += 1
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            queue.put_nowait(_Error(exc))
        finally:
            # Reporting the count is what makes termination race-free: the driver
            # counts `_Done` messages instead of inspecting a mutating task set.
            queue.put_nowait(_Fed(started))

    feeder = asyncio.create_task(feed())
    started_total: int | None = None
    finished = 0

    try:
        while started_total is None or finished < started_total:
            match await queue.get():
                case _Value(value):
                    yield value
                    # Returned only after the consumer takes the value, so a
                    # slow consumer holds the slot and throttles production.
                    capacity.release()
                case _Error(error):
                    raise error
                case _Done():
                    finished += 1
                    slots.release()
                case _Fed(started):
                    started_total = started
    finally:
        await _cancel_tasks([feeder, *expansions])
        await _close_async_iter(source_iter)


async def _emit_expansion[T, U](
    fn: Expander[T, U],
    item: T,
    queue: asyncio.Queue[_QueueItem[U]],
    capacity: asyncio.Semaphore,
) -> None:
    """Run one flat-map expansion and publish its values or error to `queue`.

    Each value costs a capacity slot, so an expansion blocks once it runs ahead
    of the consumer rather than buffering its whole output. A final `_Done`
    message is always sent so `_flat_map` can retire this expansion task even
    when the expansion raises.

    Every failure is published, including `BaseException`, because the driver
    counts `_Done` messages rather than awaiting each task; an exception left on
    the task object would be silently dropped.
    """
    try:
        expanded = await _maybe_await(fn(item))
        if isinstance(expanded, AsyncIterable):
            async for value in expanded:
                await capacity.acquire()
                queue.put_nowait(_Value(value))
        else:
            for value in cast(Iterable[U], expanded):
                await capacity.acquire()
                queue.put_nowait(_Value(value))
    except BaseException as exc:
        queue.put_nowait(_Error(exc))
        if isinstance(exc, asyncio.CancelledError):
            raise
    finally:
        # _Done means the task terminated, not that it completed successfully.
        # The queue is intentionally unbounded so both terminal messages can be
        # sent without awaiting during cancellation cleanup; `capacity`, not the
        # queue size, is what bounds buffering.
        queue.put_nowait(_Done())


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
