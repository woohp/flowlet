import asyncio
import enum
import inspect
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
)
from dataclasses import dataclass
from typing import Any, Protocol, cast, overload

type Source[T] = Iterable[T] | AsyncIterable[T]
type Expander[T, U] = Callable[
    [T], Iterable[U] | AsyncIterable[U] | Awaitable[Iterable[U] | AsyncIterable[U]]
]
type Predicate[T] = Callable[[T], bool | Awaitable[bool]]


class Operator[T, U](Protocol):
    """A reusable stream transform from a source of `T` to an async iterator of `U`."""

    def __call__(self, source: Source[T], /) -> AsyncIterator[U]: ...


@dataclass(frozen=True)
class _Value[T]:
    """A result tagged with its input position."""

    value: T
    seq: int


@dataclass(frozen=True)
class _Error:
    """An item's failure tagged with its input position."""

    error: BaseException
    seq: int


@dataclass(frozen=True)
class _Done:
    """An expansion task has finished, whether or not it succeeded."""

    seq: int


@dataclass(frozen=True)
class _Fed:
    """The feeder has stopped after starting `started` items; `error` says why.

    A source failure ends the input but has no item position. Keeping it with the
    count lets an ordered driver wait until every started item is delivered,
    rather than treating it as an item's failure.
    """

    started: int
    error: BaseException | None = None


_UNPOSITIONED = -1
"""Placeholder position for a boundary that owns the whole source.

Source cleanup failures are kept for teardown, not queued, so an ordered driver
never reads this position. Only expansion boundaries carry a real one.
"""


class _Positional(Protocol):
    """A queue message that belongs to one input position."""

    @property
    def seq(self) -> int: ...


type _MapItem[T] = _Value[T] | _Error | _Fed
type _QueueItem[T] = _Value[T] | _Error | _Done | _Fed

DEFAULT_BUFFER = 256
"""How many expanded values `flat_map` holds by default for a slow consumer.

The bound is what gives the stage backpressure. A larger one trades memory for
throughput on wide expansions, since every value past the bound costs an
event-loop round trip to free a slot.
"""


class _Role(enum.Enum):
    """How an ownership boundary delivers a failure from closing its iterator.

    Fixed when the owner is built. Whether a failure should surface on a
    particular exit is a separate decision, made by `_may_surface`.
    """

    DIRECT = "direct"
    """Raise directly, for the public wrapper and sequential stages."""

    RETAIN = "retain"
    """Keep source close failures for teardown when a feeder owns the source.

    An exhausted source can't run away, so its close failure can wait.
    The driver delivers in-flight results first; the recorded failure survives
    even if the queue is no longer read.
    """

    RETAIN_NOTIFY = "retain_notify"
    """Keep expansion close failures for teardown and notify the driver.

    Other expansions can still be running. Keeping the failure for teardown
    alone would delay it until the source runs out, possibly forever.
    """


def _may_surface(error: BaseException | None) -> bool:
    """Whether a cleanup failure is the most useful thing left to report.

    `error` ended the body that read the iterator. A cleanup failure must not
    hide that cause, but it must not go unnoticed after a clean exit either.
    Report it only after normal completion or a plain close.
    """
    return error is None or isinstance(error, GeneratorExit)


def _bypasses_ordering(error: BaseException) -> bool:
    """Whether an error skips the line instead of waiting for its position.

    Cancellation means teardown, not an item's outcome. Holding it behind an
    unfinished item would delay shutdown in ordered mode. Every other failure
    belongs to its item and waits its turn.
    """
    return isinstance(error, asyncio.CancelledError)


class _Failures[U]:
    """Record cleanup failures for teardown and, if needed, notify the driver.

    The driver stops reading the queue during teardown, so a failure published
    only there would be lost. Raising immediately could mask the original error.
    Recording it preserves it for teardown; `role` decides whether to notify a
    consumer that's still reading.
    """

    def __init__(
        self, role: _Role, queue: asyncio.Queue[_QueueItem[U]] | None = None
    ) -> None:
        self._role = role
        self._queue = queue
        self._recorded: list[BaseException] = []

    def report(self, error: BaseException, may_surface: bool, seq: int) -> None:
        """Report a close failure according to this boundary's role.

        `seq` lets an ordered driver hold a notified failure until its input
        position, just like an item's failure.
        """
        if self._role is _Role.DIRECT:
            if may_surface:
                raise error
            return
        self._recorded.append(error)
        if (
            self._role is _Role.RETAIN_NOTIFY
            and self._queue is not None
            and may_surface
        ):
            self._queue.put_nowait(_Error(error, seq))

    @property
    def first(self) -> BaseException | None:
        """The first recorded failure, for teardown to raise."""
        return self._recorded[0] if self._recorded else None


async def _adapt_sync[T](iterator: Iterator[T]) -> AsyncIterator[T]:
    """Present a sync iterator as an async one.

    It only iterates; the caller owns the iterator and closes it.
    """
    for item in iterator:
        yield item


class _OwnedAsync[T]:
    """Owns an async iterator for as long as a body of code reads it.

    Wrap it around the code that reads the iterator. On exit it closes the
    iterator and reports any close failure according to why the *body* ended.

    The iterator can't judge why its caller stopped. If the body fails after
    receiving a value, closing the iterator sends only `GeneratorExit`.
    Treating that as a plain close would mask the body's error with a cleanup
    failure and notify a driver that's already failing.
    """

    def __init__(
        self,
        source: AsyncIterable[T],
        failures: _Failures[Any],
        seq: int = _UNPOSITIONED,
    ) -> None:
        self._source = source
        self._failures = failures
        self._seq = seq
        self._iterator: AsyncIterator[T] | None = None

    async def __aenter__(self) -> AsyncIterator[T]:
        self._iterator = aiter(self._source)
        return self._iterator

    async def __aexit__(
        self, exc_type: Any, error: BaseException | None, traceback: Any
    ) -> bool:
        if self._iterator is not None:
            try:
                await _aclose(self._iterator)
            except BaseException as close_error:
                self._failures.report(close_error, _may_surface(error), self._seq)
        return False


class _OwnedSync[T]:
    """Owns a sync iterator, with the same policy as `_OwnedAsync`.

    It's synchronous on purpose. Nothing in it awaits, and the async protocol
    costs two coroutines per owner, which `filter` pays for every item.
    The body gets the raw iterator, too; an async adapter would add a generator
    resumption per value.
    """

    def __init__(
        self, source: Iterable[T], failures: _Failures[Any], seq: int = _UNPOSITIONED
    ) -> None:
        self._source = source
        self._failures = failures
        self._seq = seq
        self._iterator: Iterator[T] | None = None

    def __enter__(self) -> Iterator[T]:
        self._iterator = iter(self._source)
        return self._iterator

    def __exit__(
        self, exc_type: Any, error: BaseException | None, traceback: Any
    ) -> None:
        # Returns None, never True: an owner reports cleanup failures,
        # but never swallows the exception that ended the body.
        if self._iterator is not None:
            try:
                _close(self._iterator)
            except BaseException as close_error:
                self._failures.report(close_error, _may_surface(error), self._seq)


class _OwnedSource[T]:
    """Owns a sync or async source and gives feeders an async view.

    Feeders pull with `anext`, so source adaptation belongs here.

    Don't use `_OwnedAsync(to_async_iter(source))`: that wrapper raises close
    failures directly. On exhaustion, a close failure would come out of `anext`
    and end the input, dropping in-flight results instead of delivering them
    before teardown reports it.
    """

    def __init__(self, source: Source[T], failures: _Failures[Any]) -> None:
        self._source = source
        self._failures = failures
        self._async_iterator: AsyncIterator[T] | None = None
        self._sync_iterator: Iterator[T] | None = None

    async def __aenter__(self) -> AsyncIterator[T]:
        if isinstance(self._source, AsyncIterable):
            self._async_iterator = aiter(self._source)
            return self._async_iterator
        self._sync_iterator = iter(self._source)
        return _adapt_sync(self._sync_iterator)

    async def __aexit__(
        self, exc_type: Any, error: BaseException | None, traceback: Any
    ) -> bool:
        try:
            if self._async_iterator is not None:
                await _aclose(self._async_iterator)
            elif self._sync_iterator is not None:
                _close(self._sync_iterator)
        except BaseException as close_error:
            # A source is the whole input, not a position in it.
            self._failures.report(close_error, _may_surface(error), _UNPOSITIONED)
        return False


async def to_async_iter[T](source: Source[T]) -> AsyncIterator[T]:
    """Iterate any iterable or async iterable as an async iterator.

    Closing this iterator closes the source too; otherwise the source's cleanup
    would wait for garbage collection.

    A close failure comes out of `aclose()` unless an error is already leaving
    this iterator. `aclose()` takes no exception, so a *caller's* own failure
    can't be seen here: a close failure then surfaces as if the caller had
    simply stopped reading.
    """
    failures = _Failures[Any](_Role.DIRECT)
    if isinstance(source, AsyncIterable):
        async with _OwnedAsync(source, failures) as async_items:
            async for item in async_items:
                yield item
        return
    with _OwnedSync(source, failures) as sync_items:
        for item in sync_items:
            yield item


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = False
) -> Operator[T, U]: ...


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], U], *, concurrency: int = 1, ordered: bool = False
) -> Operator[T, U]: ...


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
    *,
    concurrency: int = 1,
    ordered: bool = False,
) -> Operator[T, U]:
    """An operator that applies `fn` to each item.

    `fn` can be sync or async. With `concurrency > 1`, results come out in the
    order they finish.

    With `ordered=True`, output matches `concurrency=1`: the same values, order,
    and ordinary failures. Only timing differs. If item 1 hangs and item 2 fails,
    an ordered stage keeps waiting. An unordered stage cancels pending work
    instead of waiting for item 1's result, then raises after cleanup.
    Cancellation starts shutdown immediately in either mode,
    but shutdown still waits for the stage's tasks.
    """
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")

    def apply(source: Source[T]) -> AsyncIterator[U]:
        # Return the driver itself; don't re-yield from it. `async for` doesn't
        # close its iterator when GeneratorExit passes through, so a wrapper
        # would swallow the close and leave the driver's cleanup (cancelling
        # tasks, closing the source) to garbage collection.
        return _map(source, fn, concurrency, ordered)

    return apply


def flat_map[T, U](
    fn: Expander[T, U],
    *,
    concurrency: int = 1,
    buffer: int = DEFAULT_BUFFER,
    ordered: bool = False,
) -> Operator[T, U]:
    """An operator that expands each item into zero or more items.

    `fn` can return an iterable, an async iterable, or an awaitable of either.
    Concurrent expansions yield values as they become available.

    `buffer` caps how many values can wait for the consumer. Once it's full,
    expansions pause rather than pile up, so large expansions stream.
    A bigger `buffer` speeds up wide expansions at the cost of memory.

    With `ordered=True`, all of item *n*'s values come before item *n + 1*'s,
    preserving order within each expansion. Each running expansion gets its own
    buffer, so the stage can hold up to `concurrency * (buffer + 1)` values
    instead of `buffer + concurrency`. An expansion that never ends blocks every
    item behind it; unordered mode would interleave them.
    """
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    if buffer < 1:
        raise ValueError("buffer must be >= 1")

    def apply(source: Source[T]) -> AsyncIterator[U]:
        # Returned directly, not re-yielded: see the note in `map`.
        return _flat_map(source, fn, concurrency, buffer, ordered)

    return apply


def filter[T](  # noqa: A001
    pred: Predicate[T], *, concurrency: int = 1, ordered: bool = False
) -> Operator[T, T]:
    """An operator that keeps the items `pred` accepts.

    `pred` can be sync or async. With `concurrency > 1`, kept items come out in
    the order they finish, or in input order with `ordered=True`.
    """

    async def expand(item: T) -> list[T]:
        return [item] if await _maybe_await(pred(item)) else []

    # Each expansion yields at most one value, so ordered mode holds at most
    # `concurrency` values, regardless of `buffer`.
    return flat_map(expand, concurrency=concurrency, ordered=ordered)


def batch[T](size: int) -> Operator[T, list[T]]:
    """An operator that groups items into lists of up to `size`.

    The last list can be shorter if the source runs out mid-group.
    """
    if size < 1:
        raise ValueError("size must be >= 1")

    async def apply(source: Source[T]) -> AsyncIterator[list[T]]:
        chunk: list[T] = []
        # Own the wrapper to keep sync/async branching in `to_async_iter`,
        # without adding another adapter.
        async with _OwnedAsync(
            to_async_iter(source), _Failures[Any](_Role.DIRECT)
        ) as items:
            async for item in items:
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
def chain[T, A, B, U](
    op1: Operator[T, A], op2: Operator[A, B], op3: Operator[B, U], /
) -> Operator[T, U]: ...


@overload
def chain[T, A, B, C, U](
    op1: Operator[T, A],
    op2: Operator[A, B],
    op3: Operator[B, C],
    op4: Operator[C, U],
    /,
) -> Operator[T, U]: ...


@overload
def chain(*operators: Operator[Any, Any]) -> Operator[Any, Any]: ...


def chain(*operators: Operator[Any, Any]) -> Operator[Any, Any]:
    """Compose operators into one reusable transform.

    With no operators, it's the identity.
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
    """Read a source to the end and return its items in a list."""
    return [item async for item in to_async_iter(source)]


async def drain[T](source: Source[T]) -> None:
    """Read a source to the end, discarding its items."""
    async for _ in to_async_iter(source):
        pass


async def _map[T, U](
    source: Source[T],
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
    concurrency: int,
    ordered: bool,
) -> AsyncIterator[U]:
    """The `map` driver: lazy, with a bound on in-flight calls.

    At `concurrency=1`, it runs sequentially without scheduling tasks.
    Otherwise, a feeder owns the source and starts one worker per item.
    The driver reads finished results from a queue, so a slow source can't hold
    back ready output.

    Slots provide backpressure: each is returned only after its result is yielded,
    keeping started but undelivered work within `concurrency`. Ordered mode needs
    no extra bound: at most `concurrency` results can be waiting for their turn.
    """
    if concurrency == 1:
        # Ordering is built in here, so `ordered` must not move a stage off this path.
        # The owner sees the whole body, so an error from `fn` takes precedence
        # over a source close error.
        async with _OwnedAsync(
            to_async_iter(source), _Failures[Any](_Role.DIRECT)
        ) as items:
            async for item in items:
                yield await _maybe_await(fn(item))
        return

    failures = _Failures[U](_Role.RETAIN)
    results: asyncio.Queue[_MapItem[U]] = asyncio.Queue()
    slots = asyncio.Semaphore(concurrency)
    workers: set[asyncio.Task[None]] = set()

    async def run(seq: int, item: T) -> None:
        # Publish on every exit, cancellation included; otherwise the driver
        # counts work that will never arrive and hangs on `get`.
        try:
            value = await _maybe_await(fn(item))
        except BaseException as exc:
            results.put_nowait(_Error(exc, seq))
            if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
                raise
        else:
            results.put_nowait(_Value(value, seq))

    async def feed() -> None:
        started = 0
        failure: BaseException | None = None
        try:
            async with _OwnedSource(source, failures) as items:
                while True:
                    await slots.acquire()
                    try:
                        item = await anext(items)
                    except StopAsyncIteration:
                        return
                    # Start eagerly: work that finishes without suspending
                    # publishes in this turn, saving an event-loop turn per item.
                    task = asyncio.create_task(run(started, item), eager_start=True)
                    workers.add(task)
                    task.add_done_callback(workers.discard)
                    started += 1
        except BaseException as exc:
            # Report it even when cancelled: a source that raises CancelledError
            # must surface, not pass for a short stream. If the cancellation is
            # this driver tearing the feeder down, nobody reads the message.
            failure = exc
            if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
                raise
        finally:
            # A final count lets the driver detect completion without racing
            # against done callbacks that change the worker set.
            results.put_nowait(_Fed(started, failure))

    started_total: int | None = None
    yielded = 0
    ordering = (
        _Ordered[_Value[U] | _Error]() if ordered else _Unordered[_Value[U] | _Error]()
    )
    source_failure: BaseException | None = None

    async with _Stage(asyncio.create_task(feed()), workers, failures):
        while started_total is None or yielded < started_total:
            match await results.get():
                case _Error(error) if _bypasses_ordering(error):
                    # Cancellation is teardown, not a result, so it never waits
                    # for its turn, in either mode.
                    raise error
                case _Fed(started, error):
                    started_total = started
                    if error is not None:
                        if not ordered or _bypasses_ordering(error):
                            raise error
                        # Hold it until every started item is delivered:
                        # the input ended here, so this is its position.
                        source_failure = error
                case _Value() | _Error() as result:
                    for ready in ordering.push(result, closes=True):
                        if isinstance(ready, _Error):
                            raise ready.error
                        yielded += 1
                        yield ready.value
                        slots.release()

        if source_failure is not None:
            raise source_failure


async def _flat_map[T, U](
    source: Source[T], fn: Expander[T, U], concurrency: int, buffer: int, ordered: bool
) -> AsyncIterator[U]:
    """The `flat_map` driver: bounded concurrent expansions.

    As in `_map`, a feeder owns the source and starts one expansion task per
    item. The driver reads from the queue, so a slow source can't delay output.

    Two semaphores do separate jobs: `slots` caps concurrent expansions,
    and `capacity` caps buffered values. Capacity returns after a value is yielded,
    so a slow consumer stalls production.

    Ordered mode changes both. A shared `capacity` would deadlock:
    an out-of-turn expansion could fill it while the current expansion still
    needs room to publish. Each expansion therefore gets its own allowance.
    Slots return only after an expansion's values are delivered, not when it
    finishes; otherwise the feeder could start more expansions while their
    output still takes memory.

    Each expansion can hold `buffer` published values plus one pulled while
    waiting for capacity. The ordered bound is `concurrency * (buffer + 1)`
    values, compared with `buffer + concurrency` unordered.
    """
    queue: asyncio.Queue[_QueueItem[U]] = asyncio.Queue()
    source_failures = _Failures[U](_Role.RETAIN)
    # An expansion's failure leaves the rest of the stage running, so its cleanup
    # failure must reach a consumer that's still reading, not just teardown.
    expansion_failures = _Failures[U](_Role.RETAIN_NOTIFY, queue)
    shared_capacity = asyncio.Semaphore(buffer)
    slots = asyncio.Semaphore(concurrency)
    expansions: set[asyncio.Task[None]] = set()
    capacities: dict[int, asyncio.Semaphore] = {}

    async def feed() -> None:
        started = 0
        failure: BaseException | None = None
        try:
            async with _OwnedSource(source, source_failures) as items:
                while True:
                    await slots.acquire()
                    try:
                        item = await anext(items)
                    except StopAsyncIteration:
                        return
                    capacity = shared_capacity
                    if ordered:
                        capacity = asyncio.Semaphore(buffer)
                        capacities[started] = capacity
                    # Start eagerly, as in `_map`'s feeder.
                    task = asyncio.create_task(
                        _emit_expansion(
                            fn, item, started, queue, capacity, expansion_failures
                        ),
                        eager_start=True,
                    )
                    expansions.add(task)
                    task.add_done_callback(expansions.discard)
                    started += 1
        except BaseException as exc:
            # Report it even when cancelled; see `_map`'s feeder.
            failure = exc
            if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
                raise
        finally:
            # A final count lets the driver count `_Done` messages instead of
            # inspecting a task set that done callbacks keep changing.
            queue.put_nowait(_Fed(started, failure))

    started_total: int | None = None
    finished = 0
    ordering = (
        _Ordered[_Value[U] | _Error | _Done]()
        if ordered
        else _Unordered[_Value[U] | _Error | _Done]()
    )
    source_failure: BaseException | None = None

    async with _Stage(
        asyncio.create_task(feed()), expansions, source_failures, expansion_failures
    ):
        while started_total is None or finished < started_total:
            deliverable: Iterable[_Value[U] | _Error | _Done] = ()
            match await queue.get():
                case _Value(value) if not ordered:
                    # Keep this fast path: the general path adds a push, a tuple,
                    # and another dispatch per value.
                    yield value
                    # Returned only once the consumer takes the value, so a slow
                    # consumer holds capacity and throttles production.
                    shared_capacity.release()
                case _Value() as message:
                    deliverable = ordering.push(message, closes=False)
                case _Done() as message:
                    finished += 1
                    deliverable = ordering.push(message, closes=True)
                case _Error(error) if _bypasses_ordering(error):
                    # Cancellation is teardown, not a result, so it never waits
                    # for its turn, in either mode.
                    raise error
                case _Error() as message:
                    deliverable = ordering.push(message, closes=False)
                case _Fed(started, error):
                    started_total = started
                    if error is not None:
                        if not ordered or _bypasses_ordering(error):
                            raise error
                        # Hold it until every started expansion is drained:
                        # the input ended here, so this is its position.
                        source_failure = error

            for message in deliverable:
                match message:
                    case _Value(value, seq):
                        yield value
                        # Returned only once the consumer takes the value,
                        # so a slow consumer holds capacity and throttles production.
                        (capacities[seq] if ordered else shared_capacity).release()
                    case _Error(error):
                        raise error
                    case _Done(seq):
                        # All values are delivered, so release the allowance and
                        # slot together. In unordered mode, delivery is complete
                        # as soon as the expansion finishes.
                        capacities.pop(seq, None)
                        slots.release()

        if source_failure is not None:
            raise source_failure


async def _emit_expansion[T, U](
    fn: Expander[T, U],
    item: T,
    seq: int,
    queue: asyncio.Queue[_QueueItem[U]],
    capacity: asyncio.Semaphore,
    failures: _Failures[U],
) -> None:
    """Run one expansion, publishing its values or failure to `queue`.

    Each value takes a unit of `capacity`, so an expansion that gets ahead of the
    consumer blocks instead of buffering its whole output. A final `_Done` always
    follows, so `_flat_map` can retire the task even when it fails.

    The owner closes the expanded iterator on every exit. A bare `async for` or
    `for` never calls a custom iterator's `aclose`/`close`, which leaks whatever
    it holds, like a cursor or a response body, when the pipeline stops early.

    Every failure is published, `BaseException` included, because the driver
    counts `_Done` messages rather than awaiting each task; an exception left on
    the task object would be silently dropped.
    """
    try:
        expanded = await _maybe_await(fn(item))
        if isinstance(expanded, AsyncIterable):
            async with _OwnedAsync(expanded, failures, seq) as async_values:
                async for value in async_values:
                    await capacity.acquire()
                    queue.put_nowait(_Value(value, seq))
        else:
            # Branch rather than adapt: `filter` expands each item to a sync list,
            # so this path is hot.
            with _OwnedSync(cast(Iterable[U], expanded), failures, seq) as sync_values:
                for value in sync_values:
                    await capacity.acquire()
                    queue.put_nowait(_Value(value, seq))
    except BaseException as exc:
        queue.put_nowait(_Error(exc, seq))
        if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
            raise
    finally:
        # `_Done` means the task finished, not that it succeeded. The queue is
        # unbounded on purpose, so both final messages go out without awaiting
        # during cancellation cleanup; `capacity`, not the queue, bounds buffering.
        queue.put_nowait(_Done(seq))


class _Ordered[M: _Positional]:
    """Replays positional messages in input order.

    `push` buffers a message by input position and returns messages ready for
    delivery, preserving arrival order within each position. With `closes=True`,
    the position retires once replayed: a `map` result closes its own position,
    and a `flat_map` `_Done` closes its expansion's position.

    The driver still yields values, returns slots and capacity, and raises
    failures. Those actions control backpressure and teardown; keeping them in
    the driver leaves this as a plain data structure.
    """

    def __init__(self) -> None:
        self._waiting: dict[int, list[M]] = {}
        self._closed: set[int] = set()
        self._turn = 0

    def push(self, message: M, *, closes: bool) -> Iterable[M]:
        """Buffer a message by position and return messages ready for delivery."""
        seq = message.seq
        self._waiting.setdefault(seq, []).append(message)
        if closes:
            self._closed.add(seq)
        if seq != self._turn:
            # A later position can't unblock the current one.
            return ()
        return self._replay()

    def _replay(self) -> Iterator[M]:
        while True:
            yield from self._waiting.pop(self._turn, ())
            if self._turn not in self._closed:
                return
            self._closed.discard(self._turn)
            self._turn += 1


class _Unordered[M]:
    """Passes messages straight through: completion order is arrival order.

    This shares `_Ordered`'s interface, so drivers choose a policy once rather
    than branch on the mode for each message.
    """

    def push(self, message: M, *, closes: bool) -> tuple[M, ...]:
        """Every message is in turn the moment it arrives."""
        return (message,)


class _Stage:
    """Supervises a concurrent stage's tasks and reports cleanup failures.

    Wrap it around the driver loop, including `yield`. On exit, it cancels tasks,
    waits for cleanup, and raises the first cleanup failure if allowed.

    It closes no sources itself. Iterator owners close them and report through
    `_Failures`, so tasks must finish before teardown reads those records.
    """

    def __init__(
        self,
        feeder: asyncio.Task[Any],
        tasks: set[asyncio.Task[Any]],
        *records: _Failures[Any],
    ) -> None:
        self._feeder = feeder
        # A live set: the feeder adds to it, and done callbacks remove from it.
        self._tasks = tasks
        self._records = records

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self, exc_type: Any, error: BaseException | None, traceback: Any
    ) -> bool:
        failure: BaseException | None = None
        try:
            # Feeder first, so its teardown failure takes precedence.
            await _cancel_tasks([self._feeder, *self._tasks])
        except BaseException as exc:
            failure = exc

        for record in self._records:
            if failure is None:
                failure = record.first

        if failure is not None and _may_surface(error):
            raise failure
        return False


def _is_cancelling() -> bool:
    """Whether the running task has a cancellation pending.

    Producers distinguish task cancellation from an error raised by their input.
    During teardown, the driver has stopped reading, so publishing a failure
    isn't enough: the task must also re-raise it.

    This governs task exceptions; `_may_surface` handles cleanup precedence.
    """
    task = asyncio.current_task()
    return task is not None and bool(task.cancelling())


async def _cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """Cancel tasks, wait for their cleanup, and raise any cleanup failure.

    Cancellation itself is expected and ignored. Anything else means a task's
    cleanup failed, which mustn't be swallowed; only the first one is raised.
    """
    task_list = list(tasks)
    for task in task_list:
        task.cancel()

    outcomes = await asyncio.gather(*task_list, return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, BaseException) and not isinstance(
            outcome, asyncio.CancelledError
        ):
            raise outcome


def _close(iterator: Iterator[Any]) -> None:
    """Close a sync iterator, if it has a `close` method.

    An abandoned generator is otherwise left to the cyclic collector,
    because a traceback keeps its frame reachable; closing it here is deterministic.
    Failures propagate, and the owner decides what to do with them.
    """
    close = getattr(iterator, "close", None)
    if close is not None:
        close()


async def _aclose(source: AsyncIterator[Any]) -> None:
    """Close an async iterator, if it has an `aclose` method.

    This lets stages release upstream generators promptly when the consumer
    stops early or an error interrupts iteration. Failures propagate,
    and the owner decides what to do with them.
    """
    aclose = getattr(source, "aclose", None)
    if aclose is not None:
        await aclose()


async def _maybe_await[T](value: T | Awaitable[T]) -> T:
    """Await `value` if it's awaitable; otherwise return it as is."""
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
