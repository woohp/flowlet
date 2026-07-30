import asyncio
import enum
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
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
    """One result, tagged with the input position it came from."""

    value: T
    seq: int


@dataclass(frozen=True)
class _Error:
    """One item's failure, tagged with the input position it came from."""

    error: BaseException
    seq: int


@dataclass(frozen=True)
class _Done:
    """One expansion task terminated, successfully or not."""

    seq: int


@dataclass(frozen=True)
class _Fed:
    """The feeder stopped after starting `started` items, `error` if it failed.

    One terminal message rather than an error followed by a count. A source
    failure has no input position of its own -- it is what *ended* the input --
    so an ordered driver has to hold it until every started item is delivered,
    which means telling it apart from an item's failure.
    """

    started: int
    error: BaseException | None = None


_UNPOSITIONED = -1
"""Position for an ownership boundary that does not belong to one input item.

A source boundary is the whole input, not a position within it, and its cleanup
failures are recorded for teardown rather than queued -- so no ordered driver
reads this. Only an expansion boundary carries a real position.
"""


type _MapItem[T] = _Value[T] | _Error | _Fed
type _QueueItem[T] = _Value[T] | _Error | _Done | _Fed

DEFAULT_BUFFER = 256
"""Default number of expanded values `flat_map` holds for a lagging consumer.

Bounding this is what gives the stage backpressure. Raising it trades memory for
throughput on wide expansions, because every value produced past the bound needs
an event-loop round trip to reclaim a slot; lowering it does the reverse.
"""


class _Role(enum.Enum):
    """How an ownership boundary delivers a failure raised by closing.

    Static: a property of the boundary, fixed when the owner is built, and not of
    any particular exit -- that is `_Exit`. Collapsing the two into one flag is
    what made this policy hard to keep correct across six call sites.
    """

    DIRECT = "direct"
    """Raise it at the close. For boundaries whose exceptions reach the consumer
    directly: the public wrapper and the sequential stage paths."""

    RETAIN = "retain"
    """Record it for terminal teardown. For a source read by a feeder task, whose
    exceptions reach the consumer only through a queue the consumer may already
    have stopped reading. An exhausted source cannot run away, so the driver
    delivers its in-flight work and teardown reports the failure afterwards."""

    RETAIN_NOTIFY = "retain_notify"
    """Record it and also wake the driver. For a `flat_map` expansion, whose
    failure leaves the rest of the stage running: recording alone would not be
    reported until the source ran dry, and against an endless source never."""


class _Exit(enum.Enum):
    """Why a consumption body ended. Dynamic: potentially different every exit."""

    NORMAL = "normal"
    CLOSED = "closed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @property
    def may_surface(self) -> bool:
        """Whether a cleanup failure is the most useful thing left to report.

        Both directions matter: masking the error that caused the teardown loses
        the diagnosis, but silent success from `aclose()` hides a leaked
        resource. So it is reported only when nothing better already is.
        """
        return self is _Exit.NORMAL or self is _Exit.CLOSED


def _exit_reason(error: BaseException | None) -> _Exit:
    """Classify how a consumption body ended, from the exception leaving it."""
    if error is None:
        return _Exit.NORMAL
    if isinstance(error, GeneratorExit):
        return _Exit.CLOSED
    if isinstance(error, asyncio.CancelledError):
        return _Exit.CANCELLED
    return _Exit.FAILED


class _Failures[U]:
    """Durable record of one stage's cleanup failures, and how they are sent.

    A cleanup failure outlives the queue. The driver stops reading during
    teardown, so a failure published only to the queue is lost; but a failure
    raised at the point of closing would mask whatever caused the teardown.
    Recording keeps it where teardown can still find it, and `role` decides
    whether it also reaches a consumer that is still reading.
    """

    def __init__(self, role: _Role, queue: asyncio.Queue[_QueueItem[U]] | None = None) -> None:
        self._role = role
        self._queue = queue
        self._recorded: list[BaseException] = []

    def report(self, error: BaseException, reason: _Exit, seq: int) -> None:
        """Deliver a close failure as this boundary's role requires.

        `seq` is the input position the boundary belongs to, so an ordered driver
        can hold a notified failure until its turn instead of jumping the queue.
        """
        if self._role is _Role.DIRECT:
            if reason.may_surface:
                raise error
            return
        self._recorded.append(error)
        if self._role is _Role.RETAIN_NOTIFY and self._queue is not None and reason.may_surface:
            self._queue.put_nowait(_Error(error, seq))

    @property
    def first(self) -> BaseException | None:
        """The first recorded failure, for teardown to surface."""
        return self._recorded[0] if self._recorded else None


async def _adapt_sync[T](iterator: Iterator[T]) -> AsyncIterator[T]:
    """Present a synchronous iterator as an asynchronous one.

    Iteration only: the caller owns the wrapped iterator and closes it.
    """
    for item in iterator:
        yield item


class _OwnedAsync[T]:
    """Owns an async iterator's lifetime for the duration of a consumption body.

    Enter it around the code that reads the iterator; `__aexit__` closes the
    iterator and reports any close failure with the reason the *body* ended.

    Do not infer that reason from the iterator's own iteration instead. A body
    that fails after receiving a value closes the iterator, so the close sees
    only the `GeneratorExit` it was sent and reads ordinary abandonment --
    measurably masking the body's error, and sending an unretractable
    notification to an already-failing driver.
    """

    def __init__(self, source: AsyncIterable[T], failures: _Failures[Any], seq: int = _UNPOSITIONED) -> None:
        self._source = source
        self._failures = failures
        self._seq = seq
        self._iterator: AsyncIterator[T] | None = None

    async def __aenter__(self) -> AsyncIterator[T]:
        self._iterator = aiter(self._source)
        return self._iterator

    async def __aexit__(self, exc_type: Any, error: BaseException | None, traceback: Any) -> bool:
        if self._iterator is not None:
            try:
                await _aclose(self._iterator)
            except BaseException as close_error:
                self._failures.report(close_error, _exit_reason(error), self._seq)
        return False


class _OwnedSync[T]:
    """Owns a sync iterator, under the same policy as `_OwnedAsync`.

    Synchronous on purpose, twice over: nothing here awaits, and the async
    protocol costs two coroutines per owner (~10% on `filter`, which builds one
    per item); and the body gets the iterator itself, not an async view, because
    adapting it measured ~40% slower on `plain sync src`.
    """

    def __init__(self, source: Iterable[T], failures: _Failures[Any], seq: int = _UNPOSITIONED) -> None:
        self._source = source
        self._failures = failures
        self._seq = seq
        self._iterator: Iterator[T] | None = None

    def __enter__(self) -> Iterator[T]:
        self._iterator = iter(self._source)
        return self._iterator

    def __exit__(self, exc_type: Any, error: BaseException | None, traceback: Any) -> None:
        # Returns None, never True: an owner reports cleanup failures, it never
        # swallows the exception that ended the body.
        if self._iterator is not None:
            try:
                _close(self._iterator)
            except BaseException as close_error:
                self._failures.report(close_error, _exit_reason(error), self._seq)


class _OwnedSource[T]:
    """Owns either flavor of source, presenting one async view to the body.

    For bodies that pull with `anext` and so cannot cheaply branch on the
    source's flavor -- the concurrent feeders. A synchronous source pays one
    adapter generator here, the same layer the previous `_iterate_source` cost.
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

    async def __aexit__(self, exc_type: Any, error: BaseException | None, traceback: Any) -> bool:
        try:
            if self._async_iterator is not None:
                await _aclose(self._async_iterator)
            elif self._sync_iterator is not None:
                _close(self._sync_iterator)
        except BaseException as close_error:
            # A source is the whole input rather than a position in it.
            self._failures.report(close_error, _exit_reason(error), _UNPOSITIONED)
        return False


async def to_async_iter[T](source: Source[T]) -> AsyncIterator[T]:
    """Yield items from an iterable or async iterable as an async iterator.

    Closing this iterator also closes the wrapped source. Without that, closing
    the wrapper would release nothing and the source's own cleanup would wait for
    garbage collection.

    A close failure surfaces from `aclose()` unless an error is already
    propagating out of this iterator. The `AsyncIterator` protocol sets a limit
    here: `aclose()` takes no exception argument, so a *caller's* own failure
    cannot be known, and a close failure then surfaces as though the caller had
    simply walked away.
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
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = False
) -> Operator[T, U]:
    """Return an operator that applies `fn` to each source item.

    `fn` may be sync or async. With `concurrency > 1`, outputs are yielded in
    completion order.

    `ordered` yields them in input order instead: the stream becomes exactly what
    `concurrency=1` would produce -- same values, same order, same failure at the
    same point -- with only the timing differing. Note that this includes waiting
    on a slow item: if item 1 hangs and item 2 fails, an ordered stage waits where
    an unordered one reports the failure at once.
    """
    _validate_concurrency(concurrency)

    def apply(source: Source[T]) -> AsyncIterator[U]:
        # Return the driver itself rather than re-yielding from it. `async for`
        # does not close its iterator when GeneratorExit passes through, so a
        # wrapper here would swallow the close and leave the driver's cleanup --
        # cancelling tasks, closing the source -- to garbage collection.
        return _map(source, fn, concurrency, ordered)

    return apply


def flat_map[T, U](
    fn: Expander[T, U], *, concurrency: int = 1, buffer: int = DEFAULT_BUFFER, ordered: bool = False
) -> Operator[T, U]:
    """Return an operator that expands each source item into zero or more outputs.

    `fn` may return an iterable, async iterable, or an awaitable resolving to
    either. Concurrent expansions yield values as they become available.

    `buffer` caps how many expanded values may wait for the consumer. Once it is
    full, expansions pause instead of buffering, so large expansions stream. Wide
    expansions go faster with a larger `buffer` at the cost of holding more
    values in memory.

    `ordered` groups the output by input instead: every value from item *n*, in
    the order that expansion produced them, precedes any value from item *n + 1*.
    Two costs come with it. Each in-flight expansion gets its own allowance, so the
    stage holds up to `concurrency * (buffer + 1)` values rather than
    `buffer + concurrency`. And an expansion that never ends starves every item
    behind it, where unordered mode would interleave -- so it suits bounded
    expansions.
    """
    _validate_concurrency(concurrency)
    _validate_buffer(buffer)

    def apply(source: Source[T]) -> AsyncIterator[U]:
        # Returned directly, not re-yielded from: see the note in `map`.
        return _flat_map(source, fn, concurrency, buffer, ordered)

    return apply


def filter[T](pred: Predicate[T], *, concurrency: int = 1, ordered: bool = False) -> Operator[T, T]:  # noqa: A001
    """Return an operator that keeps items where `pred` returns true.

    `pred` may be sync or async. With `concurrency > 1`, kept items are yielded
    in completion order, or in input order when `ordered` is set.
    """

    async def expand(item: T) -> list[T]:
        return [item] if await _maybe_await(pred(item)) else []

    # Each expansion is at most one value, so the memory ordered `flat_map` warns
    # about cannot be reached here: `concurrency` values, whatever `buffer` says.
    return flat_map(expand, concurrency=concurrency, ordered=ordered)


def batch[T](size: int) -> Operator[T, list[T]]:
    """Return an operator that collects items into lists of up to `size`.

    The last emitted list may contain fewer than `size` items when the source
    is exhausted with a partial group.
    """
    if size < 1:
        raise ValueError("size must be >= 1")

    async def apply(source: Source[T]) -> AsyncIterator[list[T]]:
        chunk: list[T] = []
        # Owning `to_async_iter`'s wrapper rather than the raw source keeps the
        # sync/async branch in one place, and costs the same single layer.
        async with _OwnedAsync(to_async_iter(source), _Failures[Any](_Role.DIRECT)) as items:
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
    source: Source[T], fn: Callable[[T], U] | Callable[[T], Awaitable[U]], concurrency: int, ordered: bool
) -> AsyncIterator[U]:
    """Map a source lazily with bounded in-flight calls.

    The sequential path avoids task scheduling overhead. On the concurrent path a
    single feeder task owns the source and starts one worker per item, while this
    driver only ever reads finished results from a queue. Keeping the source off
    the driver's await path is what lets a finished result be yielded while the
    source is still blocked producing the next item.

    A slot, returned once a value is yielded, bounds started-but-unyielded work at
    `concurrency` and gives the stage backpressure. `ordered` therefore needs no
    bound of its own: at most `concurrency` finished results can be waiting for
    their turn, because a slot is not returned until its result is delivered.
    """
    if concurrency == 1:
        # Ordering is inherent here, so `ordered` must not move a stage off this
        # path. The owner sees the whole body, so a failure from `fn` correctly
        # outranks a failure from closing the source.
        async with _OwnedAsync(to_async_iter(source), _Failures[Any](_Role.DIRECT)) as items:
            async for item in items:
                yield await _apply_map(fn, item)
        return

    failures = _Failures[U](_Role.RETAIN)
    results: asyncio.Queue[_MapItem[U]] = asyncio.Queue()
    slots = asyncio.Semaphore(concurrency)
    workers: set[asyncio.Task[None]] = set()

    async def run(seq: int, item: T) -> None:
        # Every exit publishes exactly one message, cancellation included, so the
        # driver's count of outstanding work can never leave it stranded on `get`.
        try:
            value = await _apply_map(fn, item)
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
                    task = asyncio.create_task(run(started, item))
                    workers.add(task)
                    task.add_done_callback(workers.discard)
                    started += 1
        except BaseException as exc:
            # Reported even when cancelled: a source that raises CancelledError
            # must surface, not look like a short stream. When the cancellation is
            # this driver tearing the feeder down, it has already stopped reading
            # and the message is discarded.
            failure = exc
            if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
                raise
        finally:
            # Reporting the count is what makes termination race-free: the driver
            # never has to inspect a worker set that done callbacks mutate.
            results.put_nowait(_Fed(started, failure))

    started_total: int | None = None
    yielded = 0
    # Ordered only: results that arrived before their turn, and the turn to fill.
    waiting: dict[int, _Value[U] | _Error] = {}
    next_seq = 0
    source_failure: BaseException | None = None

    async with _Stage(failures) as stage:
        stage.supervise_group(workers)
        stage.supervise(asyncio.create_task(feed()))
        while started_total is None or yielded < started_total:
            match await results.get():
                case _Value(value) if not ordered:
                    yielded += 1
                    yield value
                    slots.release()
                case _Error(error) if not ordered:
                    raise error
                case _Fed(started, error):
                    started_total = started
                    if error is not None:
                        if not ordered:
                            raise error
                        # Held until every started item is delivered: the input
                        # ended here, so this is that position.
                        source_failure = error
                case _Value() | _Error() as result:
                    waiting[result.seq] = result
                    while next_seq in waiting:
                        ready = waiting.pop(next_seq)
                        next_seq += 1
                        if isinstance(ready, _Error):
                            raise ready.error
                        yielded += 1
                        yield ready.value
                        slots.release()

        if source_failure is not None:
            raise source_failure


async def _apply_map[T, U](fn: Callable[[T], U] | Callable[[T], Awaitable[U]], item: T) -> U:
    """Apply a map function and await the result only when needed."""
    return await _maybe_await(fn(item))


async def _flat_map[T, U](
    source: Source[T], fn: Expander[T, U], concurrency: int, buffer: int, ordered: bool
) -> AsyncIterator[U]:
    """Flat-map a source with bounded concurrent expansions.

    A feeder task owns the source and starts one expansion task per item, while
    this driver only ever reads from the queue. Keeping the source off the
    driver's await path is what lets a ready value be yielded while the source is
    still blocked producing the next item.

    Two semaphores do separate jobs. `slots` bounds concurrent expansions.
    `capacity`, returned after a value is yielded, bounds unconsumed values so a
    slow consumer stalls production instead of letting expansions buffer freely.

    `ordered` changes both. One shared `capacity` would deadlock: an expansion
    that finishes early fills it with values awaiting its turn, and the driver
    cannot drain them until the in-turn expansion -- now unable to publish --
    finishes. Each expansion therefore gets its own allowance, so an out-of-turn
    one parks against its own and blocks nobody. And a slot is returned only once
    an expansion's values have been *delivered*, not when it terminates: an
    expansion holding undelivered values still holds memory, so freeing its slot
    early would let the feeder start another and break the bound.

    The price is memory. Each expansion holds up to `buffer` published values plus
    one it has already pulled while waiting for a permit, so an ordered stage holds
    up to `concurrency * (buffer + 1)` values against `buffer + concurrency`
    unordered -- both measured.
    """
    queue: asyncio.Queue[_QueueItem[U]] = asyncio.Queue()
    source_failures = _Failures[U](_Role.RETAIN)
    # An expansion failing leaves the rest of the stage running, so its cleanup
    # failure has to reach a consumer that is still reading, not just teardown.
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
                    task = asyncio.create_task(_emit_expansion(fn, item, started, queue, capacity, expansion_failures))
                    expansions.add(task)
                    task.add_done_callback(expansions.discard)
                    started += 1
        except BaseException as exc:
            # Reported even when cancelled; see the note in `_map`'s feeder.
            failure = exc
            if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
                raise
        finally:
            # Reporting the count is what makes termination race-free: the driver
            # counts `_Done` messages instead of inspecting a mutating task set.
            queue.put_nowait(_Fed(started, failure))

    started_total: int | None = None
    finished = 0
    # Ordered only: values held for an expansion whose turn has not come, which
    # expansions have terminated, how they failed, and the turn to fill.
    buffered: dict[int, list[U]] = {}
    done: set[int] = set()
    failed: dict[int, BaseException] = {}
    next_seq = 0
    source_failure: BaseException | None = None

    async with _Stage(source_failures, expansion_failures) as stage:
        stage.supervise_group(expansions)
        stage.supervise(asyncio.create_task(feed()))
        while started_total is None or finished < started_total:
            match await queue.get():
                case _Value(value) if not ordered:
                    yield value
                    # Returned only after the consumer takes the value, so a
                    # slow consumer holds the slot and throttles production.
                    shared_capacity.release()
                case _Error(error) if not ordered:
                    raise error
                case _Done() if not ordered:
                    finished += 1
                    slots.release()
                case _Fed(started, error):
                    started_total = started
                    if error is not None:
                        if not ordered:
                            raise error
                        # Held until every started expansion is drained: the input
                        # ended here, so this is that position.
                        source_failure = error
                case _Value(value, seq):
                    buffered.setdefault(seq, []).append(value)
                case _Error(error, seq):
                    failed.setdefault(seq, error)
                case _Done(seq):
                    finished += 1
                    done.add(seq)

            while ordered:
                for value in buffered.pop(next_seq, ()):
                    yield value
                    capacities[next_seq].release()
                if next_seq in failed:
                    raise failed[next_seq]
                if next_seq not in done:
                    break
                capacities.pop(next_seq, None)
                next_seq += 1
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
    """Run one flat-map expansion and publish its values or error to `queue`.

    Each value costs a capacity slot, so an expansion blocks once it runs ahead
    of the consumer rather than buffering its whole output. A final `_Done` is
    always sent, so `_flat_map` can retire this task even when it raises.

    The owner closes the expanded iterator on every exit: a bare `async for` or
    `for` leaves a custom iterator's `aclose`/`close` uncalled, leaking an
    expansion that holds a cursor or response body when the pipeline stops early.

    Every failure is published, `BaseException` included, because the driver
    counts `_Done` rather than awaiting each task; an exception left on the task
    object is silently dropped.
    """
    try:
        expanded = await _maybe_await(fn(item))
        if isinstance(expanded, AsyncIterable):
            async with _OwnedAsync(expanded, failures, seq) as async_values:
                async for value in async_values:
                    await capacity.acquire()
                    queue.put_nowait(_Value(value, seq))
        else:
            # Branched rather than adapted: `filter` expands to a sync list per
            # item, so this is a hot path.
            with _OwnedSync(cast(Iterable[U], expanded), failures, seq) as sync_values:
                for value in sync_values:
                    await capacity.acquire()
                    queue.put_nowait(_Value(value, seq))
    except BaseException as exc:
        queue.put_nowait(_Error(exc, seq))
        if isinstance(exc, asyncio.CancelledError) or _is_cancelling():
            raise
    finally:
        # _Done means the task terminated, not that it completed successfully.
        # The queue is intentionally unbounded so both terminal messages can be
        # sent without awaiting during cancellation cleanup; `capacity`, not the
        # queue size, is what bounds buffering.
        queue.put_nowait(_Done(seq))


class _Stage:
    """Supervises a concurrent stage's tasks and its final failure precedence.

    Enter it around the driver loop, `yield` included. On exit it cancels
    everything it supervises, waits for their cleanup, then surfaces the first
    recorded cleanup failure if the exit reason permits.

    Sources are not closed here: each owner closes what it owns as its own body
    ends and reports through a `_Failures` record, which is why the tasks are
    cancelled and awaited first.
    """

    def __init__(self, *records: _Failures[Any]) -> None:
        self._records = records
        self._tasks: list[asyncio.Task[Any]] = []
        self._groups: list[set[asyncio.Task[Any]]] = []

    def supervise(self, task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        """Supervise one task, returning it for the caller to keep."""
        self._tasks.append(task)
        return task

    def supervise_group(self, tasks: set[asyncio.Task[Any]]) -> None:
        """Supervise a live set that done callbacks add to and remove from."""
        self._groups.append(tasks)

    async def __aenter__(self) -> "_Stage":
        return self

    async def __aexit__(self, exc_type: Any, error: BaseException | None, traceback: Any) -> bool:
        failure: BaseException | None = None
        try:
            await _cancel_tasks([*self._tasks, *(task for group in self._groups for task in group)])
        except BaseException as exc:
            failure = exc

        for record in self._records:
            if failure is None:
                failure = record.first

        if failure is not None and _exit_reason(error).may_surface:
            raise failure
        return False


def _is_cancelling() -> bool:
    """Whether the running task has a pending cancellation request.

    Producer tasks use this to tell "I am being torn down" from "the thing I was
    reading raised". In the first case the driver has stopped reading, so
    publishing to the queue is not enough and the failure must also be re-raised.

    Concerns a *task's* exception, not cleanup precedence -- `_Exit` covers that.
    """
    task = asyncio.current_task()
    return task is not None and bool(task.cancelling())


async def _cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """Cancel tasks, wait for their cleanup, and surface any cleanup failure.

    Cancellation itself is expected and ignored; anything else means a task's
    cleanup failed and must not be swallowed. Only the first failure is raised.
    """
    task_list = list(tasks)
    for task in task_list:
        task.cancel()
    if not task_list:
        return

    outcomes = await asyncio.gather(*task_list, return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
            raise outcome


def _close(iterator: Iterator[Any]) -> None:
    """Close a synchronous iterator when it exposes `close`.

    Abandoning a generator relies on the cyclic collector to close it, because a
    live traceback keeps its frame reachable; closing here makes it deterministic.
    Failures propagate: the owner decides what to do with them.
    """
    close = getattr(iterator, "close", None)
    if close is not None:
        close()


async def _aclose(source: AsyncIterator[Any]) -> None:
    """Close an async iterator when it exposes `aclose`.

    This lets pipeline stages release upstream generators promptly when a
    downstream consumer stops early or an error interrupts iteration. Failures
    propagate: the owner decides what to do with them.
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
