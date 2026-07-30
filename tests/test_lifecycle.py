"""Executable specification for iterator ownership and cleanup-failure delivery.

A `flowlet` stage owns the iterators it reads: the upstream source, and the
iterator a `flat_map` expander returns. Owning one means closing it on every exit
-- exhaustion, consumer close, read failure, cancellation -- and deciding how a
failure raised by *the close itself* reaches the consumer.

The policy is uniform: the same rules hold at every ownership boundary, for every
stage shape, and for sync and async iterators alike. That uniformity is the point
of this module -- one rule table applied everywhere, so a stage added later has a
specification to conform to instead of six precedents to imitate.

What is contractual:
  * which exception the consumer sees,
  * that the close was attempted exactly once,
  * that a cleanup failure never replaces a more useful error,
  * that no shape hangs.

What is deliberately not contractual: `__context__` structure. Whether a
suppressed cleanup failure is chained, recorded, or dropped is an implementation
detail. Nor is the exact number of values delivered once a failure is in flight
-- with concurrency that depends on scheduling -- so output is bounded rather
than pinned, and pinned exactly only where nothing fails.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from flowlet import pipe

READ_ERROR = "read boom"
CLOSE_ERROR = "close boom"
CANCEL_MESSAGE = "read cancel"
FN_ERROR = "fn boom"

ITEMS = 3
TIMEOUT = 5.0

# (terminal, close, consumer) -> the exception the consumer must see, or None.
#
# Two halves. `drain` asks for everything, so a failing read is always reached and
# always outranks a cleanup failure. `abandon` stops after one value, so the
# failing read is never reached -- which leaves a cleanup failure as the only
# thing left to report.
RULES: dict[tuple[str, str, str], tuple[type[BaseException], str] | None] = {
    ("exhaust", "ok", "drain"): None,
    ("exhaust", "ok", "abandon"): None,
    ("exhaust", "fail", "drain"): (ValueError, CLOSE_ERROR),
    ("exhaust", "fail", "abandon"): (ValueError, CLOSE_ERROR),
    ("read_error", "ok", "drain"): (RuntimeError, READ_ERROR),
    ("read_error", "ok", "abandon"): None,
    ("read_error", "fail", "drain"): (RuntimeError, READ_ERROR),
    ("read_error", "fail", "abandon"): (ValueError, CLOSE_ERROR),
    ("read_cancel", "ok", "drain"): (asyncio.CancelledError, CANCEL_MESSAGE),
    ("read_cancel", "ok", "abandon"): None,
    ("read_cancel", "fail", "drain"): (asyncio.CancelledError, CANCEL_MESSAGE),
    ("read_cancel", "fail", "abandon"): (ValueError, CLOSE_ERROR),
}


class Tracked:
    """Records what a stage did to the iterator it was given.

    A plain class, not a generator, so the cyclic collector can never stand in
    for an explicit close and let a stage that closes nothing pass.
    """

    def __init__(self) -> None:
        self.closed = 0
        self.reads = 0


def _next_value(state: Tracked, items: int, terminal: str) -> int:
    """Yield `items` values, then end however `terminal` says."""
    state.reads += 1
    if state.reads <= items:
        return state.reads
    if terminal == "exhaust":
        raise StopIteration
    if terminal == "read_error":
        raise RuntimeError(READ_ERROR)
    raise asyncio.CancelledError(CANCEL_MESSAGE)


class SyncIterator:
    def __init__(self, state: Tracked, items: int, terminal: str, close: str) -> None:
        self.state, self.items, self.terminal, self.close_mode = state, items, terminal, close

    def __iter__(self) -> "SyncIterator":
        return self

    def __next__(self) -> int:
        return _next_value(self.state, self.items, self.terminal)

    def close(self) -> None:
        self.state.closed += 1
        if self.close_mode == "fail":
            raise ValueError(CLOSE_ERROR)


class AsyncIterator:
    def __init__(self, state: Tracked, items: int, terminal: str, close: str) -> None:
        self.state, self.items, self.terminal, self.close_mode = state, items, terminal, close

    def __aiter__(self) -> "AsyncIterator":
        return self

    async def __anext__(self) -> int:
        try:
            return _next_value(self.state, self.items, self.terminal)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self) -> None:
        self.state.closed += 1
        if self.close_mode == "fail":
            raise ValueError(CLOSE_ERROR)


type Factory = Callable[[Tracked, int, str, str], Any]

KINDS: dict[str, Factory] = {"sync": SyncIterator, "async": AsyncIterator}

# Each shape maps to what a clean drain of a 3-item source produces. Ownership
# behavior must not vary with the stage in front of it.
SOURCE_SHAPES: dict[str, tuple[Callable[[Any], Any], list[Any]]] = {
    "plain": (lambda source: pipe(source), [1, 2, 3]),
    "map_c1": (lambda source: pipe(source).map(lambda x: x, concurrency=1), [1, 2, 3]),
    "map_c2": (lambda source: pipe(source).map(lambda x: x, concurrency=2), [1, 2, 3]),
    # Ordering changes when a result is delivered, never how the stage cleans up.
    "map_c2_ordered": (lambda source: pipe(source).map(lambda x: x, concurrency=2, ordered=True), [1, 2, 3]),
    "flat_map_c1": (lambda source: pipe(source).flat_map(lambda x: [x], concurrency=1), [1, 2, 3]),
    "flat_map_c2": (lambda source: pipe(source).flat_map(lambda x: [x], concurrency=2), [1, 2, 3]),
    "batch2": (lambda source: pipe(source).batch(2), [[1, 2], [3]]),
}

EXPANSION_SHAPES: dict[str, tuple[Callable[[Callable[[], Any]], Any], list[Any]]] = {
    "expansion_c1": (lambda make: pipe([0]).flat_map(lambda _: make(), concurrency=1), [1, 2, 3]),
    "expansion_c2": (lambda make: pipe([0]).flat_map(lambda _: make(), concurrency=2), [1, 2, 3]),
}


async def drain(stream: Any) -> tuple[list[Any], BaseException | None]:
    """Ask for everything, reporting what arrived and what stopped it."""
    got: list[Any] = []

    async def consume() -> None:
        async for value in stream:
            got.append(value)

    try:
        await asyncio.wait_for(consume(), timeout=TIMEOUT)
    except TimeoutError as exc:  # pragma: no cover - a hang is a failure, not an outcome
        pytest.fail(f"stage hung after {got}: {exc!r}")
    except BaseException as exc:  # noqa: BLE001 - the exception under test
        return got, exc
    return got, None


async def abandon(stream: Any, take: int = 1) -> tuple[list[Any], BaseException | None]:
    """Take `take` values then close, reporting what `aclose()` raised."""
    iterator = stream.__aiter__()
    got: list[Any] = []
    while len(got) < take:
        got.append(await anext(iterator))
    try:
        await asyncio.wait_for(iterator.aclose(), timeout=TIMEOUT)
    except TimeoutError as exc:  # pragma: no cover - a hang is a failure, not an outcome
        pytest.fail(f"aclose() hung after {got}: {exc!r}")
    except BaseException as exc:  # noqa: BLE001 - the exception under test
        return got, exc
    return got, None


def assert_cell(
    values: list[Any],
    error: BaseException | None,
    state: Tracked,
    *,
    rule: tuple[type[BaseException], str] | None,
    clean_output: list[Any],
    exact_output: bool,
) -> None:
    """Check one cell of the matrix against the rule table."""
    assert state.closed == 1, f"expected exactly one close, got {state.closed}"

    if rule is None:
        assert error is None, f"expected a clean exit, got {error!r}"
    else:
        expected_type, expected_message = rule
        assert type(error) is expected_type, f"expected {expected_type.__name__}, got {error!r}"
        assert str(error) == expected_message

    assert len(values) <= len(clean_output)
    for value in values:
        assert value in clean_output, f"stage invented {value!r}"
    if exact_output:
        assert values == clean_output


@pytest.mark.parametrize(("terminal", "close", "consumer"), list(RULES))
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize("shape", list(SOURCE_SHAPES))
@pytest.mark.asyncio
async def test_source_ownership(shape: str, kind: str, terminal: str, close: str, consumer: str) -> None:
    """Every stage closes its upstream source and delivers failures by one rule."""
    build, clean_output = SOURCE_SHAPES[shape]
    state = Tracked()
    stream = build(KINDS[kind](state, ITEMS, terminal, close))

    values, error = await (drain(stream) if consumer == "drain" else abandon(stream))

    assert_cell(
        values,
        error,
        state,
        rule=RULES[terminal, close, consumer],
        clean_output=clean_output,
        exact_output=(terminal, close, consumer) == ("exhaust", "ok", "drain"),
    )


@pytest.mark.parametrize(("terminal", "close", "consumer"), list(RULES))
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize("shape", list(EXPANSION_SHAPES))
@pytest.mark.asyncio
async def test_expansion_ownership(shape: str, kind: str, terminal: str, close: str, consumer: str) -> None:
    """`flat_map` owns the iterator its expander returned, under the same rules."""
    build, clean_output = EXPANSION_SHAPES[shape]
    state = Tracked()
    stream = build(lambda: KINDS[kind](state, ITEMS, terminal, close))

    values, error = await (drain(stream) if consumer == "drain" else abandon(stream))

    assert_cell(
        values,
        error,
        state,
        rule=RULES[terminal, close, consumer],
        clean_output=clean_output,
        exact_output=(terminal, close, consumer) == ("exhaust", "ok", "drain"),
    )


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.asyncio
async def test_expansion_cleanup_failure_stops_a_live_stage(kind: str, concurrency: int) -> None:
    """A cleanup failure must reach a consumer that is still reading.

    Recording it for teardown is not enough on its own: the stage here has plenty
    of work left, so a recorded-only failure would not be reported until the
    source ran dry -- and against an endless source, never.
    """
    state = Tracked()
    expansions = 0

    def expand(_: int) -> Any:
        nonlocal expansions
        expansions += 1
        if expansions == 1:
            return KINDS[kind](state, 1, "exhaust", "fail")
        return [0] * 5

    values, error = await drain(pipe(range(50)).flat_map(expand, concurrency=concurrency))

    assert type(error) is ValueError
    assert str(error) == CLOSE_ERROR
    assert state.closed == 1
    assert len(values) < 50, "the stage drained its source instead of failing fast"


STAGE_ERROR_SHAPES: dict[str, Callable[[Any, Callable[[int], Any]], Any]] = {
    "map_c1": lambda source, fn: pipe(source).map(fn, concurrency=1),
    "map_c2": lambda source, fn: pipe(source).map(fn, concurrency=2),
    "map_c2_ordered": lambda source, fn: pipe(source).map(fn, concurrency=2, ordered=True),
    "flat_map_c1": lambda source, fn: pipe(source).flat_map(fn, concurrency=1),
    "flat_map_c2": lambda source, fn: pipe(source).flat_map(fn, concurrency=2),
    "filter_c2": lambda source, fn: pipe(source).filter(fn, concurrency=2),
}


@pytest.mark.parametrize("close", ["ok", "fail"])
@pytest.mark.parametrize("kind", list(KINDS))
@pytest.mark.parametrize("shape", list(STAGE_ERROR_SHAPES))
@pytest.mark.asyncio
async def test_stage_failure_outranks_a_failing_source_close(shape: str, kind: str, close: str) -> None:
    """The stage function's error is the useful one; cleanup must not replace it."""

    def boom(_: int) -> int:
        raise RuntimeError(FN_ERROR)

    state = Tracked()
    stream = STAGE_ERROR_SHAPES[shape](KINDS[kind](state, ITEMS, "exhaust", close), boom)

    values, error = await drain(stream)

    assert type(error) is RuntimeError, f"cleanup replaced the stage failure: {error!r}"
    assert str(error) == FN_ERROR
    assert state.closed == 1
    assert values == []
