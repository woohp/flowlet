import asyncio
import contextvars
import functools
import multiprocessing as mp
import threading
import time
from collections.abc import AsyncIterator, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, cast

import pytest

import flowlet.functional as F
from flowlet import Flow, Pipeline, in_process, in_thread, op, pipe, thread_local


async def double(x: int) -> int:
    return x * 2


async def add_one(x: int) -> int:
    return x + 1


def to_str(x: int) -> str:
    return str(x)


def process_double(x: int) -> int:
    return x * 2


def process_is_even(x: int) -> bool:
    return x % 2 == 0


def process_expand(x: int) -> list[int]:
    return [x, x * 10]


async def settle(turns: int = 20) -> None:
    """Give a blocked feeder time to reveal any extra work it could start.

    Yield a fixed number of event-loop turns, not a wall-clock delay.
    Waking on a slot, pulling an item, and starting a task take only a few turns,
    regardless of machine speed.
    """
    for _ in range(turns):
        await asyncio.sleep(0)


class TestPipelineApi:
    @pytest.mark.asyncio
    async def test_method_chain_collects_results(self) -> None:
        result: list[int] = await pipe([1, 2, 3]).map(double).map(add_one).collect()

        assert result == [3, 5, 7]

    @pytest.mark.asyncio
    async def test_operator_composition(self) -> None:
        pipeline: Pipeline[int] = pipe([1, 2, 3]) | op.map(double) | op.map(add_one)

        assert await pipeline.collect() == [3, 5, 7]

    @pytest.mark.asyncio
    async def test_reusable_flow(self) -> None:
        transform: Flow[int, str] = Flow[int]().map(double).map(to_str)

        result: list[str] = await pipe([1, 2, 3]).through(transform).collect()

        assert result == ["2", "4", "6"]

    @pytest.mark.asyncio
    async def test_pipeline_is_immutable(self) -> None:
        base: Pipeline[int] = pipe([1, 2, 3])
        doubled: Pipeline[int] = base.map(double)

        assert await base.collect() == [1, 2, 3]
        assert await doubled.collect() == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_pipeline_can_run_twice(self) -> None:
        pipeline: Pipeline[int] = pipe([1, 2, 3]).map(double)

        assert await pipeline.collect() == [2, 4, 6]
        assert await pipeline.collect() == [2, 4, 6]


class TestCardinality:
    @pytest.mark.asyncio
    async def test_filter_keeps_matching_items(self) -> None:
        result: list[int] = (
            await pipe([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).collect()
        )

        assert result == [2, 4]

    @pytest.mark.asyncio
    async def test_flat_map_can_emit_zero_or_many_items(self) -> None:
        def expand(x: int) -> list[int]:
            return [] if x == 2 else [x, x * 10]

        result: list[int] = await pipe([1, 2, 3]).flat_map(expand).collect()

        assert result == [1, 10, 3, 30]

    @pytest.mark.asyncio
    async def test_flat_map_streams_async_expansions(self) -> None:
        never = asyncio.Event()

        async def expand(x: int) -> AsyncIterator[int]:
            yield x
            await never.wait()

        stream = pipe([1]).flat_map(expand).__aiter__()

        try:
            assert await asyncio.wait_for(anext(stream), timeout=0.1) == 1
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_flat_map_close_cancels_running_expansion(self) -> None:
        cancelled = asyncio.Event()

        async def expand(x: int) -> AsyncIterator[int]:
            try:
                yield x
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        stream = pipe([1]).flat_map(expand).__aiter__()

        assert await asyncio.wait_for(anext(stream), timeout=0.1) == 1
        await cast(Any, stream).aclose()

        await asyncio.wait_for(cancelled.wait(), timeout=0.1)

    @pytest.mark.asyncio
    async def test_none_is_a_normal_value(self) -> None:
        def maybe_value(x: int) -> list[int | None]:
            return [None] if x % 2 else [x]

        result: list[int | None] = await pipe([1, 2, 3]).flat_map(maybe_value).collect()

        assert result == [None, 2, None]

    @pytest.mark.asyncio
    async def test_empty_multi_stage_pipeline_finishes(self) -> None:
        result: list[int] = await pipe([]).map(double).map(add_one).collect()

        assert result == []


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_large_input_does_not_deadlock(self) -> None:
        result: list[int] = await asyncio.wait_for(
            pipe(range(100)).map(double, concurrency=10).collect(), timeout=1
        )

        assert sorted(result) == [x * 2 for x in range(100)]

    @pytest.mark.asyncio
    async def test_map_emits_completion_order(self) -> None:
        async def slow_inverse(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x

        result: list[int] = (
            await pipe([1, 2, 3]).map(slow_inverse, concurrency=3).collect()
        )

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_flat_map_emits_completion_order(self) -> None:
        async def expand(x: int) -> list[int]:
            await asyncio.sleep((3 - x) * 0.01)
            return [x]

        result: list[int] = (
            await pipe([1, 2, 3]).flat_map(expand, concurrency=3).collect()
        )

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_filter_emits_completion_order(self) -> None:
        async def slow_keep(x: int) -> bool:
            await asyncio.sleep((3 - x) * 0.01)
            return True

        result: list[int] = (
            await pipe([1, 2, 3]).filter(slow_keep, concurrency=3).collect()
        )

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_flat_map_chatty_expansion_does_not_block_siblings(self) -> None:
        async def expand(x: int) -> list[int]:
            if x == 1:
                return list(range(20))
            return [x]

        result = await asyncio.wait_for(
            pipe([1, 2, 3, 4]).flat_map(expand, concurrency=4).collect(), timeout=1
        )

        assert sorted(result) == [0, 1, 2, 2, 3, 3, 4, *range(4, 20)]

    @pytest.mark.asyncio
    async def test_flat_map_supports_concurrent_async_generator_expansions(
        self,
    ) -> None:
        async def expand(x: int) -> AsyncIterator[int]:
            await asyncio.sleep((3 - x) * 0.01)
            yield x
            yield x * 10

        result = await pipe([1, 2, 3]).flat_map(expand, concurrency=3).collect()

        assert result == [3, 30, 2, 20, 1, 10]

    @pytest.mark.asyncio
    async def test_chained_concurrent_stages_complete(self) -> None:
        async def first(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x + 1

        async def second(x: int) -> int:
            await asyncio.sleep(0)
            return x * 10

        result = (
            await pipe([1, 2, 3])
            .map(first, concurrency=3)
            .map(second, concurrency=2)
            .collect()
        )

        assert sorted(result) == [20, 30, 40]

    @pytest.mark.asyncio
    async def test_map_applies_backpressure_to_a_slow_consumer(self) -> None:
        # A slot returns only once its result is delivered, so a consumer that
        # stops reading stops the stage from starting more work.
        started = 0

        def work(x: int) -> int:
            nonlocal started
            started += 1
            return x

        stream = pipe(range(100)).map(work, concurrency=4).__aiter__()
        await anext(stream)
        await settle()

        assert started == 4
        await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_flat_map_applies_backpressure_to_a_slow_consumer(self) -> None:
        produced = 0

        async def expand(x: int) -> AsyncIterator[int]:
            nonlocal produced
            for i in range(10_000):
                produced += 1
                yield i
                await asyncio.sleep(0)

        stream = pipe([1, 2]).flat_map(expand, concurrency=2, buffer=32).__aiter__()
        try:
            await anext(stream)
            # The consumer holds one value and stops reading. Expansions must
            # pause at the buffer limit, not drain their whole output.
            await asyncio.sleep(0.05)

            assert produced <= 48, f"ran {produced} values ahead of a stalled consumer"
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_flat_map_stays_bounded_across_sustained_slow_consumption(
        self,
    ) -> None:
        produced = 0

        async def expand(x: int) -> AsyncIterator[int]:
            nonlocal produced
            for i in range(10_000):
                produced += 1
                yield i
                await asyncio.sleep(0)

        consumed = 0
        async for _ in pipe(range(10)).flat_map(expand, concurrency=4, buffer=32):
            consumed += 1
            await asyncio.sleep(0)
            if consumed == 20:
                break

        assert produced <= consumed + 48, f"produced {produced} for {consumed} consumed"

    @pytest.mark.asyncio
    async def test_flat_map_buffer_smaller_than_concurrency_still_completes(
        self,
    ) -> None:
        async def expand(x: int) -> AsyncIterator[int]:
            for i in range(20):
                yield i
                await asyncio.sleep(0)

        result = await asyncio.wait_for(
            pipe(range(8)).flat_map(expand, concurrency=8, buffer=1).collect(),
            timeout=5,
        )

        assert sorted(result) == sorted(list(range(20)) * 8)

    @pytest.mark.asyncio
    async def test_flat_map_larger_buffer_allows_more_runahead(self) -> None:
        async def counting_expansion(counter: list[int]) -> Any:
            async def expand(x: int) -> AsyncIterator[int]:
                for i in range(10_000):
                    counter[0] += 1
                    yield i
                    await asyncio.sleep(0)

            return expand

        async def runahead(buffer: int) -> int:
            counter = [0]
            expand = await counting_expansion(counter)
            stream = (
                pipe([1]).flat_map(expand, concurrency=1, buffer=buffer).__aiter__()
            )
            try:
                await anext(stream)
                await asyncio.sleep(0.02)
                return counter[0]
            finally:
                await cast(Any, stream).aclose()

        assert await runahead(4) < await runahead(512)

    @pytest.mark.asyncio
    async def test_invalid_buffer_raises(self) -> None:
        with pytest.raises(ValueError, match="buffer"):
            pipe([1]).flat_map(lambda x: [x], buffer=0)

    @pytest.mark.asyncio
    async def test_flat_map_abandoned_mid_expansion_does_not_hang(self) -> None:
        async def expand(x: int) -> AsyncIterator[int]:
            for i in range(10_000):
                yield i

        # Expansions are waiting for capacity when the consumer leaves.
        # Cancelling them must not depend on the consumer freeing that capacity.
        async def run() -> None:
            async for _ in pipe(range(20)).flat_map(expand, concurrency=4):
                break

        await asyncio.wait_for(run(), timeout=1)

    @pytest.mark.asyncio
    async def test_flat_map_emits_every_value_of_a_large_expansion(self) -> None:
        async def expand(x: int) -> AsyncIterator[int]:
            for i in range(500):
                yield i
                await asyncio.sleep(0)

        result = await asyncio.wait_for(
            pipe(range(6)).flat_map(expand, concurrency=3).collect(), timeout=5
        )

        assert len(result) == 3000
        assert sorted(result) == sorted(list(range(500)) * 6)

    @pytest.mark.asyncio
    async def test_map_emits_while_the_source_is_blocked(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            await asyncio.Event().wait()

        stream = pipe(source()).map(double, concurrency=2).__aiter__()
        try:
            # A ready result must not wait for the source's next item.
            assert await asyncio.wait_for(anext(stream), timeout=0.5) == 2
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_flat_map_emits_while_the_source_is_blocked(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            await asyncio.Event().wait()

        stream = pipe(source()).flat_map(lambda x: [x * 10], concurrency=2).__aiter__()
        try:
            assert await asyncio.wait_for(anext(stream), timeout=0.5) == 10
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_filter_emits_while_the_source_is_blocked(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            await asyncio.Event().wait()

        stream = pipe(source()).filter(lambda x: True, concurrency=2).__aiter__()
        try:
            assert await asyncio.wait_for(anext(stream), timeout=0.5) == 1
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_map_emits_every_ready_result_while_the_source_is_blocked(
        self,
    ) -> None:
        async def source() -> AsyncIterator[int]:
            for value in (1, 2, 3):
                yield value
            await asyncio.Event().wait()

        stream = pipe(source()).map(double, concurrency=3).__aiter__()
        collected: list[int] = []
        try:
            for _ in range(3):
                collected.append(await asyncio.wait_for(anext(stream), timeout=0.5))
        finally:
            await cast(Any, stream).aclose()

        assert sorted(collected) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_chained_concurrent_stages_do_not_stall_each_other(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            await asyncio.Event().wait()

        stream = (
            pipe(source())
            .map(double, concurrency=2)
            .map(add_one, concurrency=2)
            .__aiter__()
        )
        try:
            assert await asyncio.wait_for(anext(stream), timeout=0.5) == 3
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_concurrent_map_worker_cancelled_error_propagates(self) -> None:
        # The driver counts outstanding work instead of awaiting each task,
        # so a failing worker must publish its failure, or the driver would hang.
        async def cancel(_: int) -> int:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                pipe([1, 2, 3]).map(cancel, concurrency=2).collect(), timeout=1
            )

    @pytest.mark.asyncio
    async def test_concurrent_map_worker_base_exception_propagates(self) -> None:
        class StopNow(BaseException):
            pass

        async def stop(_: int) -> int:
            raise StopNow

        with pytest.raises(StopNow):
            await asyncio.wait_for(
                pipe([1, 2, 3]).map(stop, concurrency=2).collect(), timeout=1
            )

    @pytest.mark.asyncio
    async def test_invalid_concurrency_raises(self) -> None:
        with pytest.raises(ValueError, match="concurrency"):
            pipe([1]).map(double, concurrency=0)


class TestOrderedMap:
    """`ordered=True` must produce exactly what `concurrency=1` would.

    Values, order, and ordinary failures match; only timing differs.
    `TestOrderedCancellation` covers cancellation, which never waits its turn.
    """

    @pytest.mark.asyncio
    async def test_map_emits_input_order(self) -> None:
        async def slow_inverse(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x

        result: list[int] = (
            await pipe([1, 2, 3])
            .map(slow_inverse, concurrency=3, ordered=True)
            .collect()
        )

        assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_ordered_matches_the_sequential_run(self) -> None:
        # Delays chosen so completion order is neither input order nor its reverse.
        delays = [0.03, 0.0, 0.02, 0.01, 0.0, 0.04, 0.01]

        async def work(x: int) -> int:
            await asyncio.sleep(delays[x])
            return x * 10

        sequential = await pipe(range(len(delays))).map(work, concurrency=1).collect()
        ordered = (
            await pipe(range(len(delays)))
            .map(work, concurrency=4, ordered=True)
            .collect()
        )
        unordered = await pipe(range(len(delays))).map(work, concurrency=4).collect()

        assert ordered == sequential
        assert unordered != sequential, "the delays no longer reorder anything"

    @pytest.mark.asyncio
    async def test_ordered_at_concurrency_one_stays_sequential(self) -> None:
        # `ordered` must not bypass the sequential fast path. Here `fn` runs in
        # the consumer's task; the concurrent driver would use a worker task.
        seen: list[asyncio.Task[Any] | None] = []

        async def record(x: int) -> int:
            seen.append(asyncio.current_task())
            return x

        async def run() -> list[int]:
            return (
                await pipe([1, 2, 3]).map(record, concurrency=1, ordered=True).collect()
            )

        task = asyncio.create_task(run())

        assert await task == [1, 2, 3]
        assert seen == [task, task, task]

    @pytest.mark.asyncio
    async def test_failure_surfaces_at_its_input_position(self) -> None:
        async def work(x: int) -> int:
            if x == 1:
                await asyncio.sleep(0.02)
                raise RuntimeError("boom")
            return x

        got: list[int] = []
        with pytest.raises(RuntimeError, match="boom"):
            async for value in pipe([0, 1, 2]).map(work, concurrency=3, ordered=True):
                got.append(value)

        # Item 2 finished first, but the failure at position 1 ends the stream.
        assert got == [0]

    @pytest.mark.asyncio
    async def test_source_failure_surfaces_after_every_started_item(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 0
            yield 1
            raise RuntimeError("source boom")

        async def work(x: int) -> int:
            await asyncio.sleep((2 - x) * 0.02)
            return x

        got: list[int] = []
        with pytest.raises(RuntimeError, match="source boom"):
            async for value in pipe(source()).map(work, concurrency=4, ordered=True):
                got.append(value)

        # The source failed while producing item 2, so the failure belongs after
        # items 0 and 1, even though it was known before either finished.
        assert got == [0, 1]

    @pytest.mark.asyncio
    async def test_an_item_failure_outranks_a_held_source_failure(self) -> None:
        # A sequential run would stop at item 1 without reaching the source error.
        # Ordered mode must likewise report the item failure, not the held error.
        async def source() -> AsyncIterator[int]:
            yield 0
            yield 1
            raise RuntimeError("source boom")

        async def work(x: int) -> int:
            if x == 1:
                raise ValueError("item boom")
            await asyncio.sleep(0.02)
            return x

        got: list[int] = []
        with pytest.raises(ValueError, match="item boom"):
            async for value in pipe(source()).map(work, concurrency=4, ordered=True):
                got.append(value)

        assert got == [0]

    @pytest.mark.asyncio
    async def test_holds_at_most_concurrency_results(self) -> None:
        # Slots return on delivery, so out-of-turn results can't let the feeder
        # run ahead. Ordered mode needs no separate buffer limit.
        release = asyncio.Event()
        all_slots_used = asyncio.Event()
        started = 0

        async def work(x: int) -> int:
            nonlocal started
            started += 1
            if started == 4:
                all_slots_used.set()
            if x == 0:
                await release.wait()
            return x

        stream = pipe(range(100)).map(work, concurrency=4, ordered=True).__aiter__()

        async def take_first() -> int:
            return await anext(stream)

        first = asyncio.create_task(take_first())
        await asyncio.wait_for(all_slots_used.wait(), timeout=5)
        await settle()

        assert started == 4, f"{started} items started against 4 slots"

        release.set()
        assert await first == 0
        await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_ordered_threads_through_op_and_flow(self) -> None:
        async def slow_inverse(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x

        via_op = await (
            pipe([1, 2, 3]) | op.map(slow_inverse, concurrency=3, ordered=True)
        ).collect()
        via_flow = await (
            pipe([1, 2, 3]) | Flow[int]().map(slow_inverse, concurrency=3, ordered=True)
        ).collect()

        assert via_op == [1, 2, 3]
        assert via_flow == [1, 2, 3]


class TestOrderedFlatMap:
    """Ordered `flat_map` groups output by input item, each group in its own order."""

    @pytest.mark.asyncio
    async def test_output_is_grouped_by_input_item(self) -> None:
        async def expand(x: int) -> list[int]:
            await asyncio.sleep((3 - x) * 0.01)
            return [x, x * 10]

        result = (
            await pipe([1, 2, 3])
            .flat_map(expand, concurrency=3, ordered=True)
            .collect()
        )

        assert result == [1, 10, 2, 20, 3, 30]

    @pytest.mark.asyncio
    async def test_ordered_matches_the_sequential_run(self) -> None:
        delays = [0.03, 0.0, 0.02, 0.01, 0.0]

        async def expand(x: int) -> list[int]:
            await asyncio.sleep(delays[x])
            return [x, -x]

        sequential = (
            await pipe(range(len(delays))).flat_map(expand, concurrency=1).collect()
        )
        ordered = (
            await pipe(range(len(delays)))
            .flat_map(expand, concurrency=4, ordered=True)
            .collect()
        )
        unordered = (
            await pipe(range(len(delays))).flat_map(expand, concurrency=4).collect()
        )

        assert ordered == sequential
        assert unordered != sequential, "the delays no longer reorder anything"

    @pytest.mark.asyncio
    async def test_out_of_turn_expansion_cannot_stall_the_stage(self) -> None:
        # With a shared allowance, expansion 1 would fill it before expansion 0
        # could publish all its values. Neither could progress: expansion 0 needs
        # room, and expansion 1's values must wait for expansion 0.
        async def expand(x: int) -> AsyncIterator[int]:
            for i in range(12):
                if x == 0:
                    await asyncio.sleep(0)
                yield x * 100 + i

        stream = pipe([0, 1]).flat_map(expand, concurrency=2, buffer=4, ordered=True)
        result = await asyncio.wait_for(stream.collect(), timeout=5)

        assert result == [*range(12), *(100 + i for i in range(12))]

    @pytest.mark.asyncio
    async def test_holds_at_most_concurrency_expansions(self) -> None:
        # Finished expansions still hold undelivered values. Returning their
        # slots before delivery would let the feeder start more work and exceed
        # the `concurrency * (buffer + 1)` bound.
        release = asyncio.Event()
        all_slots_used = asyncio.Event()
        started = 0

        async def expand(x: int) -> list[int]:
            nonlocal started
            started += 1
            if started == 2:
                all_slots_used.set()
            if x == 0:
                await release.wait()
            return [x]

        stream = (
            pipe(range(100)).flat_map(expand, concurrency=2, ordered=True).__aiter__()
        )

        async def take_first() -> int:
            return await anext(stream)

        first = asyncio.create_task(take_first())
        await asyncio.wait_for(all_slots_used.wait(), timeout=5)
        await settle()

        assert started == 2, f"{started} expansions started against 2 slots"

        release.set()
        assert await first == 0
        await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_expansion_failure_surfaces_at_its_input_position(self) -> None:
        async def expand(x: int) -> list[int]:
            if x == 1:
                await asyncio.sleep(0.02)
                raise RuntimeError("boom")
            return [x * 10]

        got: list[int] = []
        with pytest.raises(RuntimeError, match="boom"):
            async for value in pipe([0, 1, 2]).flat_map(
                expand, concurrency=3, ordered=True
            ):
                got.append(value)

        assert got == [0]

    @pytest.mark.asyncio
    async def test_values_before_a_failure_in_the_same_expansion_arrive_first(
        self,
    ) -> None:
        async def expand(x: int) -> AsyncIterator[int]:
            yield x
            if x == 1:
                raise RuntimeError("boom")

        got: list[int] = []
        with pytest.raises(RuntimeError, match="boom"):
            async for value in pipe([0, 1, 2]).flat_map(
                expand, concurrency=3, ordered=True
            ):
                got.append(value)

        assert got == [0, 1]

    @pytest.mark.asyncio
    async def test_source_failure_surfaces_after_every_started_expansion(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            yield 2
            raise RuntimeError("source boom")

        async def expand(x: int) -> list[int]:
            await asyncio.sleep((2 - x) * 0.02)
            return [x, x * 10]

        got: list[int] = []
        with pytest.raises(RuntimeError, match="source boom"):
            async for value in pipe(source()).flat_map(
                expand, concurrency=4, ordered=True
            ):
                got.append(value)

        # The source error belongs after both started expansions,
        # even though it happened before either finished.
        assert got == [1, 10, 2, 20]

    @pytest.mark.asyncio
    async def test_an_expansion_failure_outranks_a_held_source_failure(self) -> None:
        # As with map, a sequential run would hit the expansion error first,
        # without reaching the source error.
        async def source() -> AsyncIterator[int]:
            yield 0
            yield 1
            raise RuntimeError("source boom")

        async def expand(x: int) -> list[int]:
            if x == 1:
                raise ValueError("expansion boom")
            await asyncio.sleep(0.02)
            return [x]

        got: list[int] = []
        with pytest.raises(ValueError, match="expansion boom"):
            async for value in pipe(source()).flat_map(
                expand, concurrency=4, ordered=True
            ):
                got.append(value)

        assert got == [0]

    @pytest.mark.asyncio
    async def test_filter_keeps_input_order(self) -> None:
        async def slow_keep(x: int) -> bool:
            await asyncio.sleep((3 - x) * 0.01)
            return x != 2

        result = (
            await pipe([1, 2, 3])
            .filter(slow_keep, concurrency=3, ordered=True)
            .collect()
        )

        assert result == [1, 3]

    @pytest.mark.asyncio
    async def test_ordered_threads_through_op_and_flow(self) -> None:
        async def expand(x: int) -> list[int]:
            await asyncio.sleep((3 - x) * 0.01)
            return [x]

        via_op = await (
            pipe([1, 2, 3]) | op.flat_map(expand, concurrency=3, ordered=True)
        ).collect()
        via_flow = await (
            pipe([1, 2, 3]) | Flow[int]().flat_map(expand, concurrency=3, ordered=True)
        ).collect()
        filtered = await (
            pipe([1, 2, 3]) | op.filter(lambda x: True, concurrency=3, ordered=True)
        ).collect()

        assert via_op == [1, 2, 3]
        assert via_flow == [1, 2, 3]
        assert filtered == [1, 2, 3]


class TestOrderedCancellation:
    """Cancellation isn't a result to put in order, so it never waits for a turn.

    Each case blocks item 0 on an event that's never set. If cancellation waits
    behind it, the stage hangs. `wait_for` then raises `TimeoutError` instead of
    the expected `CancelledError`.
    """

    @pytest.mark.asyncio
    async def test_a_worker_cancellation_does_not_wait_for_its_position(self) -> None:
        hang = asyncio.Event()

        async def work(x: int) -> int:
            if x == 0:
                await hang.wait()
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                pipe([0, 1]).map(work, concurrency=2, ordered=True).collect(), timeout=1
            )

    @pytest.mark.asyncio
    async def test_an_expansion_cancellation_does_not_wait_for_its_position(
        self,
    ) -> None:
        hang = asyncio.Event()

        async def expand(x: int) -> AsyncIterator[int]:
            if x == 0:
                # The unreachable yield makes this an async generator,
                # not a coroutine.
                await hang.wait()
                yield 0
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                pipe([0, 1]).flat_map(expand, concurrency=2, ordered=True).collect(),
                timeout=1,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stage", ["map", "flat_map"])
    async def test_a_source_cancellation_does_not_wait_for_started_work(
        self, stage: str
    ) -> None:
        hang = asyncio.Event()

        async def source() -> AsyncIterator[int]:
            yield 0
            raise asyncio.CancelledError

        async def slow_map(x: int) -> int:
            await hang.wait()
            return x

        async def slow_expand(x: int) -> list[int]:
            await hang.wait()
            return [x]

        builders = {
            "map": lambda: pipe(source()).map(slow_map, concurrency=2, ordered=True),
            "flat_map": lambda: pipe(source()).flat_map(
                slow_expand, concurrency=2, ordered=True
            ),
        }

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(builders[stage]().collect(), timeout=1)


class TestInThread:
    @pytest.mark.asyncio
    async def test_in_thread_supports_blocking_map(self) -> None:
        result = (
            await pipe([1, 2, 3])
            .map(in_thread(lambda x: x * 2), concurrency=3)
            .collect()
        )

        assert sorted(result) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_in_thread_supports_filter_and_flat_map(self) -> None:
        filtered = (
            await pipe([1, 2, 3, 4])
            .filter(in_thread(lambda x: x % 2 == 0), concurrency=1)
            .collect()
        )
        expanded = (
            await pipe([1, 2])
            .flat_map(in_thread(lambda x: [x, x * 10]), concurrency=1)
            .collect()
        )

        assert filtered == [2, 4]
        assert expanded == [1, 10, 2, 20]

    @pytest.mark.asyncio
    async def test_in_thread_supports_shared_executor(self) -> None:
        thread_names: set[str] = set()
        lock = threading.Lock()

        def record_thread(x: int) -> int:
            with lock:
                thread_names.add(threading.current_thread().name)
            time.sleep(0.01)
            return x

        with ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="flowlet-test"
        ) as pool:
            result = (
                await pipe([1, 2, 3])
                .map(in_thread(record_thread, executor=pool), concurrency=3)
                .collect()
            )

        assert sorted(result) == [1, 2, 3]
        assert thread_names
        assert all(name.startswith("flowlet-test") for name in thread_names)

    @pytest.mark.asyncio
    async def test_in_thread_propagates_contextvars(self) -> None:
        request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id")
        request_id.set("req-1")

        result = (
            await pipe([1, 2])
            .map(in_thread(lambda _: request_id.get()), concurrency=2)
            .collect()
        )

        assert result == ["req-1", "req-1"]

    def test_in_thread_preserves_function_metadata(self) -> None:
        def named(x: int) -> int:
            """docstring"""

            return x

        wrapped = in_thread(named)

        assert wrapped.__name__ == "named"
        assert wrapped.__doc__ == "docstring"


class TestInProcess:
    @pytest.mark.asyncio
    async def test_in_process_supports_blocking_map(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=2, mp_context=ctx) as pool:
            result = (
                await pipe([1, 2, 3])
                .map(in_process(process_double, executor=pool), concurrency=3)
                .collect()
            )

        assert sorted(result) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_in_process_supports_filter_and_flat_map(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=2, mp_context=ctx) as pool:
            filtered = (
                await pipe([1, 2, 3, 4])
                .filter(in_process(process_is_even, executor=pool), concurrency=2)
                .collect()
            )
            expanded = (
                await pipe([1, 2])
                .flat_map(in_process(process_expand, executor=pool), concurrency=2)
                .collect()
            )

        assert sorted(filtered) == [2, 4]
        assert sorted(expanded) == [1, 2, 10, 20]

    def test_in_process_preserves_function_metadata(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
            wrapped = in_process(to_str, executor=pool)

        assert wrapped.__name__ == "to_str"


async def async_add(x: int, y: int = 0) -> int:
    return x + y


async def async_gen(x: int) -> AsyncIterator[int]:
    yield x


def in_process_pool(fn: Any) -> Any:
    with ProcessPoolExecutor(max_workers=1) as pool:
        return in_process(fn, executor=pool)


@pytest.mark.parametrize("wrap", [in_thread, in_process_pool])
@pytest.mark.parametrize(
    "fn",
    [
        pytest.param(async_add, id="coroutine"),
        pytest.param(functools.partial(async_add, 1), id="partial"),
        pytest.param(async_gen, id="async_generator"),
    ],
)
def test_executor_wrappers_reject_async_callables(wrap: Any, fn: Any) -> None:
    with pytest.raises(TypeError, match="async callables"):
        wrap(fn)


class TestSourcesAndTerminals:
    @pytest.mark.asyncio
    async def test_async_iterable_source(self) -> None:
        async def source() -> AsyncIterator[int]:
            for item in [1, 2, 3]:
                yield item

        result: list[int] = await pipe(source()).map(double).collect()

        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_async_iteration(self) -> None:
        output: list[int] = []

        item: int
        async for item in pipe([1, 2, 3]).map(double):
            output.append(item)

        assert output == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_break_from_concurrent_pipeline_cancels_pending_work(self) -> None:
        cancelled = asyncio.Event()

        async def work(x: int) -> int:
            if x == 1:
                return x

            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            return x

        async for _ in pipe([1, 2, 3]).map(work, concurrency=3):
            break

        await asyncio.wait_for(cancelled.wait(), timeout=0.1)

    @pytest.mark.asyncio
    async def test_close_cancels_in_flight_work_before_returning(self) -> None:
        running: set[int] = set()

        async def work(x: int) -> int:
            if x == 0:
                return x
            running.add(x)
            try:
                await asyncio.Event().wait()
            finally:
                running.discard(x)
            return x

        stream = pipe(range(20)).map(work, concurrency=4).__aiter__()
        assert await anext(stream) == 0
        for _ in range(200):
            await asyncio.sleep(0)
            if len(running) >= 3:
                break
        assert running

        await cast(Any, stream).aclose()

        assert not running, "aclose returned while workers were still running"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrency", [1, 2, 4])
    async def test_source_cleanup_failure_is_reported_by_close(
        self, concurrency: int
    ) -> None:
        async def source() -> AsyncIterator[int]:
            try:
                yield 1
                await asyncio.Event().wait()
            finally:
                raise ValueError("cleanup boom")

        # With concurrency > 1 the feeder is parked in anext() when cancelled.
        # The driver has stopped reading the queue by then, so the feeder must
        # re-raise the failure from its task for teardown to see it.
        stream = pipe(source()).map(double, concurrency=concurrency).__aiter__()
        await anext(stream)
        await settle()

        with pytest.raises(ValueError, match="cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(
                lambda work: pipe(range(3)).map(work, concurrency=2), id="map"
            ),
            pytest.param(
                lambda work: pipe(range(3)).flat_map(work, concurrency=2), id="flat_map"
            ),
        ],
    )
    async def test_task_cleanup_failure_during_teardown_is_reported(
        self, build: Any
    ) -> None:
        async def work(x: int) -> list[int]:
            if x > 0:
                try:
                    await asyncio.Event().wait()
                finally:
                    raise ValueError("cleanup boom")
            return [x]

        stream = build(work).__aiter__()
        await anext(stream)
        await settle()

        # During teardown the driver has stopped reading the queue, so a worker
        # or expansion must re-raise its failure, not just publish it.
        with pytest.raises(ValueError, match="cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(
                lambda source, work: pipe(source).map(work, concurrency=4), id="map"
            ),
            pytest.param(
                lambda source, work: pipe(source).flat_map(work, concurrency=4),
                id="flat_map",
            ),
        ],
    )
    async def test_source_cleanup_failure_outranks_worker_cleanup_failures(
        self, build: Any
    ) -> None:
        async def source() -> AsyncIterator[int]:
            yield 0
            yield 1
            yield 2
            try:
                await asyncio.Event().wait()
            finally:
                raise ValueError("source cleanup boom")

        async def work(x: int) -> list[int]:
            if x > 0:
                try:
                    await asyncio.Event().wait()
                finally:
                    raise RuntimeError("worker cleanup boom")
            return [x]

        stream = build(source(), work).__aiter__()
        await anext(stream)
        await settle()

        # The feeder comes first in teardown's task list, so its cleanup failure
        # takes precedence over the workers' failures.
        with pytest.raises(ValueError, match="source cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            # Only concurrent drivers can exhaust the source before the consumer
            # leaves. test_source_cleanup_failure_is_reported_by_close covers
            # sequential paths.
            pytest.param(lambda s: pipe(s).map(double, concurrency=2), id="map_c2"),
            pytest.param(
                lambda s: pipe(s).flat_map(lambda x: [x], concurrency=2),
                id="flat_map_c2",
            ),
        ],
    )
    async def test_cleanup_failure_on_natural_exhaustion_is_reported(
        self, build: Any
    ) -> None:
        cleanup_ran = asyncio.Event()

        class ExhaustOnSecondRead:
            def __init__(self) -> None:
                self.emitted = 0

            def __aiter__(self) -> ExhaustOnSecondRead:
                return self

            async def __anext__(self) -> int:
                self.emitted += 1
                if self.emitted == 1:
                    return 1
                raise StopAsyncIteration

            async def aclose(self) -> None:
                cleanup_ran.set()
                raise ValueError("cleanup boom")

        stream = build(ExhaustOnSecondRead()).__aiter__()
        # The value still arrives: the feeder keeps its source's close failure
        # for teardown instead of ending the input with it.
        await anext(stream)
        # The driver is paused at yield. Wait for cleanup without relying on a
        # timed sleep.
        await asyncio.wait_for(cleanup_ran.wait(), timeout=1)

        # Cleanup failed before cancellation, so the failure must be retained
        # for teardown, not just published to the now-unread queue.
        with pytest.raises(ValueError, match="cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    async def test_drain_drains_pipeline(self) -> None:
        output: list[int] = []

        await pipe([1, 2, 3]).map(lambda x: output.append(x)).drain()

        assert output == [1, 2, 3]


class TestErrors:
    @pytest.mark.asyncio
    async def test_source_exception_propagates(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            raise RuntimeError("source boom")

        with pytest.raises(RuntimeError, match="source boom"):
            await pipe(source()).map(double).collect()

    @pytest.mark.asyncio
    async def test_worker_exception_propagates(self) -> None:
        async def boom(x: int) -> int:
            if x == 2:
                raise RuntimeError("boom")
            return x

        with pytest.raises(RuntimeError, match="boom"):
            await pipe([1, 2, 3]).map(boom, concurrency=2).collect()

    @pytest.mark.asyncio
    async def test_flat_map_exception_propagates(self) -> None:
        async def boom(x: int) -> list[int]:
            if x == 2:
                raise RuntimeError("boom")
            return [x]

        with pytest.raises(RuntimeError, match="boom"):
            await pipe([1, 2, 3]).flat_map(boom, concurrency=2).collect()

    @pytest.mark.asyncio
    async def test_flat_map_base_exception_propagates(self) -> None:
        class StopNow(BaseException):
            pass

        async def stop(_: int) -> list[int]:
            raise StopNow

        with pytest.raises(StopNow):
            await pipe([1]).flat_map(stop).collect()

    @pytest.mark.asyncio
    async def test_flat_map_base_exception_during_async_iteration_propagates(
        self,
    ) -> None:
        class StopNow(BaseException):
            pass

        async def stop(_: int) -> AsyncIterator[int]:
            yield 1
            raise StopNow

        with pytest.raises(StopNow):
            await pipe([1]).flat_map(stop).collect()

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self) -> None:
        async def cancelled(_: int) -> list[int]:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await pipe([1]).flat_map(cancelled).collect()


class TestBatch:
    @pytest.mark.asyncio
    async def test_batch_exact_division(self) -> None:
        result = await pipe([1, 2, 3, 4, 5, 6]).batch(3).collect()

        assert result == [[1, 2, 3], [4, 5, 6]]

    @pytest.mark.asyncio
    async def test_batch_partial_last_group(self) -> None:
        result = await pipe([1, 2, 3, 4, 5]).batch(3).collect()

        assert result == [[1, 2, 3], [4, 5]]

    @pytest.mark.asyncio
    async def test_batch_single_item(self) -> None:
        result = await pipe([1]).batch(5).collect()

        assert result == [[1]]

    @pytest.mark.asyncio
    async def test_batch_empty_source(self) -> None:
        source: list[int] = []
        result = await pipe(source).batch(3).collect()

        assert result == []

    @pytest.mark.asyncio
    async def test_batch_size_one(self) -> None:
        result = await pipe([1, 2, 3]).batch(1).collect()

        assert result == [[1], [2], [3]]

    def test_batch_invalid_size_raises(self) -> None:
        with pytest.raises(ValueError, match="size"):
            pipe([1]).batch(0)

    @pytest.mark.asyncio
    async def test_batch_with_async_source(self) -> None:
        async def source() -> AsyncIterator[int]:
            for i in range(5):
                yield i

        result = await pipe(source()).batch(2).collect()

        assert result == [[0, 1], [2, 3], [4]]

    @pytest.mark.asyncio
    async def test_batch_feeds_downstream_map(self) -> None:
        result = await pipe([1, 2, 3, 4]).batch(2).map(sum).collect()

        assert result == [3, 7]

    @pytest.mark.asyncio
    async def test_batch_after_concurrent_stage(self) -> None:
        result: list[list[int]] = (
            await pipe(range(6)).map(double, concurrency=3).batch(2).collect()
        )

        all_values = sorted(v for batch in result for v in batch)
        assert all_values == [0, 2, 4, 6, 8, 10]
        assert all(len(b) == 2 for b in result)

    @pytest.mark.asyncio
    async def test_batch_with_op(self) -> None:
        result = await (pipe([1, 2, 3, 4]) | op.batch(3)).collect()

        assert result == [[1, 2, 3], [4]]

    @pytest.mark.asyncio
    async def test_batch_with_flow(self) -> None:
        chunk_and_sum: Flow[int, int] = Flow[int]().batch(2).map(sum)

        result = await pipe([1, 2, 3, 4, 5]).through(chunk_and_sum).collect()

        assert result == [3, 7, 5]

    @pytest.mark.asyncio
    async def test_batch_with_functional_api(self) -> None:
        transform: F.Operator[int, int] = F.chain(F.batch(3), F.map(len))

        result = await F.collect(transform([1, 2, 3, 4, 5]))

        assert result == [3, 2]

    @pytest.mark.asyncio
    async def test_batch_preserves_none_values(self) -> None:
        result = await pipe([None, 1, None, 2]).batch(2).collect()

        assert result == [[None, 1], [None, 2]]


class TestFunctionalApi:
    @pytest.mark.asyncio
    async def test_functional_api(self) -> None:
        transform = F.chain(F.map(double), F.map(add_one), F.map(to_str))

        result: list[str] = await F.collect(transform([1, 2, 3]))

        assert result == ["3", "5", "7"]

    @pytest.mark.asyncio
    async def test_functional_chain_allows_identity(self) -> None:
        identity: F.Operator[int, int] = F.chain()

        assert await F.collect(identity([1, 2, 3])) == [1, 2, 3]


def test_typing_surface() -> None:
    numbers: Pipeline[int] = pipe([1, 2, 3])
    text: Pipeline[str] = numbers.map(to_str)
    threaded = in_thread(to_str)
    sourceless_flow = Flow[int]().map(double).map(to_str)
    transform: Flow[int, str] = op.map(double) | op.map(to_str)
    filtered: Flow[int, int] = op.filter(lambda x: x > 1)
    expanded: Flow[int, int] = op.flat_map(lambda x: [x, x])
    functional_transform: F.Operator[int, str] = F.chain(F.map(double), F.map(to_str))
    batched_pipeline: Pipeline[list[int]] = numbers.batch(2)
    batched_flow: Flow[int, list[int]] = Flow[int]().batch(2)
    batched_op: Flow[int, list[int]] = op.batch(2)
    batched_functional: F.Operator[int, list[int]] = F.batch(2)

    assert isinstance(text, Pipeline)
    assert callable(threaded)
    assert isinstance(sourceless_flow, Flow)
    assert isinstance(transform, Flow)
    assert isinstance(filtered, Flow)
    assert isinstance(expanded, Flow)
    assert functional_transform is not None
    assert isinstance(batched_pipeline, Pipeline)
    assert isinstance(batched_flow, Flow)
    assert isinstance(batched_op, Flow)
    assert batched_functional is not None


class TestThreadLocal:
    def test_lazy_reuses_and_explicit_close(self) -> None:
        created: list[int] = []
        closed: list[int] = []

        @thread_local
        def resource() -> Generator[dict[str, int]]:
            value = len(created)
            created.append(value)
            try:
                yield {"value": value}
            finally:
                closed.append(value)

        assert created == []
        first = resource()
        assert first is resource()
        assert created == [0]

        resource.close()
        assert closed == [0]
        second = resource()
        assert second is not first
        assert second == {"value": 1}
        assert created == [0, 1]
        resource.close()
        resource.close()
        assert closed == [0, 1]

    def test_plain_factory_reuses_without_cleanup(self) -> None:
        calls = 0

        @thread_local
        def resource() -> object:
            nonlocal calls
            calls += 1
            return object()

        first = resource()
        assert first is resource()
        assert calls == 1
        resource.close()
        assert resource() is not first
        assert calls == 2

    def test_close_before_init_is_noop(self) -> None:
        @thread_local
        def resource() -> object:
            return object()

        resource.close()

    def test_threads_get_independent_resources_and_thread_death_cleans_up(self) -> None:
        barrier = threading.Barrier(3)
        seen: list[tuple[int, int]] = []
        closed: list[int] = []
        lock = threading.Lock()

        @thread_local
        def resource() -> Generator[object]:
            value = threading.get_ident()
            try:
                yield object()
            finally:
                with lock:
                    closed.append(value)

        def worker() -> None:
            first = resource()
            second = resource()
            with lock:
                seen.append((id(first), id(second)))
            barrier.wait()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        assert len(seen) == 2
        assert seen[0][0] == seen[0][1]
        assert seen[1][0] == seen[1][1]
        assert seen[0][0] != seen[1][0]
        assert len(closed) == 2

    def test_cache_cleared_before_cleanup_and_after_cleanup_error(self) -> None:
        values: list[int] = []
        fail_cleanup = True

        @thread_local
        def resource() -> Generator[int]:
            nonlocal fail_cleanup
            value = len(values)
            values.append(value)
            try:
                yield value
            finally:
                if fail_cleanup:
                    fail_cleanup = False
                    raise RuntimeError("boom")

        assert resource() == 0
        with pytest.raises(RuntimeError, match="boom"):
            resource.close()
        assert resource() == 1
        resource.close()

    def test_error_cases(self) -> None:
        @thread_local
        def empty() -> Generator[object]:
            yield from ()

        with pytest.raises(RuntimeError, match="empty"):
            empty()

        async def async_gen() -> AsyncIterator[object]:
            yield object()

        async def coroutine_factory() -> object:
            return object()

        with pytest.raises(TypeError, match="synchronous"):
            thread_local(async_gen)
        with pytest.raises(TypeError, match="synchronous"):
            thread_local(coroutine_factory)

    def test_multiple_locals_do_not_interfere(self) -> None:
        @thread_local
        def a() -> list[int]:
            return []

        @thread_local
        def b() -> list[int]:
            return []

        a().append(1)
        b().append(2)
        assert a() == [1]
        assert b() == [2]

    @pytest.mark.asyncio
    async def test_in_thread_reuses_one_resource_per_worker_and_scoped_cleanup(
        self,
    ) -> None:
        created: set[int] = set()
        closed: set[int] = set()
        lock = threading.Lock()

        @thread_local
        def resource() -> Generator[int]:
            thread_id = threading.get_ident()
            with lock:
                created.add(thread_id)
            try:
                yield thread_id
            finally:
                with lock:
                    closed.add(thread_id)

        def use_resource(_: int) -> int:
            return resource()

        with ThreadPoolExecutor(max_workers=4) as pool:
            result = (
                await pipe(range(40))
                .map(in_thread(use_resource, executor=pool), concurrency=40)
                .collect()
            )
            assert set(result) == created
            assert len(created) <= 4
            assert closed == set()

        assert closed == created
