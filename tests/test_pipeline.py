import asyncio
import functools
import gc
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


@pytest.fixture
def no_cyclic_gc() -> Generator[None]:
    """Stop a cyclic collection from standing in for explicit cleanup.

    An abandoned generator is kept reachable by the live traceback, so only the
    cyclic collector closes it. If it ran mid-test it would close the generator
    on its own and let an implementation that never closes anything pass.
    """
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


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
        result: list[int] = await pipe([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).collect()

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
        result: list[int] = await asyncio.wait_for(pipe(range(100)).map(double, concurrency=10).collect(), timeout=1)

        assert sorted(result) == [x * 2 for x in range(100)]

    @pytest.mark.asyncio
    async def test_map_emits_completion_order(self) -> None:
        async def slow_inverse(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x

        result: list[int] = await pipe([1, 2, 3]).map(slow_inverse, concurrency=3).collect()

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_flat_map_emits_completion_order(self) -> None:
        async def expand(x: int) -> list[int]:
            await asyncio.sleep((3 - x) * 0.01)
            return [x]

        result: list[int] = await pipe([1, 2, 3]).flat_map(expand, concurrency=3).collect()

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_filter_emits_completion_order(self) -> None:
        async def slow_keep(x: int) -> bool:
            await asyncio.sleep((3 - x) * 0.01)
            return True

        result: list[int] = await pipe([1, 2, 3]).filter(slow_keep, concurrency=3).collect()

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_flat_map_chatty_expansion_does_not_block_siblings(self) -> None:
        async def expand(x: int) -> list[int]:
            if x == 1:
                return list(range(20))
            return [x]

        result = await asyncio.wait_for(pipe([1, 2, 3, 4]).flat_map(expand, concurrency=4).collect(), timeout=1)

        assert sorted(result) == [0, 1, 2, 2, 3, 3, 4, *range(4, 20)]

    @pytest.mark.asyncio
    async def test_flat_map_supports_concurrent_async_generator_expansions(self) -> None:
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

        result = await pipe([1, 2, 3]).map(first, concurrency=3).map(second, concurrency=2).collect()

        assert sorted(result) == [20, 30, 40]

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
            # The consumer stalls holding one value; expansions must park once
            # they fill the buffer rather than draining their whole output.
            await asyncio.sleep(0.05)

            assert produced <= 48, f"ran {produced} values ahead of a stalled consumer"
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_flat_map_stays_bounded_across_sustained_slow_consumption(self) -> None:
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
    async def test_flat_map_buffer_smaller_than_concurrency_still_completes(self) -> None:
        async def expand(x: int) -> AsyncIterator[int]:
            for i in range(20):
                yield i
                await asyncio.sleep(0)

        result = await asyncio.wait_for(pipe(range(8)).flat_map(expand, concurrency=8, buffer=1).collect(), timeout=5)

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
            stream = pipe([1]).flat_map(expand, concurrency=1, buffer=buffer).__aiter__()
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

        # Expansions are parked waiting for capacity when the consumer leaves;
        # cancelling them must not deadlock on the backpressure credit.
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

        result = await asyncio.wait_for(pipe(range(6)).flat_map(expand, concurrency=3).collect(), timeout=5)

        assert len(result) == 3000
        assert sorted(result) == sorted(list(range(500)) * 6)

    @pytest.mark.asyncio
    async def test_map_emits_while_the_source_is_blocked(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            await asyncio.Event().wait()

        stream = pipe(source()).map(double, concurrency=2).__aiter__()
        try:
            # The result for item 1 is ready; a stage must not sit on it just
            # because the source has not produced a second item yet.
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
    async def test_map_emits_every_ready_result_while_the_source_is_blocked(self) -> None:
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

        stream = pipe(source()).map(double, concurrency=2).map(add_one, concurrency=2).__aiter__()
        try:
            assert await asyncio.wait_for(anext(stream), timeout=0.5) == 3
        finally:
            await cast(Any, stream).aclose()

    @pytest.mark.asyncio
    async def test_concurrent_map_worker_cancelled_error_propagates(self) -> None:
        # The driver counts outstanding work rather than awaiting each task, so a
        # worker that fails must publish its failure or the driver would hang.
        async def cancel(_: int) -> int:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pipe([1, 2, 3]).map(cancel, concurrency=2).collect(), timeout=1)

    @pytest.mark.asyncio
    async def test_concurrent_map_worker_base_exception_propagates(self) -> None:
        class StopNow(BaseException):
            pass

        async def stop(_: int) -> int:
            raise StopNow

        with pytest.raises(StopNow):
            await asyncio.wait_for(pipe([1, 2, 3]).map(stop, concurrency=2).collect(), timeout=1)

    @pytest.mark.asyncio
    async def test_invalid_concurrency_raises(self) -> None:
        with pytest.raises(ValueError, match="concurrency"):
            pipe([1]).map(double, concurrency=0)


class TestInThread:
    @pytest.mark.asyncio
    async def test_in_thread_supports_blocking_map(self) -> None:
        result = await pipe([1, 2, 3]).map(in_thread(lambda x: x * 2), concurrency=3).collect()

        assert sorted(result) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_in_thread_supports_filter_and_flat_map(self) -> None:
        filtered = await pipe([1, 2, 3, 4]).filter(in_thread(lambda x: x % 2 == 0), concurrency=1).collect()
        expanded = await pipe([1, 2]).flat_map(in_thread(lambda x: [x, x * 10]), concurrency=1).collect()

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

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="flowlet-test") as pool:
            result = await pipe([1, 2, 3]).map(in_thread(record_thread, executor=pool), concurrency=3).collect()

        assert sorted(result) == [1, 2, 3]
        assert thread_names
        assert all(name.startswith("flowlet-test") for name in thread_names)

    @pytest.mark.asyncio
    async def test_in_thread_workers_feed_downstream_stage(self) -> None:
        started = asyncio.Event()
        release_workers = threading.Event()
        running = 0
        max_running = 0
        seen_by_downstream: list[int] = []
        lock = threading.Lock()
        loop = asyncio.get_running_loop()

        def block(x: int) -> int:
            nonlocal max_running, running
            with lock:
                running += 1
                max_running = max(max_running, running)
                if running == 3:
                    loop.call_soon_threadsafe(started.set)
            release_workers.wait(timeout=1)
            with lock:
                running -= 1
            return x

        async def downstream(x: int) -> int:
            seen_by_downstream.append(x)
            await asyncio.sleep(0)
            return x * 10

        pipeline = pipe([1, 2, 3]).map(in_thread(block), concurrency=3).map(downstream, concurrency=1)
        collect_task = asyncio.create_task(pipeline.collect())

        await asyncio.wait_for(started.wait(), timeout=0.2)
        assert seen_by_downstream == []

        release_workers.set()

        result = await asyncio.wait_for(collect_task, timeout=1)

        assert sorted(result) == [10, 20, 30]
        assert sorted(seen_by_downstream) == [1, 2, 3]
        assert max_running == 3

    @pytest.mark.asyncio
    async def test_in_thread_scales_to_many_tasks(self) -> None:
        running = 0
        max_running = 0
        seen_by_downstream = 0
        lock = threading.Lock()

        def block(x: int) -> int:
            nonlocal max_running, running
            with lock:
                running += 1
                max_running = max(max_running, running)
            time.sleep(0.005)
            with lock:
                running -= 1
            return x

        async def downstream(x: int) -> int:
            nonlocal seen_by_downstream
            seen_by_downstream += 1
            await asyncio.sleep(0)
            return x

        with ThreadPoolExecutor(max_workers=8) as pool:
            result = (
                await pipe(range(200))
                .map(in_thread(block, executor=pool), concurrency=8)
                .map(downstream, concurrency=4)
                .collect()
            )

        assert sorted(result) == list(range(200))
        assert seen_by_downstream == 200
        assert max_running == 8

    @pytest.mark.asyncio
    async def test_in_thread_shared_executor_across_stages(self) -> None:
        stage_one_running = 0
        stage_two_running = 0
        max_stage_one_running = 0
        max_stage_two_running = 0
        max_total_running = 0
        lock = threading.Lock()

        def stage_one(x: int) -> int:
            nonlocal max_stage_one_running, max_total_running, stage_one_running, stage_two_running
            with lock:
                stage_one_running += 1
                max_stage_one_running = max(max_stage_one_running, stage_one_running)
                max_total_running = max(max_total_running, stage_one_running + stage_two_running)
            time.sleep(0.01)
            with lock:
                stage_one_running -= 1
            return x + 1

        def stage_two(x: int) -> int:
            nonlocal max_stage_two_running, max_total_running, stage_one_running, stage_two_running
            with lock:
                stage_two_running += 1
                max_stage_two_running = max(max_stage_two_running, stage_two_running)
                max_total_running = max(max_total_running, stage_one_running + stage_two_running)
            time.sleep(0.01)
            with lock:
                stage_two_running -= 1
            return x * 10

        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="flowlet-shared") as pool:
            result = (
                await pipe(range(40))
                .map(in_thread(stage_one, executor=pool), concurrency=3)
                .map(in_thread(stage_two, executor=pool), concurrency=2)
                .collect()
            )

        assert sorted(result) == [(x + 1) * 10 for x in range(40)]
        assert max_stage_one_running == 3
        assert max_stage_two_running == 2
        assert 1 < max_total_running <= 4

    def test_in_thread_rejects_async_callables(self) -> None:
        async def async_fn(x: int) -> int:
            return x

        with pytest.raises(TypeError, match="async callables"):
            in_thread(async_fn)

    def test_in_thread_rejects_partial_of_async_callable(self) -> None:
        async def async_fn(x: int, y: int) -> int:
            return x + y

        with pytest.raises(TypeError, match="async callables"):
            in_thread(functools.partial(async_fn, 1))

    def test_in_thread_rejects_async_generator_callables(self) -> None:
        async def async_gen(x: int) -> AsyncIterator[int]:
            yield x

        with pytest.raises(TypeError, match="async callables"):
            in_thread(async_gen)

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
            result = await pipe([1, 2, 3]).map(in_process(process_double, executor=pool), concurrency=3).collect()

        assert sorted(result) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_in_process_supports_filter_and_flat_map(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=2, mp_context=ctx) as pool:
            filtered = (
                await pipe([1, 2, 3, 4]).filter(in_process(process_is_even, executor=pool), concurrency=2).collect()
            )
            expanded = await pipe([1, 2]).flat_map(in_process(process_expand, executor=pool), concurrency=2).collect()

        assert sorted(filtered) == [2, 4]
        assert sorted(expanded) == [1, 2, 10, 20]

    def test_in_process_rejects_async_callables(self) -> None:
        async def async_fn(x: int) -> int:
            return x

        ctx = mp.get_context("fork")
        with (
            ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool,
            pytest.raises(TypeError, match="async callables"),
        ):
            in_process(async_fn, executor=pool)

    def test_in_process_rejects_partial_of_async_callable(self) -> None:
        async def async_fn(x: int, y: int) -> int:
            return x + y

        ctx = mp.get_context("fork")
        with (
            ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool,
            pytest.raises(TypeError, match="async callables"),
        ):
            in_process(functools.partial(async_fn, 1), executor=pool)

    def test_in_process_rejects_async_generator_callables(self) -> None:
        async def async_gen(x: int) -> AsyncIterator[int]:
            yield x

        ctx = mp.get_context("fork")
        with (
            ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool,
            pytest.raises(TypeError, match="async callables"),
        ):
            in_process(async_gen, executor=pool)

    def test_in_process_preserves_function_metadata(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
            wrapped = in_process(to_str, executor=pool)

        assert wrapped.__name__ == "to_str"


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
    @pytest.mark.parametrize("concurrency", [1, 3])
    async def test_close_releases_the_upstream_source(self, concurrency: int, no_cyclic_gc: None) -> None:
        closed = False

        async def source() -> AsyncIterator[int]:
            nonlocal closed
            try:
                for value in range(100):
                    yield value
            finally:
                closed = True

        stream = pipe(source()).map(double, concurrency=concurrency).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        # Read with no sleep and no gc.collect(): cleanup has to be driven by
        # aclose itself, not by garbage collection catching up later.
        assert closed

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda s: pipe(s).flat_map(lambda x: [x], concurrency=3), id="flat_map"),
            pytest.param(lambda s: pipe(s).filter(lambda x: True, concurrency=3), id="filter"),
            pytest.param(lambda s: pipe(s).batch(2), id="batch"),
            pytest.param(lambda s: pipe(s), id="no_operators"),
            pytest.param(
                lambda s: pipe(s).map(to_str).flat_map(lambda x: [x], concurrency=3).batch(2),
                id="three_stages",
            ),
        ],
    )
    async def test_close_releases_the_upstream_source_for_every_operator(self, build: Any, no_cyclic_gc: None) -> None:
        closed = False

        async def source() -> AsyncIterator[int]:
            nonlocal closed
            try:
                for value in range(100):
                    yield value
            finally:
                closed = True

        stream = build(source()).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        assert closed

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
    @pytest.mark.parametrize("concurrency", [1, 2])
    async def test_source_cancelled_error_is_not_silent_truncation(self, concurrency: int) -> None:
        # A source raising CancelledError must surface rather than look like a
        # short stream, or callers silently accept partial data.
        async def source() -> AsyncIterator[int]:
            yield 1
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pipe(source()).map(double, concurrency=concurrency).collect(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrency", [1, 2])
    async def test_flat_map_source_cancelled_error_is_not_silent_truncation(self, concurrency: int) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pipe(source()).flat_map(lambda x: [x], concurrency=concurrency).collect(), timeout=1)

    @pytest.mark.asyncio
    async def test_close_releases_a_custom_async_expansion_iterator(self) -> None:
        closed = False

        class Cursor:
            def __init__(self) -> None:
                self.emitted = 0

            def __aiter__(self) -> "Cursor":
                return self

            async def __anext__(self) -> int:
                self.emitted += 1
                if self.emitted > 10_000:
                    raise StopAsyncIteration
                return self.emitted

            async def aclose(self) -> None:
                nonlocal closed
                closed = True

        stream = pipe([1]).flat_map(lambda x: Cursor(), concurrency=1, buffer=2).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        # A bare `async for` never calls aclose, so only an async *generator*
        # would get cleaned up here, and only via garbage collection.
        assert closed

    @pytest.mark.asyncio
    async def test_close_releases_a_custom_sync_expansion_iterator(self) -> None:
        closed = False

        class Rows:
            def __init__(self) -> None:
                self.emitted = 0

            def __iter__(self) -> "Rows":
                return self

            def __next__(self) -> int:
                self.emitted += 1
                if self.emitted > 10_000:
                    raise StopIteration
                return self.emitted

            def close(self) -> None:
                nonlocal closed
                closed = True

        stream = pipe([1]).flat_map(lambda x: Rows(), concurrency=1, buffer=2).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        # Nothing but explicit ownership calls close() on a custom iterator, so
        # unlike the generator cases this holds no matter what the collector does.
        assert closed

    @pytest.mark.asyncio
    async def test_close_releases_a_sync_generator_expansion(self, no_cyclic_gc: None) -> None:
        closed = False

        def rows(_: int) -> Generator[int]:
            nonlocal closed
            try:
                yield from range(10_000)
            finally:
                closed = True

        stream = pipe([1]).flat_map(rows, concurrency=1, buffer=2).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        assert closed

    @pytest.mark.asyncio
    async def test_close_releases_an_async_generator_expansion(self, no_cyclic_gc: None) -> None:
        closed = False

        async def rows(_: int) -> AsyncIterator[int]:
            nonlocal closed
            try:
                for value in range(10_000):
                    yield value
            finally:
                closed = True

        stream = pipe([1]).flat_map(rows, concurrency=1, buffer=2).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        assert closed

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda s: pipe(s), id="no_operators"),
            pytest.param(lambda s: pipe(s).map(double, concurrency=1), id="map_c1"),
            pytest.param(lambda s: pipe(s).map(double, concurrency=2), id="map_c2"),
            pytest.param(lambda s: pipe(s).flat_map(lambda x: [x], concurrency=2), id="flat_map_c2"),
            pytest.param(lambda s: pipe(s).batch(2), id="batch"),
        ],
    )
    async def test_close_releases_a_custom_sync_source(self, build: Any) -> None:
        closed = False

        class Source:
            def __init__(self) -> None:
                self.emitted = 0

            def __iter__(self) -> "Source":
                return self

            def __next__(self) -> int:
                self.emitted += 1
                if self.emitted > 10_000:
                    raise StopIteration
                return self.emitted

            def close(self) -> None:
                nonlocal closed
                closed = True

        stream = build(Source()).__aiter__()
        await anext(stream)
        await cast(Any, stream).aclose()

        # close() on a plain object is never called by the collector, so this
        # constrains explicit ownership regardless of gc.
        assert closed

    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrency", [1, 2, 4])
    async def test_source_cleanup_failure_is_reported_by_close(self, concurrency: int) -> None:
        async def source() -> AsyncIterator[int]:
            try:
                yield 1
                await asyncio.Event().wait()
            finally:
                raise ValueError("cleanup boom")

        # Parked in anext() when cancelled, which is where the failure used to be
        # published to a queue the driver had already stopped reading.
        stream = pipe(source()).map(double, concurrency=concurrency).__aiter__()
        await anext(stream)
        await asyncio.sleep(0.02)

        with pytest.raises(ValueError, match="cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrency", [1, 2])
    async def test_expansion_cleanup_failure_is_reported_by_close(self, concurrency: int) -> None:
        class Cursor:
            def __init__(self) -> None:
                self.emitted = 0

            def __aiter__(self) -> "Cursor":
                return self

            async def __anext__(self) -> int:
                self.emitted += 1
                if self.emitted > 10_000:
                    raise StopAsyncIteration
                return self.emitted

            async def aclose(self) -> None:
                raise ValueError("expansion cleanup boom")

        stream = pipe([1]).flat_map(lambda x: Cursor(), concurrency=concurrency, buffer=2).__aiter__()
        await anext(stream)

        with pytest.raises(ValueError, match="expansion cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrency", [1, 2, 4])
    async def test_cleanup_failure_does_not_mask_the_pipeline_error(self, concurrency: int) -> None:
        async def source() -> AsyncIterator[int]:
            try:
                for value in range(100):
                    yield value
            finally:
                raise ValueError("cleanup boom")

        async def boom(_: int) -> int:
            raise RuntimeError("pipeline boom")

        # The pipeline error is the useful one; a teardown failure must not
        # replace it, or the root cause disappears.
        with pytest.raises(RuntimeError, match="pipeline boom"):
            await asyncio.wait_for(pipe(source()).map(boom, concurrency=concurrency).collect(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            # Only the concurrent drivers read ahead, so only they can exhaust the
            # source before the consumer abandons. The sequential paths are lazy and
            # are covered by test_source_cleanup_failure_is_reported_by_close.
            pytest.param(lambda s: pipe(s).map(double, concurrency=2), id="map_c2"),
            pytest.param(lambda s: pipe(s).flat_map(lambda x: [x], concurrency=2), id="flat_map_c2"),
        ],
    )
    async def test_cleanup_failure_on_natural_exhaustion_is_reported(self, build: Any) -> None:
        cleanup_ran = asyncio.Event()

        class ExhaustOnSecondRead:
            def __init__(self) -> None:
                self.emitted = 0

            def __aiter__(self) -> "ExhaustOnSecondRead":
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
        await anext(stream)
        # The consumer holds a value, so the driver is parked at its yield.
        # Waiting on the event pins the state without depending on sleep timing.
        await asyncio.wait_for(cleanup_ran.wait(), timeout=1)

        # No cancellation was pending when cleanup failed, so the failure has to
        # be retained rather than left in a queue nobody reads.
        with pytest.raises(ValueError, match="cleanup boom"):
            await asyncio.wait_for(cast(Any, stream).aclose(), timeout=1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda s: pipe(s), id="no_operators"),
            pytest.param(lambda s: pipe(s).map(double, concurrency=1), id="map_c1"),
            pytest.param(lambda s: pipe(s).map(double, concurrency=2), id="map_c2"),
            pytest.param(lambda s: pipe(s).flat_map(lambda x: [x], concurrency=2), id="flat_map_c2"),
        ],
    )
    async def test_source_close_failure_does_not_mask_a_read_failure(self, build: Any) -> None:
        class ReadThenCloseBoth:
            def __aiter__(self) -> "ReadThenCloseBoth":
                return self

            async def __anext__(self) -> int:
                raise RuntimeError("read boom")

            async def aclose(self) -> None:
                raise ValueError("cleanup boom")

        with pytest.raises(RuntimeError, match="read boom"):
            await asyncio.wait_for(build(ReadThenCloseBoth()).collect(), timeout=1)

    @pytest.mark.asyncio
    async def test_expansion_cleanup_failure_stops_a_live_pipeline(self) -> None:
        class OneThenBadClose:
            def __init__(self) -> None:
                self.emitted = 0

            def __aiter__(self) -> "OneThenBadClose":
                return self

            async def __anext__(self) -> int:
                self.emitted += 1
                if self.emitted > 1:
                    raise StopAsyncIteration
                return self.emitted

            async def aclose(self) -> None:
                raise ValueError("cleanup boom")

        async def live() -> AsyncIterator[int]:
            value = 0
            while True:
                value += 1
                yield value
                await asyncio.sleep(0)

        # The source never ends, so recording the failure for teardown is not
        # enough: a consumer that is still reading has to be told.
        collected: list[int] = []
        with pytest.raises(ValueError, match="cleanup boom"):
            async for item in pipe(live()).flat_map(lambda x: OneThenBadClose(), concurrency=1, buffer=4):
                collected.append(item)
                if len(collected) > 20:
                    break

        assert len(collected) <= 2, f"kept running for {len(collected)} values after cleanup failed"

    @pytest.mark.asyncio
    async def test_expansion_close_failure_does_not_mask_a_read_failure(self) -> None:
        class ReadThenCloseBoth:
            def __aiter__(self) -> "ReadThenCloseBoth":
                return self

            async def __anext__(self) -> int:
                raise RuntimeError("read boom")

            async def aclose(self) -> None:
                raise ValueError("cleanup boom")

        with pytest.raises(RuntimeError, match="read boom"):
            await asyncio.wait_for(
                pipe([1]).flat_map(lambda x: ReadThenCloseBoth(), concurrency=1, buffer=4).collect(),
                timeout=1,
            )

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
    async def test_flat_map_base_exception_during_async_iteration_propagates(self) -> None:
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
        result: list[list[int]] = await pipe(range(6)).map(double, concurrency=3).batch(2).collect()

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
    async def test_in_thread_reuses_one_resource_per_worker_and_scoped_cleanup(self) -> None:
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
            result = await pipe(range(40)).map(in_thread(use_resource, executor=pool), concurrency=40).collect()
            assert set(result) == created
            assert len(created) <= 4
            assert closed == set()

        assert closed == created
