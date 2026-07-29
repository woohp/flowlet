import asyncio
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

        result = await asyncio.wait_for(
            pipe(range(8)).flat_map(expand, concurrency=8, buffer=1).collect(), timeout=5
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
