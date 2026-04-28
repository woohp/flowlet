from __future__ import annotations

import asyncio
import functools
import multiprocessing as mp
import threading
import time
from collections.abc import AsyncIterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, cast

import pytest

import flowlet.functional as F
from flowlet import Flow, Pipeline, in_process, in_thread, op, pipe


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


def process_sleep_and_track(x: int, running: Any, max_running: Any, lock: Any) -> int:
    with lock:
        running.value += 1
        max_running.value = max(max_running.value, running.value)
    time.sleep(0.05)
    with lock:
        running.value -= 1
    return x


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
    async def test_invalid_concurrency_raises(self) -> None:
        with pytest.raises(ValueError, match="concurrency"):
            pipe([1]).map(double, concurrency=0)


class TestInThread:
    @pytest.mark.asyncio
    async def test_in_thread_supports_blocking_map(self) -> None:
        result = await pipe([1, 2, 3]).map(in_thread(lambda x: x * 2), concurrency=3).collect()

        assert sorted(result) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_in_thread_limit_bounds_parallelism(self) -> None:
        running = 0
        max_running = 0
        lock = threading.Lock()

        def block(x: int) -> int:
            nonlocal max_running, running
            with lock:
                running += 1
                max_running = max(max_running, running)
            time.sleep(0.02)
            with lock:
                running -= 1
            return x

        result = await pipe(range(6)).map(in_thread(block, limit=2), concurrency=6).collect()

        assert sorted(result) == list(range(6))
        assert max_running == 2

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

        result = (
            await pipe(range(200))
            .map(in_thread(block, limit=8), concurrency=64)
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
                .map(in_thread(stage_one, executor=pool, limit=3), concurrency=16)
                .map(in_thread(stage_two, executor=pool, limit=2), concurrency=16)
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

    def test_in_thread_rejects_invalid_limit(self) -> None:
        with pytest.raises(ValueError, match="limit"):
            in_thread(lambda x: x, limit=0)

    def test_in_thread_preserves_function_metadata(self) -> None:
        def named(x: int) -> int:
            """docstring"""

            return x

        wrapped = in_thread(named)

        assert wrapped.__name__ == "named"
        assert wrapped.__doc__ == "docstring"

    def test_in_thread_wrapper_can_be_reused_across_event_loops(self) -> None:
        running = 0
        max_running = 0
        lock = threading.Lock()

        def block(x: int) -> int:
            nonlocal max_running, running
            with lock:
                running += 1
                max_running = max(max_running, running)
            time.sleep(0.01)
            with lock:
                running -= 1
            return x

        wrapped = in_thread(block, limit=2)

        async def run_once() -> tuple[list[int], int]:
            nonlocal max_running, running

            result = await pipe(range(6)).map(wrapped, concurrency=6).collect()
            return result, max_running

        first_result, first_max = asyncio.run(run_once())
        max_running = 0
        second_result, second_max = asyncio.run(run_once())

        assert sorted(first_result) == list(range(6))
        assert sorted(second_result) == list(range(6))
        assert first_max == 2
        assert second_max == 2


class TestInProcess:
    @pytest.mark.asyncio
    async def test_in_process_supports_blocking_map(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=2, mp_context=ctx) as pool:
            result = await pipe([1, 2, 3]).map(in_process(process_double, executor=pool), concurrency=3).collect()

        assert sorted(result) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_in_process_limit_bounds_parallelism(self) -> None:
        ctx = mp.get_context("fork")
        with mp.Manager() as manager:
            running = manager.Value("i", 0)
            max_running = manager.Value("i", 0)
            lock = manager.Lock()
            task = functools.partial(process_sleep_and_track, running=running, max_running=max_running, lock=lock)

            with ProcessPoolExecutor(max_workers=4, mp_context=ctx) as pool:
                result = await pipe(range(6)).map(in_process(task, executor=pool, limit=2), concurrency=6).collect()

            assert sorted(result) == list(range(6))
            assert max_running.value == 2

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

    def test_in_process_rejects_invalid_limit(self) -> None:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool, pytest.raises(ValueError, match="limit"):
            in_process(process_double, executor=pool, limit=0)

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
    async def test_for_each_runs_side_effects(self) -> None:
        output: list[int] = []

        await pipe([1, 2, 3]).for_each(lambda x: output.append(x))

        assert sorted(output) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_for_each_supports_concurrency(self) -> None:
        output: list[int] = []

        async def append_slowly(x: int) -> None:
            await asyncio.sleep((3 - x) * 0.01)
            output.append(x)

        await pipe([1, 2, 3]).for_each(append_slowly, concurrency=3)

        assert sorted(output) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_for_each_uses_concurrency_by_default(self) -> None:
        running = 0
        max_running = 0

        async def track_running(_: int) -> None:
            nonlocal max_running, running
            running += 1
            max_running = max(max_running, running)
            await asyncio.sleep(0.01)
            running -= 1

        await pipe([1, 2, 3]).for_each(track_running, concurrency=3)

        assert max_running == 3

    @pytest.mark.asyncio
    async def test_for_each_with_concurrency_one_runs_serially(self) -> None:
        output: list[int] = []

        async def append_slowly(x: int) -> None:
            await asyncio.sleep((3 - x) * 0.01)
            output.append(x)

        await pipe([1, 2, 3]).for_each(append_slowly, concurrency=1)

        assert output == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_for_each_propagates_errors(self) -> None:
        async def boom(x: int) -> None:
            if x == 2:
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await pipe([1, 2, 3]).for_each(boom, concurrency=2)

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

    assert isinstance(text, Pipeline)
    assert callable(threaded)
    assert isinstance(sourceless_flow, Flow)
    assert isinstance(transform, Flow)
    assert isinstance(filtered, Flow)
    assert isinstance(expanded, Flow)
    assert functional_transform is not None
