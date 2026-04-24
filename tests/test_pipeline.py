from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

import flowlet.functional as F
from flowlet import Flowlet, Pipeline, flowlet, op, pipe


async def double(x: int) -> int:
    return x * 2


async def add_one(x: int) -> int:
    return x + 1


def to_str(x: int) -> str:
    return str(x)


def sync_double(x: int) -> int:
    return x * 2


def sync_add_one(x: int) -> int:
    return x + 1


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
    async def test_reusable_flowlet(self) -> None:
        transform: Flowlet[int, str] = Flowlet[int, int]().map(double).map(to_str)

        result: list[str] = await pipe([1, 2, 3]).then(transform).collect()

        assert result == ["2", "4", "6"]

    @pytest.mark.asyncio
    async def test_flowlet_constructor_sugar(self) -> None:
        transform = flowlet(sync_double, sync_add_one, to_str)

        result: list[str] = await pipe([1, 2, 3]).then(transform).collect()

        assert result == ["3", "5", "7"]

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

        assert result == [x * 2 for x in range(100)]

    @pytest.mark.asyncio
    async def test_ordered_by_default(self) -> None:
        async def slow_inverse(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x

        result: list[int] = await pipe([1, 2, 3]).map(slow_inverse, concurrency=3).collect()

        assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_unordered_emits_completion_order(self) -> None:
        async def slow_inverse(x: int) -> int:
            await asyncio.sleep((3 - x) * 0.01)
            return x

        result: list[int] = await pipe([1, 2, 3]).map(slow_inverse, concurrency=3, ordered=False).collect()

        assert result == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_invalid_concurrency_raises(self) -> None:
        with pytest.raises(ValueError, match="concurrency"):
            pipe([1]).map(double, concurrency=0)


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
    async def test_for_each_runs_side_effects(self) -> None:
        output: list[int] = []

        await pipe([1, 2, 3]).for_each(lambda x: output.append(x))

        assert sorted(output) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_run_drains_pipeline(self) -> None:
        output: list[int] = []

        await pipe([1, 2, 3]).map(lambda x: output.append(x)).run()

        assert output == [1, 2, 3]


class TestErrors:
    @pytest.mark.asyncio
    async def test_worker_exception_propagates(self) -> None:
        async def boom(x: int) -> int:
            if x == 2:
                raise RuntimeError("boom")
            return x

        with pytest.raises(RuntimeError, match="boom"):
            await pipe([1, 2, 3]).map(boom, concurrency=2).collect()


class TestFunctionalApi:
    @pytest.mark.asyncio
    async def test_functional_api(self) -> None:
        transform = F.compose(F.map(double), F.map(add_one), F.map(to_str))

        result: list[str] = await F.collect(transform([1, 2, 3]))

        assert result == ["3", "5", "7"]


def test_typing_surface() -> None:
    numbers: Pipeline[int] = pipe([1, 2, 3])
    text: Pipeline[str] = numbers.map(to_str)
    transform: Flowlet[int, str] = op.map(double) | op.map(to_str)
    filtered: Flowlet[int, int] = op.filter(lambda x: x > 1)
    expanded: Flowlet[int, int] = op.flat_map(lambda x: [x, x])
    functional_transform: F.Operator[int, str] = F.compose(F.map(double), F.map(to_str))

    assert isinstance(text, Pipeline)
    assert isinstance(transform, Flowlet)
    assert isinstance(filtered, Flowlet)
    assert isinstance(expanded, Flowlet)
    assert functional_transform is not None
