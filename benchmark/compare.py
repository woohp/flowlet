"""flowlet against native executors and asyncio.

    uv run python benchmark/compare.py [--rounds N]

The sleep and math scenarios model I/O- and CPU-bound work; the no-op ones
isolate per-item overhead. Each row is relative to the first (native) row, and
every implementation gets the same pool size. Extra comparison rows appear when
their library is installed.
"""

import argparse
import asyncio
import inspect
import math
import multiprocessing as mp
import os
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor

from flowlet import in_process, in_thread, pipe

try:
    from parlane import apmap, pmap

    HAVE_PARLANE = True
except ImportError:
    HAVE_PARLANE = False

CPU = os.cpu_count() or 4
THREADS = min(32, CPU + 4)
PROCS = min(32, CPU)
ASYNC = 32

IO_ITEMS = list(range(100))
CPU_ITEMS = list(range(200))
TINY_ITEMS = list(range(20_000))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rounds", type=int, default=5)
    asyncio.run(run(parser.parse_args().rounds))


def io_task(x: int) -> int:
    time.sleep(0.05)
    return x


def cpu_task(x: int) -> float:
    total = 0.0
    for i in range(1, 50_000):
        total += math.sin(i * 0.001 + x) * math.cos(i * 0.002)
    return total


def noop(x: int) -> int:
    return x


async def async_io_task(x: int) -> int:
    await asyncio.sleep(0.05)
    return x


async def async_noop(x: int) -> int:
    return x


def optional(available: bool, label: str, run: Callable[[], object]) -> dict[str, Callable[[], object]]:
    """A comparison row that appears only when its library is installed."""
    return {label: run} if available else {}


def threads() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(THREADS)


def processes() -> ProcessPoolExecutor:
    return ProcessPoolExecutor(PROCS, mp_context=mp.get_context("fork"))


def executor_map[T](make: Callable[[], Executor], fn: Callable[[int], T], items: list[int]) -> list[T]:
    with make() as executor:
        return list(executor.map(fn, items))


async def asyncio_gather[T](fn: Callable[[int], Awaitable[T]], items: list[int], limit: int) -> list[T]:
    semaphore = asyncio.Semaphore(limit)

    async def run_one(x: int) -> T:
        async with semaphore:
            return await fn(x)

    return await asyncio.gather(*(run_one(x) for x in items))


async def asyncio_executor[T](
    make: Callable[[], Executor], fn: Callable[[int], T], items: list[int], limit: int
) -> list[T]:
    loop = asyncio.get_running_loop()
    with make() as executor:
        return await asyncio_gather(lambda x: loop.run_in_executor(executor, fn, x), items, limit)


async def flowlet_threads[T](fn: Callable[[int], T], items: list[int]) -> list[T]:
    with threads() as executor:
        return await pipe(items).map(in_thread(fn, executor=executor), concurrency=THREADS, ordered=True).collect()


async def flowlet_processes[T](fn: Callable[[int], T], items: list[int]) -> list[T]:
    with processes() as executor:
        return await pipe(items).map(in_process(fn, executor=executor), concurrency=PROCS, ordered=True).collect()


SCENARIOS: dict[str, dict[str, Callable[[], object]]] = {
    "I/O threads: 100 x 50ms sleep": {
        "concurrent.futures": lambda: executor_map(threads, io_task, IO_ITEMS),
        **optional(HAVE_PARLANE, "parlane pmap", lambda: pmap(io_task, IO_ITEMS, backend="thread", workers=THREADS)),
        "flowlet in_thread": lambda: flowlet_threads(io_task, IO_ITEMS),
    },
    "CPU processes: 200 x heavy math": {
        "concurrent.futures": lambda: executor_map(processes, cpu_task, CPU_ITEMS),
        **optional(HAVE_PARLANE, "parlane pmap", lambda: pmap(cpu_task, CPU_ITEMS, backend="process", workers=PROCS)),
        "flowlet in_process": lambda: flowlet_processes(cpu_task, CPU_ITEMS),
    },
    "async I/O: 100 x 50ms sleep": {
        "asyncio gather": lambda: asyncio_gather(async_io_task, IO_ITEMS, ASYNC),
        **optional(HAVE_PARLANE, "parlane apmap", lambda: apmap(async_io_task, IO_ITEMS, workers=ASYNC)),
        "flowlet map": lambda: pipe(IO_ITEMS).map(async_io_task, concurrency=ASYNC, ordered=True).collect(),
    },
    "threads: 20k no-op": {
        "concurrent.futures": lambda: executor_map(threads, noop, TINY_ITEMS),
        # The fair native baseline for an asyncio library: each item crosses
        # between the event loop and a worker thread, which dominates here.
        "asyncio run_in_executor": lambda: asyncio_executor(threads, noop, TINY_ITEMS, THREADS),
        **optional(HAVE_PARLANE, "parlane pmap", lambda: pmap(noop, TINY_ITEMS, backend="thread", workers=THREADS)),
        "flowlet in_thread": lambda: flowlet_threads(noop, TINY_ITEMS),
    },
    "async: 20k no-op": {
        "asyncio gather": lambda: asyncio_gather(async_noop, TINY_ITEMS, ASYNC),
        **optional(HAVE_PARLANE, "parlane apmap", lambda: apmap(async_noop, TINY_ITEMS, workers=ASYNC)),
        "flowlet map": lambda: pipe(TINY_ITEMS).map(async_noop, concurrency=ASYNC, ordered=True).collect(),
        "flowlet map unordered": lambda: pipe(TINY_ITEMS).map(async_noop, concurrency=ASYNC).collect(),
    },
}


async def run(rounds: int) -> None:
    print(f"{CPU} CPUs; threads={THREADS} processes={PROCS} async={ASYNC}; best of {rounds}")
    for title, implementations in SCENARIOS.items():
        best = dict.fromkeys(implementations, math.inf)
        for _ in range(rounds):
            for name, start_run in implementations.items():
                start = time.perf_counter()
                result = start_run()
                if inspect.isawaitable(result):
                    await result
                best[name] = min(best[name], time.perf_counter() - start)

        baseline = next(iter(best.values()))
        print(f"\n{title}")
        for name, elapsed in best.items():
            print(f"  {name:<24} {elapsed * 1e3:8.1f}ms  {(elapsed / baseline - 1) * 100:+6.1f}%")


if __name__ == "__main__":
    main()
