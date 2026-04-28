from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast
from weakref import WeakKeyDictionary


def in_thread[**P, R](
    fn: Callable[P, R], *, executor: ThreadPoolExecutor | None = None, limit: int | None = None
) -> Callable[P, Awaitable[R]]:
    """Wrap a blocking sync callable so it runs in a thread pool.

    The returned async callable can be used in pipeline stages. `limit` bounds
    submissions for this wrapper; `executor=None` uses asyncio's default thread
    pool.
    """
    if _is_async_callable(fn):
        raise TypeError("in_thread() does not accept async callables")
    if limit is not None and limit < 1:
        raise ValueError("limit must be >= 1")

    # Keep one semaphore per event loop so an in_thread(...) wrapper can be
    # safely reused across multiple asyncio.run(...) calls without sharing an
    # asyncio primitive across loop contexts. Weak keys avoid retaining closed
    # loops.
    semaphores: WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = WeakKeyDictionary()

    def get_semaphore() -> asyncio.Semaphore | None:
        if limit is None:
            return None

        loop = asyncio.get_running_loop()
        semaphore = semaphores.get(loop)
        if semaphore is None:
            semaphore = asyncio.Semaphore(limit)
            semaphores[loop] = semaphore
        return semaphore

    @functools.wraps(fn)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        # Propagate contextvars such as request or trace IDs into the worker thread.
        ctx = contextvars.copy_context()
        call = functools.partial(ctx.run, fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        semaphore = get_semaphore()

        if semaphore is None:
            return await loop.run_in_executor(executor, call)

        async with semaphore:
            return await loop.run_in_executor(executor, call)

    return wrapped


def _is_async_callable(fn: Callable[..., object]) -> bool:
    """Return true for coroutine or async-generator callables.

    `functools.partial` wrappers are unwrapped so `in_thread` and `in_process`
    reject async callables consistently even when arguments were pre-bound.
    """
    candidate = cast(Any, fn)
    while isinstance(candidate, functools.partial):
        candidate = candidate.func

    call = candidate.__call__
    return (
        inspect.iscoroutinefunction(candidate)
        or inspect.isasyncgenfunction(candidate)
        or inspect.iscoroutinefunction(call)
        or inspect.isasyncgenfunction(call)
    )


__all__ = ["in_thread"]
