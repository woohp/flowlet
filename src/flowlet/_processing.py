import asyncio
import functools
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from weakref import WeakKeyDictionary

from flowlet._threading import _is_async_callable


def in_process[**P, R](
    fn: Callable[P, R], *, executor: ProcessPoolExecutor, limit: int | None = None
) -> Callable[P, Awaitable[R]]:
    """Wrap a CPU-bound sync callable so it runs in a process pool.

    The returned async callable can be used in pipeline stages. `executor` is
    required so callers own process lifetime; `limit` bounds submissions for
    this wrapper.
    """
    if _is_async_callable(fn):
        raise TypeError("in_process() does not accept async callables")
    if limit is not None and limit < 1:
        raise ValueError("limit must be >= 1")

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
        # Do not copy contextvars here: contextvars.Context is not pickleable,
        # so it cannot be propagated through ProcessPoolExecutor like threads.
        # functools.partial keeps kwargs support while still allowing the call to
        # be pickled when fn, args, kwargs, and the return value are pickleable.
        call = functools.partial(fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        semaphore = get_semaphore()

        if semaphore is None:
            return await loop.run_in_executor(executor, call)

        async with semaphore:
            return await loop.run_in_executor(executor, call)

    return wrapped


__all__ = ["in_process"]
