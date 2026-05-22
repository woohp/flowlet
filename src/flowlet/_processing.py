import asyncio
import functools
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor

from flowlet._threading import _is_async_callable


def in_process[**P, R](fn: Callable[P, R], *, executor: ProcessPoolExecutor) -> Callable[P, Awaitable[R]]:
    """Wrap a CPU-bound sync callable so it runs in a process pool.

    The returned async callable can be used in pipeline stages. `executor` is
    required so callers own process lifetime.
    """
    if _is_async_callable(fn):
        raise TypeError("in_process() does not accept async callables")

    @functools.wraps(fn)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        # Do not copy contextvars here: contextvars.Context is not pickleable,
        # so it cannot be propagated through ProcessPoolExecutor like threads.
        # functools.partial keeps kwargs support while still allowing the call to
        # be pickled when fn, args, kwargs, and the return value are pickleable.
        call = functools.partial(fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, call)

    return wrapped


__all__ = ["in_process"]
