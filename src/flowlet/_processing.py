import asyncio
import functools
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor

from flowlet._threading import _is_async_callable


def in_process[**P, R](
    fn: Callable[P, R], *, executor: ProcessPoolExecutor
) -> Callable[P, Awaitable[R]]:
    """Wrap a CPU-bound sync function to run in a process pool.

    The result is an async callable you can pass to any stage.
    You supply `executor`, so you decide how long its processes live.
    """
    if _is_async_callable(fn):
        raise TypeError("in_process() does not accept async callables")

    @functools.wraps(fn)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        # No contextvars here: a Context can't be pickled,
        # so unlike a thread, a process can't receive one.
        # The partial keeps keyword arguments working and stays picklable
        # as long as `fn`, its arguments, and its result are.
        call = functools.partial(fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, call)

    return wrapped


__all__ = ["in_process"]
