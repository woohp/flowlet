from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import threading
from collections.abc import Awaitable, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Any, cast, overload
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


class ThreadLocalResource[R]:
    """Lazily initialized resource cached independently per thread."""

    class _Sentinel:
        __slots__ = ("closed", "gen")

        def __init__(self, gen: Generator[Any]) -> None:
            self.gen = gen
            self.closed = False

        def close(self) -> None:
            if self.closed:
                return
            self.closed = True
            self.gen.close()

        def __del__(self) -> None:
            with suppress(Exception):
                self.close()

    def __init__(self, fn: Callable[[], Any]) -> None:
        unwrapped = inspect.unwrap(fn)
        if _is_async_callable(fn) or inspect.iscoroutinefunction(unwrapped) or inspect.isasyncgenfunction(unwrapped):
            raise TypeError("thread_local only supports synchronous functions")

        self._fn = fn
        self._name = getattr(unwrapped, "__name__", type(fn).__name__)
        self._local = threading.local()
        self._is_gen = inspect.isgeneratorfunction(unwrapped)

    def __call__(self) -> R:
        try:
            return cast(R, self._local.value)
        except AttributeError:
            pass

        if self._is_gen:
            gen = self._make_generator()
            try:
                value = next(gen)
            except StopIteration:
                raise RuntimeError(f"thread_local generator '{self._name}' did not yield") from None
            self._local.value = value
            self._local._sentinel = self._Sentinel(cast(Generator[Any], gen))
            return value

        value = cast(Callable[[], R], self._fn)()
        self._local.value = value
        return value

    def close(self) -> None:
        sentinel = getattr(self._local, "_sentinel", None)
        with suppress(AttributeError):
            del self._local._sentinel
        with suppress(AttributeError):
            del self._local.value
        if sentinel is not None:
            cast(ThreadLocalResource._Sentinel, sentinel).close()

    def _make_generator(self) -> Generator[R]:
        return cast(Callable[[], Generator[R]], self._fn)()


@overload
def thread_local[R](fn: Callable[[], Generator[R]]) -> ThreadLocalResource[R]: ...


@overload
def thread_local[R](fn: Callable[[], R]) -> ThreadLocalResource[R]: ...


def thread_local[R](fn: Callable[[], Any]) -> ThreadLocalResource[R]:
    """Create a lazily initialized resource cached independently per thread.

    For generator factories, the first yielded value is used as the resource;
    the generator is never resumed and is closed during teardown.
    Plain factories have no cleanup.
    """
    return ThreadLocalResource(fn)


__all__ = ["ThreadLocalResource", "in_thread", "thread_local"]
