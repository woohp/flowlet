import asyncio
import contextvars
import functools
import inspect
import threading
from collections.abc import Awaitable, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast, overload


def in_thread[**P, R](
    fn: Callable[P, R], *, executor: ThreadPoolExecutor | None = None
) -> Callable[P, Awaitable[R]]:
    """Wrap a blocking sync function to run in a thread pool.

    The result is an async callable you can pass to any stage.
    With `executor=None`, it uses asyncio's default pool.
    """
    if _is_async_callable(fn):
        raise TypeError("in_thread() does not accept async callables")

    @functools.wraps(fn)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        # Carry contextvars, like request or trace IDs, into the worker thread.
        ctx = contextvars.copy_context()
        call = functools.partial(ctx.run, fn, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, call)

    return wrapped


def _is_async_callable(fn: Callable[..., object]) -> bool:
    """Whether `fn` is a coroutine or async-generator function.

    Looks through `functools.partial`,
    so pre-binding arguments can't sneak an async function past the wrappers.
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
    """A lazily created resource, cached separately for each thread."""

    def __init__(self, fn: Callable[[], Any]) -> None:
        unwrapped = inspect.unwrap(fn)
        if (
            _is_async_callable(fn)
            or inspect.iscoroutinefunction(unwrapped)
            or inspect.isasyncgenfunction(unwrapped)
        ):
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
            gen = cast(Generator[R], self._fn())
            try:
                value = next(gen)
            except StopIteration:
                raise RuntimeError(
                    f"thread_local generator '{self._name}' did not yield"
                ) from None
            # Only thread-local storage holds the generator, so when the thread
            # exits, the generator is finalized, which runs its cleanup.
            self._local.gen = gen
        else:
            value = cast(Callable[[], R], self._fn)()
        self._local.value = value
        return value

    def close(self) -> None:
        state = vars(self._local)
        state.pop("value", None)
        gen = state.pop("gen", None)
        if gen is not None:
            gen.close()


@overload
def thread_local[R](fn: Callable[[], Generator[R]]) -> ThreadLocalResource[R]: ...


@overload
def thread_local[R](fn: Callable[[], R]) -> ThreadLocalResource[R]: ...


def thread_local[R](fn: Callable[[], Any]) -> ThreadLocalResource[R]:
    """Create a lazily built resource, cached separately for each thread.

    With a generator factory, the first value it yields is the resource.
    The generator is never resumed;
    it's closed by `close()` or when its thread exits.
    A plain factory gets no cleanup.
    """
    return ThreadLocalResource(fn)


__all__ = ["ThreadLocalResource", "in_thread", "thread_local"]
