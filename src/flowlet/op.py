from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import overload

from flowlet import Flowlet
from flowlet.functional import Expander, Predicate


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = True
) -> Flowlet[T, U]: ...


@overload
def map[T, U](fn: Callable[[T], U], *, concurrency: int = 1, ordered: bool = True) -> Flowlet[T, U]: ...  # noqa: A001


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1, ordered: bool = True
) -> Flowlet[T, U]:
    return Flowlet[T, T]().map(fn, concurrency=concurrency, ordered=ordered)


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1, ordered: bool = True) -> Flowlet[T, U]:
    return Flowlet[T, T]().flat_map(fn, concurrency=concurrency, ordered=ordered)


def filter[T](pred: Predicate[T], *, concurrency: int = 1, ordered: bool = True) -> Flowlet[T, T]:  # noqa: A001
    return Flowlet[T, T]().filter(pred, concurrency=concurrency, ordered=ordered)


__all__ = ["filter", "flat_map", "map"]
