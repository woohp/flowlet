from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import overload

from flowlet import Flowlet, functional
from flowlet.functional import Expander, Predicate


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1, preserve_order: bool = True
) -> Flowlet[T, U]: ...


@overload
def map[T, U](fn: Callable[[T], U], *, concurrency: int = 1, preserve_order: bool = True) -> Flowlet[T, U]: ...  # noqa: A001


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1, preserve_order: bool = True
) -> Flowlet[T, U]:
    return Flowlet._from_operator(functional.map(fn, concurrency=concurrency, preserve_order=preserve_order))


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1, preserve_order: bool = True) -> Flowlet[T, U]:
    return Flowlet._from_operator(functional.flat_map(fn, concurrency=concurrency, preserve_order=preserve_order))


def filter[T](pred: Predicate[T], *, concurrency: int = 1, preserve_order: bool = True) -> Flowlet[T, T]:  # noqa: A001
    return Flowlet._from_operator(functional.filter(pred, concurrency=concurrency, preserve_order=preserve_order))


__all__ = ["filter", "flat_map", "map"]
