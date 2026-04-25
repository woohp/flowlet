from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import overload

import flowlet.functional as functional
from flowlet._flow import Flow
from flowlet.functional import Expander, Predicate


@overload
def map[T, U](  # noqa: A001
    fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1
) -> Flow[T, U]: ...


@overload
def map[T, U](fn: Callable[[T], U], *, concurrency: int = 1) -> Flow[T, U]: ...  # noqa: A001


def map[T, U](  # noqa: A001
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]], *, concurrency: int = 1
) -> Flow[T, U]:
    return Flow._from_operator(functional.map(fn, concurrency=concurrency))


def flat_map[T, U](fn: Expander[T, U], *, concurrency: int = 1) -> Flow[T, U]:
    return Flow._from_operator(functional.flat_map(fn, concurrency=concurrency))


def filter[T](pred: Predicate[T], *, concurrency: int = 1) -> Flow[T, T]:  # noqa: A001
    return Flow._from_operator(functional.filter(pred, concurrency=concurrency))


__all__ = ["filter", "flat_map", "map"]
