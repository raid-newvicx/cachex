from __future__ import annotations

import functools
import inspect
import logging
import threading
from typing import Any, TypeVar, TYPE_CHECKING

import anyio

from cachex._core import make_function_key, make_value_key


__all__ = ("cache_reference", "get_references")

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
    R = TypeVar("R")

_LOGGER = logging.getLogger("cachex.ref")

_lock = threading.Lock()
_ref_storage: dict[str, Any] = {}


def cache_reference(
    type_encoders: Mapping[type, Callable[[Any], Any]] | None = None,
) -> Callable[[Callable[P, R | Awaitable[R]]], Callable[P, R | Awaitable[R]]]:
    """Cache a reference to the return object of the decorated callable.

    Objects are shared across all threads in the application. These objects must
    be thread-safe, because they can be accessed from multiple threads concurrently.

    This decorator works with both sync and async functions. Calls are serialized
    with the appropriate lock type (:class: `threading.Lock` or :class: `anyio.Lock`)
    to ensure that duplicate objects are not created.

    Args:
        type_encoders: A mapping of types to callables that transform them
            into (eventually) a hashable type
    """

    def wraps(func: Callable[P, R | Awaitable[R]]) -> Callable[P, R | Awaitable[R]]:
        function_key = make_function_key(func)
        is_async = inspect.iscoroutinefunction(func)
        if is_async:
            lock = anyio.Lock()
        else:
            lock = threading.Lock()  # type: ignore[assignment]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> R:
            with lock:  # type: ignore[attr-defined]
                key = make_value_key(function_key, func, type_encoders, *args, **kwargs)
                try:
                    with _lock:
                        ref = _ref_storage[key]
                except KeyError:
                    _LOGGER.debug("Cache miss: %s", func)
                else:
                    _LOGGER.debug("Cache hit: %s", func)
                    return ref

                ref = func(*args, **kwargs)
                with _lock:
                    _ref_storage[key] = ref
                return ref

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> R:
            async with lock:
                key = make_value_key(function_key, func, type_encoders, *args, **kwargs)
                try:
                    with _lock:
                        ref = _ref_storage[key]
                except KeyError:
                    _LOGGER.debug("Cache miss: %s", func)
                else:
                    _LOGGER.debug("Cache hit: %s", func)
                    return ref

                ref = await func(*args, **kwargs)  # type: ignore[misc]
                with _lock:
                    _ref_storage[key] = ref
                return ref

        if is_async:
            return async_wrapper
        return sync_wrapper

    return wraps


def get_references() -> tuple[Any, ...]:
    """Return all cached references.

    This is typically used to finalize cached objects and ensure all their
    resources are cleaned up appropriately.
    """
    with _lock:
        return tuple(_ref_storage.values())
