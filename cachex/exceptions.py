from __future__ import annotations

from typing import Any, TypeVar, TYPE_CHECKING

from cachex._util import (
    get_cached_func_name,
    get_fqn_type,
    get_return_value_type,
)


if TYPE_CHECKING:
    from collections.abc import Callable
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
    R = TypeVar("R")


class CacheException(Exception):
    """Base exception that all exceptions derive from."""


class UnhashableTypeError(CacheException):
    """Internal exception raised when a function argument is not hashable."""


class CacheError(CacheException):
    """Raised when an object cannot be pickled or an error occurred reading/writing
    from/to the storage.
    """


class UnhashableParamError(CacheException):
    """Raised when an input value to a caching function is not hashable.

    This can be avoided by attaching a leading underscore (_) to the argument.
    The argument will be skipped when calculating the cache key. Also, you can
    provide type encoders to produce a hashable type.
    """

    def __init__(
        self,
        func: Callable[P, R],
        arg_name: str | None,
        arg_value: Any,
        orig_exc: BaseException,
    ):
        msg = self._create_message(func, arg_name, arg_value)
        super().__init__(msg)
        self.with_traceback(orig_exc.__traceback__)

    @staticmethod
    def _create_message(
        func: Callable[P, R],
        arg_name: str | None,
        arg_value: Any,
    ) -> str:
        arg_name_str = arg_name if arg_name is not None else "(unnamed)"
        arg_type = get_fqn_type(arg_value)
        func_name = func.__name__
        arg_replacement_name = f"_{arg_name}" if arg_name is not None else "_arg"

        return (
            "Cannot hash argument '{}' (of type {}) in '{}'. To address this, "
            "you can force this argument to be ignored by adding a leading "
            "underscore to the arguments name in the function signature "
            "(eg. '{}'). Or, you can provide a type encoder which converts "
            "the unhashable type into a hashable value."
        ).format(arg_name_str, arg_type, func_name, arg_replacement_name)


class UnserializableReturnValueError(CacheException):
    """Raised when a return value from a function cannot be serialized with pickle."""

    def __init__(self, func: Callable[P, R], value: Any):
        msg = self._create_message(func, value)
        super().__init__(msg)

    def _create_message(self, func: Callable[P, R], value: Any) -> str:
        return (
            "Cannot serialize the return value of type '{}' in '{}'. "
            "'cache_value' uses pickle to serialize the functions "
            "return value and safely store it in cache without mutating the "
            "original object. Please convert the return value to a "
            "pickle-serializable type. If you want to cache unserializable "
            "objects such as database connections or HTTP sessions, use "
            "'cache_reference' instead."
        ).format(get_return_value_type(value), get_cached_func_name(func))


class ImproperlyConfiguredException(CacheException):
    """Raised if a storage object is not configured properly."""
