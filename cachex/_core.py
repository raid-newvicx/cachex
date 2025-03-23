from __future__ import annotations

import hashlib
import inspect
import logging
from functools import partial, partialmethod
from types import MethodType
from typing import Any, TypeVar, TYPE_CHECKING

from cachex.exceptions import UnhashableParamError, UnhashableTypeError
from cachex._hashing import update_hash

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
    R = TypeVar("R")

_LOGGER = logging.getLogger("cachex.core")


def make_value_key(
    function_key: str,
    func: Callable[P, R],
    type_encoders: Mapping[type, Callable[[Any], Any]] | None,
    *args: Any,
    **kwargs: Any,
) -> str:
    """Create the key for a value within a cache.

    This key is generated from the function's arguments. All arguments
    will be hashed, except for bound method args and those named with a
    leading "_".

    Unhashable types can be hashed through the use of type encoder, a user
    defined function that returns a consistent output across runtimes. Careful
    consideration must go into type encoders to ensure they are consistent.

    Raises:
        UnhashableParamError: When one of the arguments is not hashable. This can
            can be avoided by attaching a leading underscore (_) to the argument
            in the function definition
    """
    arg_pairs: list[tuple[str | None, Any]] = []
    for arg_idx in range(len(args)):
        arg_name = get_positional_arg_name(func, arg_idx)
        arg_pairs.append((arg_name, args[arg_idx]))

    for kw_name, kw_val in kwargs.items():
        # **kwargs ordering is preserved, per PEP 468
        # https://www.python.org/dev/peps/pep-0468/, so this iteration is
        # deterministic.
        arg_pairs.append((kw_name, kw_val))

    # Create the hash from each arg value, except for those args whose name
    # starts with "_".
    args_hasher = hashlib.new("md5")

    # Omit the first arg pair if the func is a bound method
    if isinstance(func, MethodType):
        arg_pairs = arg_pairs[1:]

    for arg_name, arg_value in arg_pairs:
        if arg_name is not None and arg_name.startswith("_"):
            _LOGGER.debug("Not hashing %s because it starts with _", arg_name)
            continue

        try:
            update_hash((arg_name, arg_value), args_hasher, type_encoders)
        except UnhashableTypeError as e:
            raise UnhashableParamError(func, arg_name, arg_value, e)

    value_key = args_hasher.hexdigest()
    key = f"{function_key}_{value_key}"
    _LOGGER.debug("Cache key: %s", key)

    return key


def make_function_key(func: Callable[P, R]) -> str:
    """Create the unique key for a function's cache.

    A function's key is stable across multiple calls, and changes when the
    function's source code changes.
    """
    func_hasher = hashlib.new("md5")

    if isinstance(func, (partial, partialmethod)):
        func = func.func

    # Include the function's __module__ and __qualname__ strings in the hash.
    # This means that two identical functions in different modules
    # will not share a hash; it also means that two identical nested
    # functions in the same module will not share a hash.
    update_hash((func.__module__, func.__qualname__), hasher=func_hasher)

    # Include the function's source code in its hash. If the source code can't
    # be retrieved, fall back to the function's bytecode instead.
    source_code: str | bytes
    try:
        source_code = inspect.getsource(func)
    except OSError as e:
        _LOGGER.debug(
            "Failed to retrieve function's source code when building its key; "
            "falling back to bytecode. err={0}",
            e,
        )
        source_code = func.__code__.co_code

    update_hash(source_code, hasher=func_hasher)

    cache_key = func_hasher.hexdigest()
    return cache_key


def get_positional_arg_name(func: Callable[P, R], arg_index: int) -> str | None:
    """Return the name of a function's positional argument.

    If ``arg_index`` is out of range, or refers to a parameter that is not a
    named positional argument (e.g. an ``*args``, ``**kwargs``, or keyword-only param),
    return ``None`` instead.
    """
    if arg_index < 0:
        return None

    params: list[inspect.Parameter] = list(inspect.signature(func).parameters.values())
    if arg_index >= len(params):
        return None

    if params[arg_index].kind in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    ):
        return params[arg_index].name

    return None
