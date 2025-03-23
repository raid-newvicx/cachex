from __future__ import annotations

import math
from datetime import timedelta
from functools import partial
from typing import Any, Protocol, TypeVar, Union, cast, TYPE_CHECKING

from cachex.storage.base import AsyncStorage, Storage


__all__ = ("MemcachedStorage", "AsyncMemcachedStorage")

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing_extensions import ParamSpec, TypeAlias

    class MemcachedClient(Protocol):
        def get(self, key: bytes, default: bytes | None = None) -> bytes | None:
            """The memcached "get" command, but only for one key.

            Args:
                key: The key to retrieve a value
                default: value that will be returned if the key was not found

            Returns:
                The value for the key, or default if the key wasn't found
            """
            ...

        def set(
            self,
            key: bytes,
            value: Any,
            expire: int = 0,
        ) -> Any:
            """
            The memcached "set" command.

            Args:
                key: Key to associate the value with
                value: Value to store
                expire: The number of seconds until the item is expired
                    from the cache, or zero for no expiry (the default).

            Returns:
                The return value is ignored
            """
            ...

        def delete(self, key: bytes) -> Any:
            """The memcached "delete" command.

            Args:
                key: The key to delete from the server

            Returns:
                The return value is ignored
            """
            ...

        def flush_all(self) -> Any:
            """The memcached "flush_all" command.

            Returns:
                The return value is ignored
            """
            ...

        def close(self) -> Any:
            """Closes all sockets to the memcached server.

            Returns:
                The return value is ignored
            """

    class AsyncMemcachedClient(Protocol):
        async def get(self, key: bytes, default: bytes | None = None) -> bytes | None:
            """The memcached "get" command, but only for one key.

            Args:
                key: The key to retrieve a value
                default: value that will be returned if the key was not found

            Returns:
                The value for the key, or default if the key wasn't found
            """
            ...

        async def set(
            self,
            key: bytes,
            value: Any,
            expire: int = 0,
        ) -> Any:
            """
            The memcached "set" command.

            Args:
                key: Key to associate the value with
                value: Value to store
                expire: The number of seconds until the item is expired
                    from the cache, or zero for no expiry (the default).

            Returns:
                The return value is ignored
            """
            ...

        async def delete(self, key: bytes) -> Any:
            """The memcached "delete" command.

            Args:
                key: The key to delete from the server

            Returns:
                The return value is ignored
            """
            ...

        async def flush_all(self) -> Any:
            """The memcached "flush_all" command.

            Returns:
                The return value is ignored
            """
            ...

        async def close(self) -> Any:
            """Closes all sockets to the memcached server.

            Returns:
                The return value is ignored
            """

    T = TypeVar("T")
    P = ParamSpec("P")
    R = TypeVar("R")
    Client = TypeVar("Client", MemcachedClient, AsyncMemcachedClient)
    Result: TypeAlias = Union[T, Awaitable[T]]


class _MemcachedCommon:
    """Common config and commands between the sync and async storage types."""

    __slots__ = "_memcached"

    def __init__(self, memcached: Client) -> None:
        self._memcached = memcached  # type: ignore[var-annotated]

    def _set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> Callable[[], Result[Any]]:
        if isinstance(value, str):
            value = value.encode("utf-8")
        if isinstance(expires_in, timedelta):
            expires_in = math.ceil(expires_in.total_seconds())
        elif expires_in is None:
            expires_in = 0

        return partial(self._memcached.set, key, value, expires_in)

    def _get(self, key: str) -> Callable[[], Result[bytes | None]]:
        return partial(self._memcached.get, key)

    def _delete(self, key: str) -> Callable[[], Result[Any]]:
        return partial(self._memcached.delete, key)

    def _delete_all(self) -> Callable[[], Result[Any]]:
        return self._memcached.flush_all

    def _close(self) -> Callable[[], Result[Any]]:
        return self._memcached.close


class MemcachedStorage(Storage, _MemcachedCommon):
    """Memcached based, synchronous storage."""

    def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        self._set(key, value, expires_in)()

    def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            ``None``
        """
        data = self._get(key)()
        return cast("bytes | None", data)

    def delete(self, key: str) -> None:
        """Delete a value.

        If no such ``key`` exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        self._delete(key)()

    def delete_all(self) -> None:
        """Delete all stored values in the virtual key namespace."""
        self._delete_all()()

    def close(self) -> None:
        """Close the underlying Memcached client."""
        self._close()()


class AsyncMemcachedStorage(AsyncStorage, _MemcachedCommon):
    """Memcached based, asynchronous storage."""

    async def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        await self._set(key, value, expires_in)()  # type: ignore[misc]

    async def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            ``None``
        """
        data = await self._get(key)()  # type: ignore[misc]
        return cast("bytes | None", data)

    async def delete(self, key: str) -> None:
        """Delete a value.

        If no such ``key`` exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        await self._delete(key)()  # type: ignore[misc]

    async def delete_all(self) -> None:
        """Delete all stored values in the virtual key namespace."""
        await self._delete_all()()  # type: ignore[misc]

    async def close(self) -> None:
        """Close the underlying Memcached client."""
        await self._close()()
