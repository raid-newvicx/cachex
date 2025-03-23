from __future__ import annotations

from functools import partial
from typing import TypeVar, Union, cast, TYPE_CHECKING

try:
    from redis import Redis, RedisCluster
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio import RedisCluster as AsyncRedisCluster
except ImportError:
    raise RuntimeError(
        "Missing required dependency: redis. " "Run `pip install 'cachex[redis]'`"
    )

from cachex.storage.base import AsyncStorage, Storage


__all__ = ("RedisStorage", "AsyncRedisStorage")

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import timedelta
    from typing_extensions import ParamSpec, TypeAlias

    T = TypeVar("T")
    P = ParamSpec("P")
    R = TypeVar("R")
    Client = TypeVar("Client", Redis, RedisCluster, AsyncRedis, AsyncRedisCluster)
    Result: TypeAlias = Union[T, Awaitable[T]]


class _RedisCommon:
    """Common config and commands between the sync and async storage types."""

    __slots__ = ("_redis", "key_prefix", "_delete_all_script")

    def __init__(self, redis: Client, key_prefix: str | None = None) -> None:
        self._redis = redis  # type: ignore[var-annotated]
        self.key_prefix: str = "cachex" if key_prefix is None else key_prefix

        self._delete_all_script = self._redis.register_script(  # type: ignore[var-annotated]
            b"""
        local cursor = 0

        repeat
            local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
            for _,key in ipairs(result[2]) do
                redis.call('UNLINK', key)
            end
            cursor = tonumber(result[1])
        until cursor == 0
        """
        )

    def _set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> Callable[[], Result[None]]:
        key = self._make_key(key)
        if isinstance(value, str):
            value = value.encode("utf-8")
        return partial(self._redis.set, key, value, ex=expires_in)

    def _get(self, key: str) -> Callable[[], Result[bytes | None]]:
        key = self._make_key(key)
        return partial(self._redis.get, key)

    def _delete(self, key: str) -> Callable[[], Result[None]]:
        key = self._make_key(key)
        return partial(self._redis.delete, key)

    def _delete_all(self) -> Callable[[], Result[None]]:
        return partial(self._delete_all_script, keys=[], args=[f"{self.key_prefix}*:*"])

    def _make_key(self, key: str) -> str:
        return f"{self.key_prefix}:{key}"


class RedisStorage(Storage, _RedisCommon):
    """Redis based, synchronous storage."""

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
        """Close the underlying Redis client."""
        self._redis.close()


class AsyncRedisStorage(AsyncStorage, _RedisCommon):
    """Redis based, asynchronous storage."""

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
        """Close the underlying Redis client."""
        await self._redis.aclose()
