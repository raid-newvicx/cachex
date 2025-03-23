from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import anyio

from cachex.storage.base import AsyncStorage, Storage, StoredValue


__all__ = ("MemoryStorage", "AsyncMemoryStorage")

if TYPE_CHECKING:
    from datetime import timedelta


class MemoryStorage(Storage):
    """In memory, synchronous key/value store."""

    __slots__ = ("_mem", "_lock")

    def __init__(self) -> None:
        self._mem: dict[str, StoredValue] = {}
        self._lock = threading.Lock()

    def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        with self._lock:
            self._mem[key] = StoredValue.new(data=value, expires_in=expires_in)

    def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            None
        """
        with self._lock:
            stored_value = self._mem.get(key)

            if not stored_value:
                return None

            if stored_value.expired:
                self._mem.pop(key)
                return None

            return stored_value.data

    def delete(self, key: str) -> None:
        """Delete a value.

        If no such ``key`` exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        with self._lock:
            self._mem.pop(key, None)

    def delete_all(self) -> None:
        """Delete all stored values."""
        with self._lock:
            self._mem.clear()


class AsyncMemoryStorage(AsyncStorage):
    """In memory, asynchronous key/value store."""

    __slots__ = ("_mem", "_lock")

    def __init__(self) -> None:
        self._mem: dict[str, StoredValue] = {}
        self._lock = anyio.Lock()

    async def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        async with self._lock:
            self._mem[key] = StoredValue.new(data=value, expires_in=expires_in)

    async def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            None
        """
        async with self._lock:
            stored_value = self._mem.get(key)

            if not stored_value:
                return None

            if stored_value.expired:
                self._mem.pop(key)
                return None

            return stored_value.data

    async def delete(self, key: str) -> None:
        """Delete a value.

        If no such ``key`` exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        async with self._lock:
            self._mem.pop(key, None)

    async def delete_all(self) -> None:
        """Delete all stored values."""
        async with self._lock:
            self._mem.clear()
