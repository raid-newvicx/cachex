from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Generic, TypeVar, TYPE_CHECKING


__all__ = ("Storage", "AsyncStorage")

if TYPE_CHECKING:
    from typing_extensions import Self

T = TypeVar("T")


class Storage(ABC):
    """An abstract storage class that all synchronous storage implementations
    inherit from.
    """

    @abstractmethod
    def set(
        self, key: str, value: bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists

        Raises:
            CacheKeyNotFoundError: If a value associated with ``key`` does not
                exist
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a value.

        If no such key exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        raise NotImplementedError

    @abstractmethod
    def delete_all(self) -> None:
        """Delete all stored values."""
        raise NotImplementedError


class AsyncStorage(ABC):
    """An abstract storage class that all asynchronous storage implementations
    inherit from.
    """

    @abstractmethod
    async def set(
        self, key: str, value: bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        raise NotImplementedError

    @abstractmethod
    async def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists

        Raises:
            CacheKeyNotFoundError: If a value associated with ``key`` does not
                exist
        """
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete a value.

        If no such key exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_all(self) -> None:
        """Delete all stored values."""
        raise NotImplementedError


@dataclass(slots=True)
class StoredObject(Generic[T]):
    """Common methods for stored objects."""

    expires_at: datetime | None
    data: T

    @classmethod
    def new(cls, data: T, expires_in: int | timedelta | None) -> Self:
        """Construct a new :class:`StorageObject` instance."""
        if expires_in is not None and not isinstance(expires_in, timedelta):
            if not isinstance(expires_in, int):
                raise TypeError(f"Invalid type for 'expires_in': {type(expires_in)}")
            expires_in = timedelta(seconds=expires_in)
        if expires_in is not None and expires_in.total_seconds() <= 0:
            raise ValueError("'expires_in' must be greater than 0")
        return cls(
            data=data,
            expires_at=(datetime.now(UTC) + expires_in) if expires_in else None,
        )

    @property
    def expired(self) -> bool:
        """Return ``True`` if the :class:`StorageObject` is expired."""
        return self.expires_at is not None and datetime.now(UTC) >= self.expires_at


# A stored object typed for bytes only
StoredValue = StoredObject[bytes]
