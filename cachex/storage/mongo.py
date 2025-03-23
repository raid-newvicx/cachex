# mypy: ignore-errors
from __future__ import annotations

import asyncio
import importlib
import inspect
import pickle
import time
import threading
from datetime import datetime, timedelta
from functools import partial
from typing import Any, TypeVar, Union, TYPE_CHECKING

try:
    importlib.import_module("motor")
    from pymongo import IndexModel, MongoClient, ASCENDING, TEXT
    from pymongo.collection import Collection as MongoCollection
    from pymongo.errors import AutoReconnect, OperationFailure
except ImportError:
    raise RuntimeError(
        "Missing required dependencies: motor, pymongo. "
        "Run `pip install 'cachex[mongo]'`"
    )

import anyio

from cachex.exceptions import ImproperlyConfiguredException
from cachex.storage.base import AsyncStorage, Storage, StoredValue


__all__ = ("MongoStorage", "AsyncMongoStorage")

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing_extensions import Concatenate, ParamSpec, TypeAlias

    from motor.core import AgnosticClient, AgnosticCollection

    T = TypeVar("T")
    P = ParamSpec("P")
    R = TypeVar("R")

    AsyncMongoClient: TypeAlias = AgnosticClient
    AsyncMongoCollection: TypeAlias = AgnosticCollection
    Client = TypeVar("Client", MongoClient, AsyncMongoClient)
    Collection = TypeVar("Collection", MongoCollection, AsyncMongoCollection)
    Result: TypeAlias = Union[T, Awaitable[T]]

# Maximum backoff between each retry in seconds
DEFAULT_CAP = 0.512
# Minimum backoff between each retry in seconds
DEFAULT_BASE = 0.008
# The number of consecutive failures before failing an operation (0 base)
MAX_FAILURES = 4
# Datetime for a stored value which does not expire
DNE = datetime(year=2999, month=12, day=31)


def _create_collection(
    method: Callable[
        Concatenate[MongoStorage | AsyncMongoStorage, P], R | Awaitable[R]
    ],
) -> Callable[Concatenate[MongoStorage | AsyncMongoStorage, P], R | Awaitable[R]]:
    """Wraps a storage method and creates the collection if required."""

    def sync_wrapper(self: MongoStorage, *args: Any, **kwargs: Any) -> R:
        if self._has_collection:
            return method(self, *args, **kwargs)

        with self._lock:
            if self._has_collection:
                return method(self, *args, **kwargs)

            collection = self._mongo.get_default_database()[self.collection]
            index_expires_at = IndexModel(
                [("expiresAt", ASCENDING)], expireAfterSeconds=0
            )
            index_key = IndexModel([("key", TEXT)], unique=True)
            call = partial(collection.create_indexes, [index_expires_at, index_key])

            try:
                self._retry_for_auto_reconnect(call)
            except OperationFailure as e:
                raise ImproperlyConfiguredException(
                    "Unable to create indices on the collection. This may "
                    "happen when attempting to use an existing collection with "
                    "competing indices on the same keys used as this storage instance. "
                    "Remove the indices from the existing collection or use a "
                    "different collection name."
                ) from e

            self._collection = collection
            self._has_collection = True

            return method(self, *args, **kwargs)

    async def async_wrapper(self: AsyncMongoStorage, *args: Any, **kwargs: Any) -> R:
        if self._has_collection:
            return await method(self, *args, **kwargs)

        async with self._lock:
            if self._has_collection:
                return await method(self, *args, **kwargs)

            collection = self._mongo.get_default_database()[self.collection]
            index_expires_at = IndexModel(
                [("expiresAt", ASCENDING)], expireAfterSeconds=0
            )
            index_key = IndexModel([("key", TEXT)], unique=True)
            call = partial(collection.create_indexes, [index_expires_at, index_key])

            try:
                await self._retry_for_auto_reconnect_async(call)
            except OperationFailure as e:
                raise ImproperlyConfiguredException(
                    "Unable to create indices on the collection. This may "
                    "happen when attempting to use an existing collection with "
                    "competing indices on the same keys used as this storage instance. "
                    "Remove the indices from the existing collection or use a "
                    "different collection name."
                ) from e

            self._collection = collection
            self._has_collection = True

        return await method(self, *args, **kwargs)

    if inspect.iscoroutinefunction(method):
        return async_wrapper
    return sync_wrapper


class _MongoCommon:
    """Common config and commands between the sync and async storage types."""

    __slots__ = (
        "_mongo",
        "_collection",
        "_max_backoff",
        "_base_backoff",
        "_max_failures",
        "collection",
        "key_prefix",
    )

    def __init__(
        self,
        mongo: Client,
        collection: str | None = None,
        key_prefix: str | None = None,
        max_backoff: float = DEFAULT_CAP,
        base_backoff: float = DEFAULT_BASE,
        max_failures: int = MAX_FAILURES,
    ) -> None:
        self._mongo = mongo
        self.collection: str = "cachex" if collection is None else collection
        self.key_prefix: str = "cachex" if key_prefix is None else key_prefix
        self._max_backoff = max_backoff
        self._base_backoff = base_backoff
        self._max_failures = max_failures

        self._collection: Collection | None = None
        self._has_collection: bool = False

    def _set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> Callable[[], Result[None]]:
        assert self._collection is not None
        key = self._make_key(key)
        if isinstance(value, str):
            value = value.encode("utf-8")
        value = pickle.dumps(StoredValue.new(data=value, expires_in=expires_in))
        expires_at = self._get_expires_at(expires_in)
        return partial(
            self._collection.update_one,
            {"key": key},
            {
                "$set": {"value": value},
                "$setOnInsert": {"key": key, "expiresAt": expires_at},
            },
            upsert=True,
        )

    def _get(self, key: str) -> Callable[[], Result[dict[str, str | bytes] | None]]:
        assert self._collection is not None
        key = self._make_key(key)
        return partial(
            self._collection.find_one,
            {"key": key},
            projection={"value": 1, "_id": 0},
        )

    def _delete(self, key: str) -> Callable[[], Result[None]]:
        assert self._collection is not None
        key = self._make_key(key)
        return partial(self._collection.delete_one, {"key": key})

    def _delete_all(self) -> Callable[[], Result[None]]:
        assert self._collection is not None
        return partial(
            self._collection.delete_many, {"key": {"$regex": f"^{self.key_prefix}_"}}
        )

    def _make_key(self, key: str) -> str:
        return f"{self.key_prefix}_{key}"

    def _get_expires_at(self, expires_in: int | timedelta | None = None) -> datetime:
        if isinstance(expires_in, int):
            return datetime.utcnow() + timedelta(seconds=expires_in)
        elif isinstance(expires_in, timedelta):
            return datetime.utcnow() + expires_in
        elif expires_in is None:
            return DNE
        else:
            raise TypeError(f"Invalid type for 'expires_in'. Got {type(expires_in)}.")

    def _retry_for_auto_reconnect(
        self, call: Callable[[], dict[str, str | bytes] | None]
    ) -> dict[str, str | bytes] | None:
        failures = 0
        while True:
            try:
                return call()
            except AutoReconnect:
                failures += 1
                if failures > self._max_failures:
                    raise
                backoff = min(self._max_backoff, self._base_backoff * 2**failures)
                if backoff > 0:
                    time.sleep(backoff)

    async def _retry_for_auto_reconnect_async(
        self, call: Callable[[], Awaitable[dict[str, str | bytes] | None]]
    ) -> dict[str, str | bytes] | None:
        failures = 0
        while True:
            try:
                return await call()
            except AutoReconnect:
                failures += 1
                if failures > self._max_failures:
                    raise
                backoff = min(self._max_backoff, self._base_backoff * 2**failures)
                if backoff > 0:
                    await asyncio.sleep(backoff)


class MongoStorage(Storage, _MongoCommon):
    """MongoDB based, synchronous storage.

    This storage automatically retries operations which fail due to an
    :class: `pymongo.errors.AutoReconnect` exception using an
    exponential backoff strategy.
    """

    def __init__(
        self,
        mongo: Client,
        collection: str | None = None,
        key_prefix: str | None = None,
        max_backoff: float = DEFAULT_CAP,
        base_backoff: float = DEFAULT_BASE,
        max_failures: int = MAX_FAILURES,
    ) -> None:
        super().__init__(
            mongo, collection, key_prefix, max_backoff, base_backoff, max_failures
        )
        self._lock = threading.Lock()

    @_create_collection
    def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        call = self._set(key, value, expires_in)
        self._retry_for_auto_reconnect(call)

    @_create_collection
    def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            None
        """
        call = self._get(key)
        data = self._retry_for_auto_reconnect(call)
        if data is not None:
            stored_value: StoredValue = pickle.loads(data["value"])
            if stored_value.expired:
                return None
            return stored_value.data

    @_create_collection
    def delete(self, key: str) -> None:
        """Delete a value.

        If no such ``key`` exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        call = self._delete(key)
        self._retry_for_auto_reconnect(call)

    @_create_collection
    def delete_all(self) -> None:
        """Delete all stored values in the virtual key namespace."""
        call = self._delete_all()
        self._retry_for_auto_reconnect(call)

    def close(self) -> None:
        """Close the underlying MongoDB client."""
        self._mongo.close()


class AsyncMongoStorage(AsyncStorage, _MongoCommon):
    """MongoDB based, asynchronous storage.

    This storage automatically retries operations which fail due to an
    :class: `pymongo.errors.AutoReconnect` exception using an
    exponential backoff strategy.
    """

    def __init__(
        self,
        mongo: Client,
        collection: str | None = None,
        key_prefix: str | None = None,
        max_backoff: float = DEFAULT_CAP,
        base_backoff: float = DEFAULT_BASE,
        max_failures: int = MAX_FAILURES,
    ) -> None:
        super().__init__(
            mongo, collection, key_prefix, max_backoff, base_backoff, max_failures
        )
        self._lock = anyio.Lock()

    @_create_collection
    async def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """
        call = self._set(key, value, expires_in)
        await self._retry_for_auto_reconnect_async(call)

    @_create_collection
    async def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            None
        """
        call = self._get(key)
        data = await self._retry_for_auto_reconnect_async(call)
        if data is not None:
            stored_value: StoredValue = pickle.loads(data["value"])
            if stored_value.expired:
                return None
            return stored_value.data

    @_create_collection
    async def delete(self, key: str) -> None:
        """Delete a value.

        If no such ``key`` exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        call = self._delete(key)
        await self._retry_for_auto_reconnect_async(call)

    @_create_collection
    async def delete_all(self) -> None:
        """Delete all stored values in the virtual key namespace."""
        call = self._delete_all()
        await self._retry_for_auto_reconnect_async(call)

    async def close(self) -> None:
        """Close the underlying MongoDB client."""
        self._mongo.close()
