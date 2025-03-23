from __future__ import annotations

from typing import Any, TYPE_CHECKING


__all__ = (
    "memory_storage_factory",
    "async_memory_storage_factory",
    "file_storage_factory",
    "async_file_storage_factory",
    "mongo_storage_factory",
    "async_mongo_storage_factory",
    "redis_storage_factory",
    "async_redis_storage_factory",
    "memcached_storage_factory",
    "async_memcached_storage_factory",
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike

    from cachex.storage.file import AsyncFileStorage, FileStorage
    from cachex.storage.memcached import (
        AsyncMemcachedClient,
        AsyncMemcachedStorage,
        MemcachedClient,
        MemcachedStorage,
    )
    from cachex.storage.memory import AsyncMemoryStorage, MemoryStorage
    from cachex.storage.mongo import AsyncMongoStorage, MongoStorage
    from cachex.storage.redis import AsyncRedisStorage, RedisStorage


def memory_storage_factory() -> Callable[[], MemoryStorage]:
    """Storage factory for :class: `MemoryStorage <cacachex.storage.memory.MemoryStorage>`"""
    from cachex.storage.memory import MemoryStorage

    return lambda: MemoryStorage()


def async_memory_storage_factory() -> Callable[[], AsyncMemoryStorage]:
    """Storage factory for :class: `AsyncMemoryStorage <cacachex.storage.memory.AsyncMemoryStorage>`"""
    from cachex.storage.memory import AsyncMemoryStorage

    return lambda: AsyncMemoryStorage()


def file_storage_factory(
    path: PathLike[str], key_prefix: str | None = None
) -> Callable[[], FileStorage]:
    """Storage factory for :class: `FileStorage <cacachex.storage.file.FileStorage>`"""
    from cachex.storage.file import FileStorage

    return lambda: FileStorage(path=path, key_prefix=key_prefix)


def async_file_storage_factory(
    path: PathLike[str], key_prefix: str | None = None
) -> Callable[[], AsyncFileStorage]:
    """Storage factory for :class: `AsyncFileStorage <cacachex.storage.file.AsyncFileStorage>`"""
    from cachex.storage.file import AsyncFileStorage

    return lambda: AsyncFileStorage(path=path, key_prefix=key_prefix)


def mongo_storage_factory(
    url: str,
    collection: str | None = None,
    key_prefix: str | None = None,
    max_backoff: float | None = None,
    base_backoff: float | None = None,
    max_failures: int | None = None,
    **client_kwargs: Any,
) -> Callable[[], MongoStorage]:
    """Storage factory for :class: `MongoStorage <cacachex.storage.mongo.MongoStorage>`

    Args:
        url: MongoDB url to connect to
        collection: Collection to use
        key_prefix: Virtual key namespace to use in the collection
        max_backoff: Maximum backoff between each retry in seconds
        base_backoff: Minimum backoff between each retry in seconds
        max_failures: The number of consecutive failures before failing
            an operation (0 base)
        client_kwargs: Additional keyword arguments to pass to the
            :class:`pymongo.MongoClient` constructor
    """
    from pymongo import MongoClient
    from cachex.storage.mongo import (
        MongoStorage,
        DEFAULT_BASE,
        DEFAULT_CAP,
        MAX_FAILURES,
    )

    max_backoff = max_backoff or DEFAULT_CAP
    base_backoff = base_backoff or DEFAULT_BASE
    max_failures = max_failures or MAX_FAILURES

    def wrapper() -> MongoStorage:
        mongo: MongoClient = MongoClient(url, **client_kwargs)
        return MongoStorage(
            mongo,
            collection=collection,
            key_prefix=key_prefix,
            max_backoff=max_backoff,
            base_backoff=base_backoff,
            max_failures=max_failures,
        )

    return wrapper


def async_mongo_storage_factory(
    url: str,
    collection: str | None = None,
    key_prefix: str | None = None,
    max_backoff: float | None = None,
    base_backoff: float | None = None,
    max_failures: int | None = None,
    **client_kwargs: Any,
) -> Callable[[], AsyncMongoStorage]:
    """Storage factory for :class: `AsyncMongoStorage <cacachex.storage.mongo.AsyncMongoStorage>`

    Args:
        url: MongoDB url to connect to
        collection: Collection to use
        key_prefix: Virtual key namespace to use in the collection
        max_backoff: Maximum backoff between each retry in seconds
        base_backoff: Minimum backoff between each retry in seconds
        max_failures: The number of consecutive failures before failing
            an operation (0 base)
        client_kwargs: Additional keyword arguments to pass to the
            :class:`motor.motor_asyncio.AsyncIOMotorClient` constructor
    """
    from motor.core import AgnosticClient
    from motor.motor_asyncio import AsyncIOMotorClient
    from cachex.storage.mongo import (
        AsyncMongoStorage,
        DEFAULT_BASE,
        DEFAULT_CAP,
        MAX_FAILURES,
    )

    max_backoff = max_backoff or DEFAULT_CAP
    base_backoff = base_backoff or DEFAULT_BASE
    max_failures = max_failures or MAX_FAILURES

    def wrapper() -> AsyncMongoStorage:
        mongo: AgnosticClient = AsyncIOMotorClient(url, **client_kwargs)
        return AsyncMongoStorage(
            mongo,
            collection=collection,
            key_prefix=key_prefix,
            max_backoff=max_backoff,
            base_backoff=base_backoff,
            max_failures=max_failures,
        )

    return wrapper


def redis_storage_factory(
    url: str,
    key_prefix: str | None = None,
    **client_kwargs: Any,
) -> Callable[[], RedisStorage]:
    """Storage factory for :class: `RedisStorage <cacachex.storage.redis.RedisStorage>`

    Args:
        url: Redis url to connect to
        key_prefix: Virtual key namespace to use in the db
        client_kwargs: Additional keyword arguments to pass to the
            :method:`redis.Redis.from_url` classmethod
    """
    from redis import Redis
    from cachex.storage.redis import RedisStorage

    def wrapper() -> RedisStorage:
        redis = Redis.from_url(url, **client_kwargs)
        return RedisStorage(redis, key_prefix=key_prefix)

    return wrapper


def async_redis_storage_factory(
    url: str,
    key_prefix: str | None = None,
    **client_kwargs: Any,
) -> Callable[[], AsyncRedisStorage]:
    """Storage factory for :class: `AsyncRedisStorage <cacachex.storage.redis.AsyncRedisStorage>`

    Args:
        url: Redis url to connect to
        key_prefix: Virtual key namespace to use in the db
        client_kwargs: Additional keyword arguments to pass to the
            :method:`redis.asyncio.Redis.from_url` classmethod
    """
    from redis.asyncio import Redis
    from cachex.storage.redis import AsyncRedisStorage

    def wrapper() -> AsyncRedisStorage:
        redis = Redis.from_url(url, **client_kwargs)
        return AsyncRedisStorage(redis, key_prefix=key_prefix)

    return wrapper


def memcached_storage_factory(
    client: MemcachedClient,
) -> Callable[[], MemcachedStorage]:
    """Storage factory for :class: `MemcachedStorage <cacachex.storage.memcached.MemcachedStorage>`.

    Args:
        client: The memcached client to use with the storage
    """
    from cachex.storage.memcached import MemcachedStorage

    def wrapper() -> MemcachedStorage:
        return MemcachedStorage(client)

    return wrapper


def async_memcached_storage_factory(
    client: AsyncMemcachedClient,
) -> Callable[[], AsyncMemcachedStorage]:
    """Storage factory for :class: `AsyncMemcachedStorage <cacachex.storage.memcached.AsyncMemcachedStorage>`.

    Args:
        client: The memcached client to use with the storage
    """
    from cachex.storage.memcached import AsyncMemcachedStorage

    def wrapper() -> AsyncMemcachedStorage:
        return AsyncMemcachedStorage(client)

    return wrapper
