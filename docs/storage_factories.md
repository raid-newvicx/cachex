# Storage Factories
`cachex` supports multiple storage backends and can be extended to support many more. These backends can be mixed and matched throughout an application. For example, it is possible to have one function use system memory to cache a result while another function can use Redis.

```python
from typing import Any

from cachex import (
    async_cache_value,
    async_redis_storage_factory,
    cache_value,
    redis_storage_factory,
)


REDIS_URL = "redis://localhost:6379/"


@cache_value(storage_factory=redis_storage_factory(REDIS_URL))
def do_something(*args: Any, **kwargs: Any) -> Any:
    ...


@async_cache_value(storage_factory=async_redis_storage_factory(REDIS_URL))
async def do_something_else(*args: Any, **kwargs: Any) -> Any:
    ...


@cache_value() # If no storage factory is specified, the value is cached in memory
def cache_this_result_in_memory(*args: Any, **kwargs: Any) -> Any:
    ...
```

`cachex` uses the concept of *storage factories* to define which backend is used per function. A storage factory is a zero argument callable that returns a `Storage` or `AsyncStorage` object. The callables used in the example above are actually more like storage factory *factories*, they wrap the configuration of a storage backend and return a factory function (zero argument callable). This is best illustrated in the source code for `redis_storage_factory`...

```python
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
```

# Factory Keys
`Storage` and `AsyncStorage` implementations are meant to be re-used across different calls as most storage backend implentations use TCP clients and file handles. It would be very inefficient to create a new storage backend on every call to a cached function. Instead, we want to reuse the backend. Therefore, under the hood, storage factories are wrapped in a call to `cache_reference`. This ensures we only create a single storage instance (thus reusing the underlying client objects). However, this implementation also leads to some unintuitive side effects. For example...

```python
from cachex import cache_value, file_storage_factory


@cache_value(storage_factory=file_storage_factory("prod"))
def foo(n: int) -> int:
    return n


@cache_value(storage_factory=file_storage_factory("dev"))
def bar(n: int) -> int:
    return n


if __name__ == "__main__":
    import os
    import pathlib
    import shutil
    foo(1), bar(1)
    print(pathlib.Path("prod").exists())
    print(pathlib.Path("dev").exists())
    print(len(os.listdir("prod/cachex")))
    shutil.rmtree("prod")
```

If you run this, the output will be...
```
True
False
2
```

This is unintuitve because we defined two different paths that should have created two directories, `~/prod` and `~/dev` but instead we only get one directory and it holds 2 files which are the cached values from our two function calls.

So why does this happen? Remember that a storage factory is a zero argument callable that returns a storage backend. And, in order to not create a new storage backend on every call, the factory function is wrapped in `cache_reference` under the hood. The signature looks like `cache_reference(Callable[[], Storage | AsyncStorage])()`. Do you see the issue? `cache_reference` doesn't know that the paths are different because `file_storage_factory` is just a wrapper that returns a callable. Utimately it looks like two different storage backends are being configured but only ***one is ever created*** and it will have the configuration of whichever function is called first. For example, if we switch the order in which we call `foo` and `bar`...

```python
if __name__ == "__main__":
    import os
    import pathlib
    import shutil
    bar(1), foo(1)
    print(pathlib.Path("prod").exists())
    print(pathlib.Path("dev").exists())
    print(len(os.listdir("dev/cachex")))
    shutil.rmtree("dev")
```

The outut will be...
```
False
True
2
```

Now the `~/dev` path is the one being created and holding our cached data. In order to differentiate the configurations we need to provide a *factory key*. A factory key tells `cache_reference` that the configurations are different and a new storage backend should be created...

```python
from cachex import cache_value, file_storage_factory


@cache_value(storage_factory=file_storage_factory("prod"), factory_key="prod")
def foo(n: int) -> int:
    return n


@cache_value(storage_factory=file_storage_factory("dev"), factory_key="dev")
def bar(n: int) -> int:
    return n


if __name__ == "__main__":
    import pathlib
    import shutil
    foo(1), bar(1)
    print(pathlib.Path("prod").exists())
    print(pathlib.Path("dev").exists())
    shutil.rmtree("prod")
    shutil.rmtree("dev")
```
```
True
True
```