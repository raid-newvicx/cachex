# Data Caching
Typically, when using `cachex`, you will be caching data - HTTP response data, database query results, ML model outputs etc. are common datasets that you would want to cache. In those cases you will be using the `cache_value` and `async_cache_value` API's for sync and async contexts respectively.

`cachex` uses the [cache aside pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/cache-aside) exclusively. This may be expanded in the future.
## `pickle`
`cachex` uses `pickle` to serialize the return value of a deorated function. This has a few implications, though none more important than this...

**Only ever use `cachex` in trusted environments. The `pickle` module is not secure and you should only ever unpickle data you trust!!!**

Using `pickle` also means...
- The data is stored in a binary format which means that ***any storage service*** can be used to store cached data, even ones not officially supported by `cachex`
- Every cache hit returns a ***copy*** of the original data. This means multiple handlers can mutate the data without side effects

```python
import asyncio
import time

from cachex import async_cache_value, cache_value


class Result:
    def __init__(self) -> None:
        self.id = id(self)


@cache_value()
def sim_long_op(n: int) -> Result:
    """Simulate a long operation by sleeping and return a result."""
    time.sleep(n)
    return Result()


@async_cache_value()
async def asim_long_op(n: int) -> Result:
    """Simulate a long operation by sleeping and return a result."""
    await asyncio.sleep(n)
    return Result()


def main_sync() -> None:
    r1 = sim_long_op(2)
    r2 = sim_long_op(2)
    r3 = sim_long_op(1)

    # r2 is a copy of r1 meaning the memory address should be different
    assert id(r1) != id(r2)

    # but the id values should be the same
    assert r1.id == r2.id

    # and we can safely mutate r2 without influencing r1
    r2.id = id(r2)
    assert r1.id != r2.id

    # r3 should have a different id value because sim_long_op(1) ran
    assert r2.id != r3.id

async def main_async() -> None:
    r1 = await asim_long_op(2)
    r2 = await asim_long_op(2)
    r3 = await asim_long_op(1)

    # r2 is a copy of r1 meaning the memory address should be different
    assert id(r1) != id(r2)

    # but the id values should be the same
    assert r1.id == r2.id

    # and we can safely mutate r2 without influencing r1
    r2.id = id(r2)
    assert r1.id != r2.id

    # r3 should have a different id value because sim_long_op(1) ran
    assert r2.id != r3.id


if __name__ == "__main__":
    import timeit
    sync_start = timeit.default_timer()
    main_sync()
    sync_end = timeit.default_timer()

    async_start = timeit.default_timer()
    asyncio.run(main_async())
    async_end = timeit.default_timer()

    print(f"main_sync: executed in {sync_end-sync_start} seconds (should be ~3 seconds)")
    print(f"main_async: executed in {async_end-async_start} seconds (should be ~3 seconds)")
```

## Expiration
The data caching API supports time based expiration policies. The implementation details are dependent on the storage backend used but *all* backends support time based expiration.

```python
from datetime import datetime

from cachex import cache_value


@cache_value(expires_in=2)
def now() -> datetime:
    return datetime.now()


if __name__ == "__main__":
    import time
    print(now().isoformat())
    time.sleep(1)
    print(now().isoformat()) # This is the same time as the first call
    time.sleep(1.01)
    print(now().isoformat()) # The time is now updated because the old result expired
```

## Concurrency
By default, `cachex` does not lock concurrent calls to a function. In most cases you dont want that. But, this does mean that depending on the function implementation, the same input can produce a different output. For example...

```python
import asyncio
import random
from datetime import datetime

from cachex import async_cache_value


@async_cache_value()
async def now() -> datetime:
    await asyncio.sleep(random.uniform(0.1, 1.1))
    return  datetime.now()


async def main() -> None:
    results = await asyncio.gather(now(), now())
    for result in results:
        print(result.isoformat())


if __name__ == "__main__":
    asyncio.run(main())
```

In the above example, if `async_cache_value` locked the concurrent calls, we would get the exact same result twice. Run the script though and you will notice that two different times are printed. The `now` function is *time variant* meaning its output changes based on *when* its called. There are many cases where a function can be time variant. In a live system, database queries become time variant as users update data.

If you do want to force synchronous access to guard against time variance you can disable concurrency by setting `allow_concurrent=False`. This will ensure that two calls to a function with the same input are guarenteed to produce the same output.

**Note: This will impact performance.**

```python
import asyncio
import random
from datetime import datetime

from cachex import async_cache_value


@async_cache_value(allow_concurrent=False)
async def now() -> datetime:
    await asyncio.sleep(random.uniform(0.1, 1.1))
    return  datetime.now()


async def main() -> None:
    results = await asyncio.gather(now(), now())
    for result in results:
        print(result.isoformat())


if __name__ == "__main__":
    asyncio.run(main())
```