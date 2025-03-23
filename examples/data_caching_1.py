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