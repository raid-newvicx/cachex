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