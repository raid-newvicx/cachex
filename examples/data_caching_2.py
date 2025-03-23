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