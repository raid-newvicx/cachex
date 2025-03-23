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