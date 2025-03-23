# Type Encoders
When a function decorated with any of the `cachex` decorators is called, the library creates a unique hash from *all* arguments passed to the function. This requires that `cachex` be able to convert *all* the arguments to `bytes`. `cachex` can natively encode built in types (`int`, `float`, `str`, `dict`, `list`, etc.) when creating a hash. For types defined in third part libraries or user defined types, `cachex` doesn't know how to produce a stable hash. In those cases there are two options...

1. Name the unhashable argument with a leading underscore. `cachex` will ignore this argument entirely when computing the hash. This is recommended if the input does not impact the output. The example below downloads an HTML page from Github. The `path` argument determines what data we get back so we choose to ignore the connection object.

```python
from http.client import HTTPSConnection
from cachex import cache_value


@cache_value()
def get_something(_conn: HTTPSConnection, path: str) -> bytes:
    # _conn has a leading underscore so it will be ignored by cachex.
    # Only path will be used to compute the hash
    _conn.request("GET", path)
    response = _conn.getresponse()
    return response.read()


def main():
    conn = HTTPSConnection("github.com")
    print(get_something(conn, "/hyprxa"))
```

2. Use a type encoder to convert the custom type into a native type that can be encoded by `cachex`. A type encoder is a mapping of types to callables that return a built in type (`bytes`, `str`, `int`, `float`, etc.). This is recommended if the input helps determine the output. You can have multiple type encoders in a single decorated function.

```python
from pydantic import AnyHttpUrl
from pydantic_core import Url


@cache_value(type_encoders={Url: lambda t: t.unicode_string()})
def download_data(url: AnyHttpUrl):
    # The type encoder converts the Url type to string which cachex can convert
    # to a stable hash

    # The type for a type encoder must support `isinstance`. `AnyHttpUrl` is
    # actually a subscripted generic so it doesn't support `isinstance`. It's
    # type is `pydantic_core.Url` so thats why we use `Url` instead of
    # `AnyHttpUrl` in the decorator
    ...
```