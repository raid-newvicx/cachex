from cachex import cache_value
from pydantic import AnyHttpUrl
from pydantic_core import Url


@cache_value(type_encoders={Url: lambda t: t.unicode_string()})
def download_data(url: AnyHttpUrl) -> str:
    # The type encoder converts the Url type to string which cachex can convert
    # to a stable hash

    # The type for a type encoder must support `isinstance`. `AnyHttpUrl` is
    # actually a subscripted generic so it doesn't support `isinstance`. It's
    # type is `pydantic_core.Url` so thats why we use `Url` instead of
    # `AnyHttpUrl` in the decorator
    return url.unicode_string()


if __name__ == "__main__":
    print(download_data(AnyHttpUrl("https://github.com/hyprxa/piwebx")))