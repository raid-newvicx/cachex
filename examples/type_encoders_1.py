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


if __name__ == "__main__":
    main()