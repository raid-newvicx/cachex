import asyncio
import concurrent.futures
import time
from unittest.mock import MagicMock

import pytest

from cachex import cache_reference
from cachex.exceptions import UnhashableParamError


def test_returns_same_object():
    """Test that reference cache returns the same object not a copy of the object."""
    from http.client import HTTPConnection

    mock = MagicMock()

    @cache_reference()
    def call(host: str):
        conn = HTTPConnection(host)
        conn.connect()
        mock()
        return conn

    result_1 = call("google.com")
    result_2 = call("google.com")
    result_3 = call("apple.com")
    result_4 = call("apple.com")

    assert (
        result_1 == result_2
        and id(result_1) == id(result_2)
        and result_2 != result_3
        and id(result_2) != id(result_3)
        and result_3 == result_4
        and id(result_3) == id(result_4)
    )
    assert mock.call_count == 2


@pytest.mark.asyncio
async def test_returns_same_object_async():
    """Test that reference cache returns the same object not a copy of the object."""
    from http.client import HTTPConnection

    mock = MagicMock()

    @cache_reference()
    async def call(host: str):
        conn = HTTPConnection(host)
        conn.connect()
        mock()
        return conn

    result_1 = await call("google.com")
    result_2 = await call("google.com")
    result_3 = await call("apple.com")
    result_4 = await call("apple.com")

    assert (
        result_1 == result_2
        and id(result_1) == id(result_2)
        and result_2 != result_3
        and id(result_2) != id(result_3)
        and result_3 == result_4
        and id(result_3) == id(result_4)
    )
    assert mock.call_count == 2


def test_type_encoders():
    """Tests that types which are not natively hashable can be hashed by providing
    a callable that encodes the type.
    """
    from http.client import HTTPConnection

    mock = MagicMock()

    @cache_reference()
    def call_fail(conn: HTTPConnection):
        pass

    def encode_conn(conn: HTTPConnection):
        return f"{conn.host}:{conn.port}"

    type_encoders = {HTTPConnection: encode_conn}

    @cache_reference(type_encoders=type_encoders)
    def call_success(conn: HTTPConnection):
        mock(conn)
        return conn

    conn = HTTPConnection("google.com")
    conn.connect()
    try:
        with pytest.raises(UnhashableParamError):
            call_fail(conn)
        result_1 = call_success(conn)
        result_2 = call_success(conn)
    finally:
        conn.close()

    assert result_1 == result_2 and id(result_1) == id(result_2)
    mock.assert_called_once_with(conn)


@pytest.mark.asyncio
async def test_type_encoders_async():
    """Tests that types which are not natively hashable can be hashed by providing
    a callable that encodes the type.
    """
    from http.client import HTTPConnection

    mock = MagicMock(return_value="pickleable")

    @cache_reference()
    async def call_fail(conn: HTTPConnection):
        pass

    def encode_conn(conn: HTTPConnection):
        return f"{conn.host}:{conn.port}"

    type_encoders = {HTTPConnection: encode_conn}

    @cache_reference(type_encoders=type_encoders)
    async def call_success(conn: HTTPConnection):
        mock(conn)
        return conn

    conn = HTTPConnection("google.com")
    conn.connect()
    try:
        with pytest.raises(UnhashableParamError):
            await call_fail(conn)
        result_1 = await call_success(conn)
        result_2 = await call_success(conn)
    finally:
        conn.close()

    assert result_1 == result_2 and id(result_1) == id(result_2)
    mock.assert_called_once_with(conn)


def test_concurrent_calls_are_serialized():
    """Tests that concurrent calls to a cached function are serialized."""
    mock = MagicMock(return_value=lambda: str(10))  # Ensure we create a new object

    @cache_reference()
    def call_mock():
        time.sleep(0.1)
        return mock()()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futs = (executor.submit(call_mock), executor.submit(call_mock))
        concurrent.futures.wait(futs)

    result_1 = futs[0].result()
    result_2 = futs[1].result()
    assert result_1 == result_2 and id(result_1) == id(result_2)
    mock.assert_called_once()


@pytest.mark.asyncio
async def test_concurrent_calls_are_serialized_async():
    """Tests that concurrent calls to a cached function are serialized."""
    mock = MagicMock(return_value=lambda: str(10))  # Ensure we create a new object

    @cache_reference()
    async def call_mock():
        await asyncio.sleep(0.1)
        return mock()

    coro_1, coro_2 = (call_mock(), call_mock())
    result_1, result_2 = await asyncio.gather(coro_1, coro_2)

    assert result_1 == result_2 and id(result_1) == id(result_2)
    mock.assert_called_once()
