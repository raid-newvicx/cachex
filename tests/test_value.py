import asyncio
import concurrent.futures
import time
from unittest.mock import MagicMock

import pytest

from cachex import async_cache_value, cache_value
from cachex.exceptions import (
    UnhashableParamError,
    UnserializableReturnValueError,
)


def test_can_cache_pickleable_value():
    """Tests that a cached function is only called once."""
    mock = MagicMock(return_value="pickleable")

    @cache_value()
    def call_mock():
        return mock()

    result_1 = call_mock()
    result_2 = call_mock()
    assert result_1 == result_2
    mock.assert_called_once()


@pytest.mark.asyncio
async def test_can_cache_pickleable_value_async():
    """Tests that a cached async function is only called once."""
    mock = MagicMock(return_value="pickleable")

    @async_cache_value()
    async def call_mock():
        return mock()

    result_1 = await call_mock()
    result_2 = await call_mock()
    assert result_1 == result_2
    mock.assert_called_once()


def test_cannot_cache_non_pickleable_value():
    """Tests that non-pickleable return values cannot be cached."""
    import tempfile

    @cache_value()
    def not_picklable():
        with tempfile.TemporaryFile() as tf:
            return tf

    with pytest.raises(UnserializableReturnValueError):
        not_picklable()

    try:
        not_picklable()
    except UnserializableReturnValueError as e:
        assert isinstance(e.__cause__, TypeError)


@pytest.mark.asyncio
async def test_cannot_cache_non_pickleable_value_async():
    """Tests that non-pickleable return values cannot be cached."""
    import tempfile

    @async_cache_value()
    async def not_picklable():
        with tempfile.TemporaryFile() as tf:
            return tf

    with pytest.raises(UnserializableReturnValueError):
        await not_picklable()

    try:
        await not_picklable()
    except UnserializableReturnValueError as e:
        assert isinstance(e.__cause__, TypeError)


def test_concurrent_calls_are_not_serialized():
    """Tests that concurrent calls to a cached function are not serialized (a
    function called with same arguments can run more than once).
    """
    mock = MagicMock(return_value="pickleable")

    @cache_value()
    def call_mock():
        time.sleep(0.1)
        return mock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futs = (executor.submit(call_mock), executor.submit(call_mock))
        concurrent.futures.wait(futs)

    result_1 = futs[0].result()
    result_2 = futs[1].result()
    assert result_1 == result_2
    assert mock.call_count == 2


@pytest.mark.asyncio
async def test_concurrent_calls_are_not_serialized_async():
    """Tests that concurrent calls to a cached function are not serialized (a
    function called with same arguments can run more than once).
    """
    mock = MagicMock(return_value="pickleable")

    @async_cache_value()
    async def call_mock():
        await asyncio.sleep(0.1)
        return mock()

    coro_1, coro_2 = (call_mock(), call_mock())
    result_1, result_2 = await asyncio.gather(coro_1, coro_2)

    assert result_1 == result_2
    assert mock.call_count == 2


def test_concurrent_calls_can_be_serialized():
    """Tests that concurrent calls to a cached function can be serialized."""
    mock = MagicMock(return_value="pickleable")

    @cache_value(allow_concurrent=False)
    def call_mock():
        time.sleep(0.1)
        return mock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futs = (executor.submit(call_mock), executor.submit(call_mock))
        concurrent.futures.wait(futs)

    result_1 = futs[0].result()
    result_2 = futs[1].result()
    assert result_1 == result_2
    mock.assert_called_once()


@pytest.mark.asyncio
async def test_concurrent_calls_can_be_serialized_async():
    """Tests that concurrent calls to a cached function can be serialized."""
    mock = MagicMock(return_value="pickleable")

    @async_cache_value(allow_concurrent=False)
    async def call_mock():
        await asyncio.sleep(0.1)
        return mock()

    coro_1, coro_2 = (call_mock(), call_mock())
    result_1, result_2 = await asyncio.gather(coro_1, coro_2)

    assert result_1 == result_2
    mock.assert_called_once()


def test_cache_value_cannot_wrap_coroutine():
    """Tests that wrapping a coroutine in ``cache_value`` raises a ``TypeError``."""
    with pytest.raises(TypeError):

        @cache_value()
        async def call():
            pass


def test_async_cache_value_must_wrap_coroutine():
    """Tests that wrapping a non-coroutine in ``async_cache_value`` raises a ``TypeError``."""
    with pytest.raises(TypeError):

        @async_cache_value()
        def call():
            pass


def test_value_expiration():
    """Tests that values expire."""
    mock = MagicMock(return_value="pickleable")

    @cache_value(expires_in=1)
    def call_mock():
        return mock()

    result_1 = call_mock()
    time.sleep(1.01)
    result_2 = call_mock()
    assert result_1 == result_2
    assert mock.call_count == 2


@pytest.mark.asyncio
async def test_value_expiration_async():
    """Tests that values expire."""
    mock = MagicMock(return_value="pickleable")

    @async_cache_value(expires_in=1)
    async def call_mock():
        return mock()

    result_1 = await call_mock()
    await asyncio.sleep(1.01)
    result_2 = await call_mock()
    assert result_1 == result_2
    assert mock.call_count == 2


def test_type_encoders():
    """Tests that types which are not natively hashable can be hashed by providing
    a callable that encodes the type.
    """
    from http.client import HTTPConnection

    mock = MagicMock(return_value="pickleable")

    @cache_value()
    def call_fail(conn: HTTPConnection):
        pass

    def encode_conn(conn: HTTPConnection):
        return f"{conn.host}:{conn.port}"

    type_encoders = {HTTPConnection: encode_conn}

    @cache_value(type_encoders=type_encoders)
    def call_success(conn: HTTPConnection):
        return mock(conn)

    conn = HTTPConnection("google.com")
    conn.connect()
    try:
        with pytest.raises(UnhashableParamError):
            call_fail(conn)
        result_1 = call_success(conn)
        result_2 = call_success(conn)
    finally:
        conn.close()

    assert result_1 == result_2
    mock.assert_called_once_with(conn)


@pytest.mark.asyncio
async def test_type_encoders_async():
    """Tests that types which are not natively hashable can be hashed by providing
    a callable that encodes the type.
    """
    from http.client import HTTPConnection

    mock = MagicMock(return_value="pickleable")

    @async_cache_value()
    async def call_fail(conn: HTTPConnection):
        pass

    def encode_conn(conn: HTTPConnection):
        return f"{conn.host}:{conn.port}"

    type_encoders = {HTTPConnection: encode_conn}

    @async_cache_value(type_encoders=type_encoders)
    async def call_success(conn: HTTPConnection):
        return mock(conn)

    conn = HTTPConnection("google.com")
    conn.connect()
    try:
        with pytest.raises(UnhashableParamError):
            await call_fail(conn)
        result_1 = await call_success(conn)
        result_2 = await call_success(conn)
    finally:
        conn.close()

    assert result_1 == result_2
    mock.assert_called_once_with(conn)


def test_factory_key():
    """Tests that the same storage factory can produce different storage instances
    by providing a factory key.
    """
    from cachex.factories import memory_storage_factory

    def mock_storage_factory(mock: MagicMock):
        def wrapper():
            mock()
            return memory_storage_factory()()

        return wrapper

    mock = MagicMock()
    cache_mock = MagicMock()

    @cache_value(storage_factory=mock_storage_factory(mock), factory_key="memcache_1")
    def echo_1(i: int) -> int:
        cache_mock(i)
        return i

    @cache_value(storage_factory=mock_storage_factory(mock), factory_key="memcache_2")
    def echo_2(i: int) -> int:
        cache_mock(i)
        return i

    @cache_value(storage_factory=mock_storage_factory(mock), factory_key="memcache_2")
    def echo_3(i: int) -> int:
        cache_mock(i)
        return i

    echo_1(1)
    echo_2(2)
    echo_3(3)

    assert cache_mock.call_count == 3
    assert mock.call_count == 2


@pytest.mark.asyncio
async def test_factory_key_async():
    """Tests that the same storage factory can produce different storage instances
    by providing a factory key.
    """
    from cachex.factories import async_memory_storage_factory

    def mock_storage_factory(mock: MagicMock):
        async def wrapper():
            mock()
            return async_memory_storage_factory()()

        return wrapper

    mock = MagicMock()
    cache_mock = MagicMock()

    @async_cache_value(
        storage_factory=mock_storage_factory(mock), factory_key="memcache_1"
    )
    async def echo_1(i: int) -> int:
        cache_mock(i)
        return i

    @async_cache_value(
        storage_factory=mock_storage_factory(mock), factory_key="memcache_2"
    )
    async def echo_2(i: int) -> int:
        cache_mock(i)
        return i

    @async_cache_value(
        storage_factory=mock_storage_factory(mock), factory_key="memcache_2"
    )
    async def echo_3(i: int) -> int:
        cache_mock(i)
        return i

    await echo_1(1)
    await echo_2(2)
    await echo_3(3)

    assert cache_mock.call_count == 3
    assert mock.call_count == 2
