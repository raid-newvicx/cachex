import asyncio
import time

import pytest
from aiomcache import Client as AsyncClient
from pymemcache.client.base import Client

from cachex import async_memcached_storage_factory, memcached_storage_factory


CONN = ("localhost", 11211)


def test_set_get():
    """Test set and get operations."""
    client = Client(CONN)
    storage = memcached_storage_factory(client)()
    val = b"test"
    storage.set(b"test", val)
    ret_val = storage.get(b"test")
    assert val == ret_val and id(val) != id(ret_val)
    storage.close()


@pytest.mark.asyncio
async def test_set_get_async():
    """Test set and get operations."""
    client = AsyncClient(*CONN)
    storage = async_memcached_storage_factory(client)()
    val = b"test"
    await storage.set(b"test", val)
    ret_val = await storage.get(b"test")
    assert val == ret_val and id(val) != id(ret_val)
    await storage.close()


def test_expires():
    """Test values expire."""
    client = Client(CONN)
    storage = memcached_storage_factory(client)()
    val = b"test"
    storage.set(b"test", val, expires_in=1)
    ret_val_1 = storage.get(b"test")
    time.sleep(1.01)
    ret_val_2 = storage.get(b"test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    storage.close()


@pytest.mark.asyncio
async def test_expires_async():
    """Test values expire."""
    client = AsyncClient(*CONN)
    storage = async_memcached_storage_factory(client)()
    val = b"test"
    await storage.set(b"test", val, expires_in=1)
    ret_val_1 = await storage.get(b"test")
    await asyncio.sleep(1.01)
    ret_val_2 = await storage.get(b"test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    await storage.close()


def test_delete():
    """Test values can be deleted."""
    client = Client(CONN)
    storage = memcached_storage_factory(client)()
    val = b"test"
    storage.set(b"test", val)
    ret_val_1 = storage.get(b"test")
    storage.delete(b"test")
    ret_val_2 = storage.get(b"test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    storage.close()


@pytest.mark.asyncio
async def test_delete_async():
    """Test values can be deleted."""
    client = AsyncClient(*CONN)
    storage = async_memcached_storage_factory(client)()
    val = b"test"
    await storage.set(b"test", val)
    ret_val_1 = await storage.get(b"test")
    await storage.delete(b"test")
    ret_val_2 = await storage.get(b"test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    await storage.close()


def test_delete_all():
    """Test all values can be deleted."""
    client = Client(CONN)
    storage = memcached_storage_factory(client)()
    val_1 = b"test_1"
    val_2 = b"test_2"
    storage.set(b"test_1", val_1)
    ret_val_1 = storage.get(b"test_1")
    storage.set(b"test_2", val_2)
    ret_val_2 = storage.get(b"test_2")
    storage.delete_all()
    ret_val_3 = storage.get(b"test_1")
    ret_val_4 = storage.get(b"test_2")
    assert val_1 == ret_val_1 and id(val_1) != id(ret_val_1)
    assert val_1 != ret_val_3 and ret_val_3 is None
    assert val_2 == ret_val_2 and id(val_2) != id(ret_val_2)
    assert val_2 != ret_val_4 and ret_val_4 is None
    storage.close()


@pytest.mark.asyncio
async def test_delete_all_async():
    """Test all values can be deleted."""
    client = AsyncClient(*CONN)
    storage = async_memcached_storage_factory(client)()
    val_1 = b"test_1"
    val_2 = b"test_2"
    await storage.set(b"test_1", val_1)
    ret_val_1 = await storage.get(b"test_1")
    await storage.set(b"test_2", val_2)
    ret_val_2 = await storage.get(b"test_2")
    await storage.delete_all()
    ret_val_3 = await storage.get(b"test_1")
    ret_val_4 = await storage.get(b"test_2")
    assert val_1 == ret_val_1 and id(val_1) != id(ret_val_1)
    assert val_1 != ret_val_3 and ret_val_3 is None
    assert val_2 == ret_val_2 and id(val_2) != id(ret_val_2)
    assert val_2 != ret_val_4 and ret_val_4 is None
    await storage.close()
