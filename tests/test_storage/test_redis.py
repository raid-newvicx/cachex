import asyncio
import time

import pytest

from cachex import async_redis_storage_factory, redis_storage_factory


def test_set_get():
    """Test set and get operations."""
    storage = redis_storage_factory("redis://localhost:6379/", key_prefix="get_set")()
    val = b"test"
    storage.set("test", val)
    ret_val = storage.get("test")
    assert val == ret_val and id(val) != id(ret_val)
    storage.close()


@pytest.mark.asyncio
async def test_set_get_async():
    """Test set and get operations."""
    storage = async_redis_storage_factory(
        "redis://localhost:6379/", key_prefix="get_set_async"
    )()
    val = b"test"
    await storage.set("test", val)
    ret_val = await storage.get("test")
    assert val == ret_val and id(val) != id(ret_val)
    await storage.close()


def test_expires():
    """Test values expire."""
    storage = redis_storage_factory("redis://localhost:6379/", key_prefix="expires")()
    val = b"test"
    storage.set("test", val, expires_in=1)
    ret_val_1 = storage.get("test")
    time.sleep(1.01)
    ret_val_2 = storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    storage.close()


@pytest.mark.asyncio
async def test_expires_async():
    """Test values expire."""
    storage = async_redis_storage_factory(
        "redis://localhost:6379/", key_prefix="expires_async"
    )()
    val = b"test"
    await storage.set("test", val, expires_in=1)
    ret_val_1 = await storage.get("test")
    await asyncio.sleep(1.01)
    ret_val_2 = await storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    await storage.close()


def test_delete():
    """Test values can be deleted."""
    storage = redis_storage_factory("redis://localhost:6379/", key_prefix="delete")()
    val = b"test"
    storage.set("test", val)
    ret_val_1 = storage.get("test")
    storage.delete("test")
    ret_val_2 = storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    storage.close()


@pytest.mark.asyncio
async def test_delete_async():
    """Test values can be deleted."""
    storage = async_redis_storage_factory(
        "redis://localhost:6379/", key_prefix="delete_async"
    )()
    val = b"test"
    await storage.set("test", val)
    ret_val_1 = await storage.get("test")
    await storage.delete("test")
    ret_val_2 = await storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    await storage.close()


def test_delete_all():
    """Test all values can be deleted."""
    storage = redis_storage_factory(
        "redis://localhost:6379/", key_prefix="delete_all"
    )()
    val_1 = b"test_1"
    val_2 = b"test_2"
    storage.set("test_1", val_1)
    ret_val_1 = storage.get("test_1")
    storage.set("test_2", val_2)
    ret_val_2 = storage.get("test_2")
    storage.delete_all()
    ret_val_3 = storage.get("test_1")
    ret_val_4 = storage.get("test_2")
    assert val_1 == ret_val_1 and id(val_1) != id(ret_val_1)
    assert val_1 != ret_val_3 and ret_val_3 is None
    assert val_2 == ret_val_2 and id(val_2) != id(ret_val_2)
    assert val_2 != ret_val_4 and ret_val_4 is None
    storage.close()


@pytest.mark.asyncio
async def test_delete_all_async():
    """Test all values can be deleted."""
    storage = async_redis_storage_factory(
        "redis://localhost:6379/", key_prefix="delete_all_async"
    )()
    val_1 = b"test_1"
    val_2 = b"test_2"
    await storage.set("test_1", val_1)
    ret_val_1 = await storage.get("test_1")
    await storage.set("test_2", val_2)
    ret_val_2 = await storage.get("test_2")
    await storage.delete_all()
    ret_val_3 = await storage.get("test_1")
    ret_val_4 = await storage.get("test_2")
    assert val_1 == ret_val_1 and id(val_1) != id(ret_val_1)
    assert val_1 != ret_val_3 and ret_val_3 is None
    assert val_2 == ret_val_2 and id(val_2) != id(ret_val_2)
    assert val_2 != ret_val_4 and ret_val_4 is None
    await storage.close()
