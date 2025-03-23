import asyncio
import shutil
import time

import pytest

from cachex.storage.file import AsyncFileStorage, FileStorage


def test_set_get():
    """Test set and get operations."""
    storage = FileStorage("./file_storage_test_set_get", key_prefix="test")
    val = b"test"
    storage.set("test", val)
    ret_val = storage.get("test")
    assert val == ret_val and id(val) != id(ret_val)
    shutil.rmtree("./file_storage_test_set_get")


@pytest.mark.asyncio
async def test_set_get_async():
    """Test set and get operations."""
    storage = AsyncFileStorage("./file_storage_test_set_get_async", key_prefix="test")
    val = b"test"
    await storage.set("test", val)
    ret_val = await storage.get("test")
    assert val == ret_val and id(val) != id(ret_val)
    shutil.rmtree("./file_storage_test_set_get_async")


def test_expires():
    """Test values expire."""
    storage = FileStorage("./file_storage_test_expires", key_prefix="test")
    val = b"test"
    storage.set("test", val, expires_in=1)
    ret_val_1 = storage.get("test")
    time.sleep(1.01)
    ret_val_2 = storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    shutil.rmtree("./file_storage_test_expires")


@pytest.mark.asyncio
async def test_expires_async():
    """Test values expire."""
    storage = AsyncFileStorage("./file_storage_test_expires_async", key_prefix="test")
    val = b"test"
    await storage.set("test", val, expires_in=1)
    ret_val_1 = await storage.get("test")
    await asyncio.sleep(1.01)
    ret_val_2 = await storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    shutil.rmtree("./file_storage_test_expires_async")


def test_delete():
    """Test values can be deleted."""
    storage = FileStorage("./file_storage_test_delete", key_prefix="test")
    val = b"test"
    storage.set("test", val)
    ret_val_1 = storage.get("test")
    storage.delete("test")
    ret_val_2 = storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    shutil.rmtree("./file_storage_test_delete")


@pytest.mark.asyncio
async def test_delete_async():
    """Test values can be deleted."""
    storage = AsyncFileStorage("./file_storage_test_delete_async", key_prefix="test")
    val = b"test"
    await storage.set("test", val)
    ret_val_1 = await storage.get("test")
    await storage.delete("test")
    ret_val_2 = await storage.get("test")
    assert val == ret_val_1 and id(val) != id(ret_val_1)
    assert val != ret_val_2 and ret_val_2 is None
    shutil.rmtree("./file_storage_test_delete_async")


def test_delete_all():
    """Test all values can be deleted."""
    storage = FileStorage("./file_storage_test_delete_all", key_prefix="test")
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
    shutil.rmtree("./file_storage_test_delete_all")


@pytest.mark.asyncio
async def test_delete_all_async():
    """Test all values can be deleted."""
    storage = AsyncFileStorage(
        "./file_storage_test_delete_all_async", key_prefix="test"
    )
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
    shutil.rmtree("./file_storage_test_delete_all_async")
