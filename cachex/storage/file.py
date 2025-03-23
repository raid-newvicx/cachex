from __future__ import annotations

import os
import pickle
import shutil
import tempfile
import unicodedata
from pathlib import Path as SyncPath
from typing import Union, cast, TYPE_CHECKING

from anyio import Path as AsyncPath
from anyio.to_thread import run_sync

from cachex.storage.base import AsyncStorage, Storage, StoredValue


__all__ = ("FileStorage", "AsyncFileStorage")

if TYPE_CHECKING:
    from datetime import timedelta
    from os import PathLike
    from typing_extensions import TypeAlias

    Path: TypeAlias = Union[SyncPath, AsyncPath]


def _safe_file_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    return "".join(c if c.isalnum() else str(ord(c)) for c in name)


class _FileCommon:
    """Common config and commands between the sync and async storage types."""

    __slots__ = ("path", "key_prefix")

    def _path_from_key(self, key: str) -> Path:
        return self.path / _safe_file_name(key)  # type: ignore[attr-defined]

    @staticmethod
    def _load_from_path(path: SyncPath) -> StoredValue | None:
        try:
            data = path.read_bytes()
            return pickle.loads(data)
        except FileNotFoundError:
            return None

    @staticmethod
    async def _load_from_path_async(path: AsyncPath) -> StoredValue | None:
        try:
            data = await path.read_bytes()
            return pickle.loads(data)
        except FileNotFoundError:
            return None

    def _write(self, target_file: Path, stored_value: StoredValue) -> None:
        try:
            tmp_file_fd, tmp_file_name = tempfile.mkstemp(
                dir=self.path,  # type: ignore[attr-defined]
                prefix=f"{target_file.name}.tmp",  # type: ignore[attr-defined]
            )
            renamed = False
            try:
                try:
                    os.write(tmp_file_fd, pickle.dumps(stored_value))
                finally:
                    os.close(tmp_file_fd)

                os.replace(tmp_file_name, target_file)
                renamed = True
            finally:
                if not renamed:
                    os.unlink(tmp_file_name)
        except OSError:
            pass


class FileStorage(Storage, _FileCommon):
    """File based, synchronous key/value store."""

    def __init__(self, path: PathLike[str], key_prefix: str | None = None) -> None:
        key_prefix = key_prefix or "cachex"
        self.path = SyncPath(path).joinpath(key_prefix)
        self.key_prefix = key_prefix

    def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """

        self.path.mkdir(parents=True, exist_ok=True)
        path = cast("SyncPath", self._path_from_key(key))
        if isinstance(value, str):
            value = value.encode("utf-8")
        stored_value = StoredValue.new(data=value, expires_in=expires_in)
        self._write(path, stored_value)

    def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            None
        """
        path = cast("SyncPath", self._path_from_key(key))
        stored_value = self._load_from_path(path)

        if not stored_value:
            return None

        if stored_value.expired:
            path.unlink(missing_ok=True)
            return None

        return stored_value.data

    def delete(self, key: str) -> None:
        """Delete a value.

        If no such key exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        path = cast("SyncPath", self._path_from_key(key))
        path.unlink(missing_ok=True)

    def delete_all(self) -> None:
        """Delete all stored values.

        Note:
            This deletes and recreates :attr:`FileStorage.path`
        """
        shutil.rmtree(self.path)
        self.path.mkdir(exist_ok=True)


class AsyncFileStorage(AsyncStorage, _FileCommon):
    """File based, asynchronous key/value store."""

    def __init__(self, path: PathLike[str], key_prefix: str | None = None) -> None:
        key_prefix = key_prefix or "cachex"
        self.path = AsyncPath(path).joinpath(key_prefix)
        self.key_prefix = key_prefix

    async def set(
        self, key: str, value: str | bytes, expires_in: int | timedelta | None = None
    ) -> None:
        """Set a value.

        Args:
            key: Key to associate the value with
            value: Value to store
            expires_in: Time in seconds before the key is considered expired
        """

        await self.path.mkdir(parents=True, exist_ok=True)
        path = cast("AsyncPath", self._path_from_key(key))
        if isinstance(value, str):
            value = value.encode("utf-8")
        stored_value = StoredValue.new(data=value, expires_in=expires_in)
        await run_sync(self._write, path, stored_value)

    async def get(self, key: str) -> bytes | None:
        """Get a value.

        Args:
            key: Key associated with the value

        Returns:
            The value associated with ``key`` if it exists and is not expired, else
            None
        """
        path = cast("AsyncPath", self._path_from_key(key))
        stored_value = await self._load_from_path_async(path)

        if not stored_value:
            return None

        if stored_value.expired:
            await path.unlink(missing_ok=True)
            return None

        return stored_value.data

    async def delete(self, key: str) -> None:
        """Delete a value.

        If no such key exists, this is a no-op.

        Args:
            key: Key of the value to delete
        """
        path = cast("AsyncPath", self._path_from_key(key))
        await path.unlink(missing_ok=True)

    async def delete_all(self) -> None:
        """Delete all stored values.

        Note:
            This deletes and recreates :attr:`FileStorage.path`
        """
        await run_sync(shutil.rmtree, self.path)
        await self.path.mkdir(exist_ok=True)
