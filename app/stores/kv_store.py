from __future__ import annotations

import time
from typing import Callable
from typing import Protocol


class KVStore(Protocol):
    def set(self, key: str, value: str) -> bool:
        ...

    def get(self, key: str) -> str | None:
        ...

    def delete(self, key: str) -> bool:
        ...

    def exists(self, key: str) -> bool:
        ...

    def ttl(self, key: str) -> int:
        ...


class InMemoryKVStore:
    """In-memory store with TTL metadata support for stage-2 reads."""

    def __init__(self, time_fn: Callable[[], float] | None = None) -> None:
        self._data: dict[str, str] = {}
        self._expires_at: dict[str, float] = {}
        self._time_fn = time_fn or time.time

    def set(self, key: str, value: str) -> bool:
        self._data[key] = value
        self._expires_at.pop(key, None)
        return True

    def get(self, key: str) -> str | None:
        self._purge_if_expired(key)
        return self._data.get(key)

    def delete(self, key: str) -> bool:
        self._purge_if_expired(key)
        self._expires_at.pop(key, None)
        return self._data.pop(key, None) is not None

    def exists(self, key: str) -> bool:
        self._purge_if_expired(key)
        return key in self._data

    def expire(self, key: str, seconds: float) -> bool:
        self._purge_if_expired(key)
        if key not in self._data:
            return False
        self._expires_at[key] = self._time_fn() + seconds
        return True

    def ttl(self, key: str) -> int:
        self._purge_if_expired(key)
        if key not in self._data:
            return -2

        expires_at = self._expires_at.get(key)
        if expires_at is None:
            return -1

        remaining_seconds = expires_at - self._time_fn()
        if remaining_seconds <= 0:
            self._purge_if_expired(key)
            return -2

        return int(remaining_seconds)

    def _purge_if_expired(self, key: str) -> None:
        expires_at = self._expires_at.get(key)
        if expires_at is None:
            return

        if expires_at <= self._time_fn():
            self._expires_at.pop(key, None)
            self._data.pop(key, None)
