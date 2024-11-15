import threading
from typing import TypeVar

T = TypeVar('T')


class ThreadSafeList[T]:
    _list: list[T]
    _lock: threading.Lock

    def __init__(self):
        self._list = []
        self._lock = threading.Lock()

    def append(self, item):
        with self._lock:
            self._list.append(item)

    def remove(self, item):
        with self._lock:
            self._list.remove(item)

    def get(self, index):
        with self._lock:
            return self._list[index]

    def __len__(self):
        with self._lock:
            return len(self._list)

    def __iter__(self):
        with self._lock:
            return iter(self._list.copy())

    def __str__(self):
        with self._lock:
            return str(self._list)
