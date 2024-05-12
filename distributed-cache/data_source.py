from typing import TypeVar, Generic
from concurrent.futures import Future
from abc import ABC, abstractmethod

TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


class DataSource(Generic[TKey, TValue], ABC):
    @abstractmethod
    def load(self, key: TKey) -> Future:
        pass

    @abstractmethod
    def persist(self, key: TKey, value: TValue, timestamp: int) -> Future:
        pass
