from enum import Enum
from datetime import timedelta
from typing import Set, TypeVar, Callable
from cache import Cache
from models.fetch_algorithem import FetchAlgorithm
from models.eviction_algorithem import EvictionAlgorithm
from models.timer import Timer
from data_source import DataSource

TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


class CacheBuilder:
    def __init__(self):
        self.maximum_size = 1000
        self.expiry_time = timedelta(days=365)
        self.fetch_algorithm = FetchAlgorithm.WRITE_THROUGH
        self.eviction_algorithm = EvictionAlgorithm.LRU
        self.on_start_load = set()
        self.timer = Timer()
        self.pool_size = 1

    def maximum_size(self, maximum_size: int) -> 'CacheBuilder':
        self.maximum_size = maximum_size
        return self

    def expiry_time(self, expiry_time: timedelta) -> 'CacheBuilder':
        self.expiry_time = expiry_time
        return self

    def load_keys_on_start(self, keys: Set[TKey]) -> 'CacheBuilder':
        self.on_start_load.update(keys)
        return self

    def eviction_algorithm(self, eviction_algorithm: EvictionAlgorithm) -> 'CacheBuilder':
        self.eviction_algorithm = eviction_algorithm
        return self

    def fetch_algorithm(self, fetch_algorithm: FetchAlgorithm) -> 'CacheBuilder':
        self.fetch_algorithm = fetch_algorithm
        return self

    def data_source(self, data_source: DataSource) -> 'CacheBuilder':
        self.data_source = data_source
        return self

    def timer(self, timer: Timer) -> 'CacheBuilder':
        self.timer = timer
        return self

    def pool_size(self, pool_size: int) -> 'CacheBuilder':
        self.pool_size = pool_size
        return self

    def build(self) -> 'Cache':
        if self.data_source is None:
            raise ValueError("No data source configured")
        return Cache(
            maximum_size=self.maximum_size,
            expiry_time=self.expiry_time,
            fetch_algorithm=self.fetch_algorithm,
            eviction_algorithm=self.eviction_algorithm,
            data_source=self.data_source,
            keys_to_eagerly_load=self.on_start_load,
            timer=self.timer,
            pool_size=self.pool_size
        )
