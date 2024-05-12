import asyncio
import heapq
from enum import Enum
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Set, Tuple, TypeVar
from models.access_details import AccessDetails
from models.fetch_algorithem import FetchAlgorithm
from models.eviction_algorithem import EvictionAlgorithm
from models.record import Record
from models.timer import Timer
from events.events import Event
from events.eviction import Type
from queue import PriorityQueue
from heapq import heappop, heappush
from threading import Lock
from concurrent.futures import ThreadPoolExecutor


TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


class Cache:
    def __init__(self, maximum_size, expiry_time, fetch_algorithm, eviction_algorithm, data_source, keys_to_eagerly_load, timer, pool_size):
        self.maximum_size = maximum_size
        self.expiry_time = expiry_time
        self.fetch_algorithm = fetch_algorithm
        self.timer = timer
        self.cache = {}
        self.event_queue = []
        self.data_source = data_source
        self.executor_pool = [ThreadPoolExecutor(1) for _ in range(pool_size)]
        self.priority_queue_lock = Lock()
        self.priority_queue = []
        self.expiry_queue = defaultdict(set)

        for key in keys_to_eagerly_load:
            asyncio.run(self.add_to_cache(key, self.load_from_db(data_source, key)))

    async def add_to_cache(self, key, value_future):
        await self.manage_entries()
        record_future = asyncio.create_task(value_future)
        self.cache[key] = record_future
        record = await record_future
        return record

    async def manage_entries(self):
        current_time = self.timer.get_current_time()

        while self.expiry_queue and self.has_expired(next(iter(self.expiry_queue.keys()))):
            keys = self.expiry_queue.popitem()[1]
            for key in keys:
                expired_record = await self.cache.pop(key)
                self.remove_from_priority_queue(key)
                self.event_queue.append((Event.Eviction, expired_record, EvictionAlgorithm.LRU, current_time))

        while len(self.cache) >= self.maximum_size:
            _, key = self.pop_priority_queue()
            lowest_priority_record = await self.cache.pop(key)
            self.expiry_queue[lowest_priority_record.get_insertion_time()].remove(key)
            self.event_queue.append((Event.Eviction, lowest_priority_record, EvictionAlgorithm.LRU, current_time))

    async def get(self, key):
        return await self.get_from_cache(key)

    async def set(self, key, value):
        return await self.set_in_cache(key, value)

    async def get_from_cache(self, key):
        if key not in self.cache:
            result = await self.add_to_cache(key, self.load_from_db(self.data_source, key))
        else:
            result = self.cache[key]
            if self.has_expired(await result):
                result = await self.add_to_cache(key, self.load_from_db(self.data_source, key))
        await self.remove_from_priority_queue(key)
        record = await result
        updated_access_details = record.get_access_details().update(self.timer.get_current_time())
        await self.add_to_priority_queue(updated_access_details, key)
        return record.get_value()

    async def set_in_cache(self, key, value):
        if key in self.cache:
            old_record = await self.cache.pop(key)
            await self.remove_from_priority_queue(key)
            if self.has_expired(old_record):
                self.event_queue.append((Event.Eviction, old_record, EvictionAlgorithm.LRU, self.timer.get_current_time()))
            else:
                self.event_queue.append((Event.Update, Record(key, value, self.timer.get_current_time()), old_record, self.timer.get_current_time()))
        record = await self.add_to_cache(key, value)
        if self.fetch_algorithm == FetchAlgorithm.WRITE_THROUGH:
            await self.persist_record(record)
        return None

    async def persist_record(self, record):
        await self.data_source.persist(record.get_key(), record.get_value(), record.get_insertion_time())
        self.event_queue.append((Event.Write, record, self.timer.get_current_time()))

    def has_expired(self, record):
        return (datetime.utcnow() - datetime.utcfromtimestamp(record.get_insertion_time() / 1e9)) > self.expiry_time

    async def load_from_db(self, data_source, key):
        value = await data_source.load(key)
        if not value[1]:
            self.event_queue.append((Event.Load, Record(key, value[0], self.timer.get_current_time()), self.timer.get_current_time()))
        return value[0]

    async def remove_from_priority_queue(self, key):
        with self.priority_queue_lock:
            self.priority_queue = [(priority, k) for priority, k in self.priority_queue if k != key]

    async def add_to_priority_queue(self, access_details, key):
        with self.priority_queue_lock:
            heappush(self.priority_queue, (access_details.get_last_access_time(), key))

    async def pop_priority_queue(self):
        with self.priority_queue_lock:
            return heappop(self.priority_queue)
