import unittest
import asyncio
from datetime import datetime, timedelta
import uuid
import random
from models.access_details import AccessDetails
from models.fetch_algorithem import FetchAlgorithm
from models.eviction_algorithem import EvictionAlgorithm
from models.record import Record
from models.timer import Timer
from events.events import Event
from events.eviction import Type
from events.eviction import Eviction
from cache import Cache
from data_source import DataSource
from events.load import Load
from events.write import Write
from events.update import Update
from cache_builder import CacheBuilder
import time

class SettableTimer:
    def __init__(self):
        self.time = -1

    def get_current_time(self):
        return time.time() if self.time == -1 else self.time

    def set_time(self, time):
        self.time = time
dataMap = {
    "PROFILE_MUMBAI_ENGINEER": "violet",
    "PROFILE_HYDERABAD_ENGINEER": "blue"
}
    
class DummyDataSource(DataSource):
    async def load(self, key):
        if key in dataMap:
            return dataMap[key]
        else:
            raise ValueError("Key not found")

    async def persist(self, key, value, timestamp):
        dataMap[key] = value

class WriteBackDataSource(DataSource):
    async def load(self, key):
        if key in dataMap:
            return dataMap[key]
        else:
            raise ValueError("Key not found")
        
    async def persist(self, key, value, timestamp):
        dataMap[key] = value

        
class TestCache(unittest.TestCase):
    dataSource = DummyDataSource()
    writeBackDataSource = WriteBackDataSource()
    PROFILE_MUMBAI_ENGINEER = "profile_mumbai_engineer"
    PROFILE_HYDERABAD_ENGINEER = "profile_hyderabad_engineer"

    def setUp(self):
        self.data_map = {}
        self.write_operations = []

        self.data_map[self.PROFILE_MUMBAI_ENGINEER] = "violet"
        self.data_map[self.PROFILE_HYDERABAD_ENGINEER] = "blue"
        import asyncio

    def accept_write(self):
        write = self.write_operations.pop(0)
        write.set_result(None)

    def test_cache_construction_without_data_source_failure(self):
        builder = CacheBuilder()
        with self.assertRaises(ValueError):
            builder.build()

    def test_cache_default_behavior(self):
        cache = CacheBuilder().data_source(DataSource).build()
        self.assertIsNotNone(cache)
        self.assertEqual(cache.get(self.PROFILE_MUMBAI_ENGINEER).result(), "violet")
        self.assertEqual(cache.get("random").result(), True)
        self.assertEqual(cache.set(self.PROFILE_MUMBAI_ENGINEER, "brown").result(), None)
        self.assertEqual(cache.get(self.PROFILE_MUMBAI_ENGINEER).result(), "brown")
        self.assertEqual(len(cache.get_event_queue()), 3)
        self.assertIsInstance(cache.get_event_queue()[0], Load)
        self.assertIsInstance(cache.get_event_queue()[1], Update)
        self.assertIsInstance(cache.get_event_queue()[2], Write)

    def test_eviction_lru(self):
        maximum_size = 2
        cache = Cache(maximum_size=maximum_size, eviction_algorithm=EvictionAlgorithm.LRU, fetch_algorithm=FetchAlgorithm.WRITE_BACK, dataSource=self.writeBackDataSource)
        cache.get(self.PROFILE_MUMBAI_ENGINEER)
        for i in range(maximum_size):
            cache.set("key{}".format(i), "value{}".format(i))
        self.assertEqual(len(cache.get_event_queue()), 2)
        self.assertIsInstance(cache.get_event_queue()[0], Load)
        self.assertIsInstance(cache.get_event_queue()[1], Eviction)
        eviction_event = cache.get_event_queue()[1]
        self.assertEqual(eviction_event.type, Eviction.Type.REPLACEMENT)
        self.assertEqual(eviction_event.element.key, self.PROFILE_MUMBAI_ENGINEER)
        cache.get_event_queue().clear()
        permutation = list(range(maximum_size))
        random.shuffle(permutation)
        for index in permutation:
            cache.get("key{}".format(index))
        for i in range(maximum_size):
            cache.set("random{}".format(permutation[i]), "random_value")
            self.assertIsInstance(cache.get_event_queue()[i], Eviction)
            eviction = cache.get_event_queue()[i]
            self.assertEqual(eviction.type, Eviction.Type.REPLACEMENT)
            self.assertEqual(eviction.element.key, "key{}".format(permutation[i]))

    def test_eviction_lfu(self):
        maximum_size = 2
        cache = Cache(maximum_size=maximum_size, eviction_algorithm=EvictionAlgorithm.LFU, fetch_algorithm=FetchAlgorithm.WRITE_BACK, dataSource=self.writeBackDataSource)
        cache.get(self.PROFILE_MUMBAI_ENGINEER)
        for i in range(maximum_size):
            cache.set("key{}".format(i), "value{}".format(i))
        self.assertEqual(len(cache.get_event_queue()), 2)
        self.assertIsInstance(cache.get_event_queue()[0], Load)
        self.assertIsInstance(cache.get_event_queue()[1], Eviction)
        eviction_event = cache.get_event_queue()[1]
        self.assertEqual(eviction_event.type, Eviction.Type.REPLACEMENT)
        self.assertEqual(eviction_event.element.key, "key0")
        for i in range(maximum_size):
            self.accept_write()
        permutation = list(range(maximum_size))
        random.shuffle(permutation)
        for index in permutation:
            for i in range(index + 1):
                cache.get("key{}".format(index))
        cache.get_event_queue().clear()
        for i in range(maximum_size):
            cache.set("random{}".format(i), "random_value")
            self.accept_write()
            for j in range(maximum_size + 1):
                cache.get("random{}".format(i))
            self.assertEqual(cache.get_event_queue()[i * 2].__class__.__name__, Eviction.__name__)
            self.assertEqual(cache.get_event_queue()[i * 2 + 1].__class__.__name__, Write.__name__)
            eviction = cache.get_event_queue()[i * 2]
            self.assertEqual(eviction.type, Eviction.Type.REPLACEMENT)
            self.assertEqual(eviction.element.key, "key{}".format(i))

    def test_expiry_on_get(self):
        timer = SettableTimer()
        start_time = datetime.now()
        cache = Cache(timer=timer, dataSource=self.dataSource, expiry_time=timedelta(seconds=10))
        timer.set_time(start_time)
        cache.get(self.PROFILE_MUMBAI_ENGINEER).result()
        self.assertEqual(len(cache.get_event_queue()), 1)
        self.assertIsInstance(cache.get_event_queue()[0], Load)
        self.assertEqual(cache.get_event_queue()[0].element.key, self.PROFILE_MUMBAI_ENGINEER)
        timer.set_time(start_time + timedelta(seconds=10) + timedelta(milliseconds=1))
        cache.get(self.PROFILE_MUMBAI_ENGINEER).result()
        self.assertEqual(len(cache.get_event_queue()), 3)
        self.assertIsInstance(cache.get_event_queue()[1], Eviction)
        self.assertIsInstance(cache.get_event_queue()[2], Load)
        eviction = cache.get_event_queue()[1]
        self.assertEqual(eviction.type, Eviction.Type.EXPIRY)
        self.assertEqual(eviction.element.key, self.PROFILE_MUMBAI_ENGINEER)

    def test_expiry_on_set(self):
        timer = SettableTimer()
        start_time = datetime.now()
        cache = Cache(timer=timer, dataSource=self.dataSource, expiry_time=timedelta(seconds=10))
        timer.set_time(start_time)
        cache.get(self.PROFILE_MUMBAI_ENGINEER).result()
        self.assertEqual(len(cache.get_event_queue()), 1)
        self.assertIsInstance(cache.get_event_queue()[0], Load)
        self.assertEqual(cache.get_event_queue()[0].element.key, self.PROFILE_MUMBAI_ENGINEER)
        timer.set_time(start_time + timedelta(seconds=10) + timedelta(milliseconds=1))
        cache.set(self.PROFILE_MUMBAI_ENGINEER, "blue").result()
        self.assertEqual(len(cache.get_event_queue()), 3)
        self.assertIsInstance(cache.get_event_queue()[1], Eviction)
        self.assertIsInstance(cache.get_event_queue()[2], Write)
        eviction = cache.get_event_queue()[1]
        self.assertEqual(eviction.type, Eviction.Type.EXPIRY)
        self.assertEqual(eviction.element.key, self.PROFILE_MUMBAI_ENGINEER)

    def test_expiry_on_eviction(self):
        timer = SettableTimer()
        start_time = datetime.now()
        cache = Cache(maximum_size=2, timer=timer, dataSource=self.dataSource, expiry_time=timedelta(seconds=10))
        timer.set_time(start_time)
        cache.get(self.PROFILE_MUMBAI_ENGINEER).result()
        cache.get(self.PROFILE_HYDERABAD_ENGINEER).result()
        timer.set_time(start_time + timedelta(seconds=10) + timedelta(milliseconds=1))
        cache.set("randomKey", "randomValue").result()
        self.assertEqual(len(cache.get_event_queue()), 5)
        self.assertIsInstance(cache.get_event_queue()[2], Eviction)
        self.assertIsInstance(cache.get_event_queue()[3], Eviction)
        self.assertIsInstance(cache.get_event_queue()[4], Write)
        eviction1 = cache.get_event_queue()[2]
        self.assertEqual(eviction1.type, Eviction.Type.EXPIRY)
        self.assertEqual(eviction1.element.key, self.PROFILE_MUMBAI_ENGINEER)
        eviction2 = cache.get_event_queue()[3]
        self.assertEqual(eviction2.type, Eviction.Type.EXPIRY)
        self.assertEqual(eviction2.element.key, self.PROFILE_HYDERABAD_ENGINEER)

    def test_fetching_write_back(self):
        cache = Cache(maximum_size=1, dataSource=self.writeBackDataSource, fetch_algorithm=FetchAlgorithm.WRITE_BACK)
        cache.set("randomKey", "randomValue").result()
        self.assertEqual(len(cache.get_event_queue()), 0)
        self.assertIsNone(self.data_map.get("randomValue"))
        self.accept_write()

    def test_fetching_write_through(self):
        cache = Cache(dataSource=self.dataSource, fetch_algorithm=FetchAlgorithm.WRITE_THROUGH)
        cache.set("randomKey", "randomValue").result()
        self.assertEqual(len(cache.get_event_queue()), 1)
        self.assertIsInstance(cache.get_event_queue()[0], Write)
        self.assertEqual(self.data_map.get("randomKey"), "randomValue")

    def test_eager_loading(self):
        eagerly_load = {self.PROFILE_MUMBAI_ENGINEER, self.PROFILE_HYDERABAD_ENGINEER}
        cache = Cache(load_keys_on_start=eagerly_load, dataSource=self.dataSource)
        self.assertEqual(len(cache.get_event_queue()), 2)
        self.assertIsInstance(cache.get_event_queue()[0], Load)
        self.assertIsInstance(cache.get_event_queue()[1], Load)
        cache.get_event_queue().clear()
        self.data_map.clear()
        self.assertEqual(cache.get(self.PROFILE_MUMBAI_ENGINEER).result(), "violet")
        self.assertEqual(cache.get(self.PROFILE_HYDERABAD_ENGINEER).result(), "blue")
        self.assertEqual(len(cache.get_event_queue()), 0)

    def test_race_conditions(self):
        cache = Cache(pool_size=8, dataSource=self.dataSource)
        cache_entries = {}
        number_of_entries = 100
        number_of_values = 1000
        key_list = [str(uuid.uuid4()) for _ in range(number_of_entries)]
        inverse_mapping = {key: index for index, key in enumerate(key_list)}
        for entry in range(number_of_entries):
            key = key_list[entry]
            cache_entries[key] = [str(uuid.uuid4()) for _ in range(number_of_values)]
            first_value = cache_entries[key][0]
            self.data_map[key] = first_value
            for value in range(1, number_of_values):
                cache_entries[key].append(str(uuid.uuid4()))
        futures = []
        queries = []
        updates = [0] * number_of_entries
        for _ in range(1000000):
            index = random.randint(0, number_of_entries - 1)
            key = key_list[index]
            if random.random() <= 0.05:
                if updates[index] - 1 < number_of_entries:
                    updates[index] += 1
                cache.set(key, cache_entries[key][updates[index] + 1])
            else:
                queries.append(key)
                futures.append(cache.get(key))
        results = asyncio.gather(*futures)
        current_indexes = [0] * number_of_entries
        for value in results.result():
            key = queries.pop(0)
            possible_values_for_key = cache_entries[key]
            current_value = current_indexes[inverse_mapping[key]]
            self.assertEqual(possible_values_for_key[current_value], value)
            if possible_values_for_key[current_value] != value:
                offset = 1
                while current_value + offset < number_of_values and possible_values_for_key[current_value + offset] != value:
                    offset += 1
                self.assertEqual(current_value + offset, number_of_values)
                current_indexes[inverse_mapping[key]] += offset
