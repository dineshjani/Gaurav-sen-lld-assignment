from concurrent.futures import Future
from typing import Callable
from models.event import Event

import threading

class LongAdder:
    def __init__(self):
        self._lock = threading.Lock()
        self._value = 0

    def add(self, value):
        with self._lock:
            self._value += value

    def reset(self):
        with self._lock:
            self._value = 0

    def value(self):
        with self._lock:
            return self._value

class Subscription:
    def __init__(self, topic: str, subscriber: str, precondition: Callable[[Event], bool],
                 event_handler: Callable[[Event], Future], number_of_retries: int):
        self.topic = topic
        self.subscriber = subscriber
        self.precondition = precondition
        self.event_handler = event_handler
        self.number_of_retries = number_of_retries
        self.current_index = 0

    def get_topic(self) -> str:
        return self.topic

    def get_subscriber(self) -> str:
        return self.subscriber

    def get_precondition(self) -> Callable[[Event], bool]:
        return self.precondition

    def get_event_handler(self) -> Callable[[Event], Future]:
        return self.event_handler

    def get_current_index(self) -> int:
        return self.current_index

    def set_current_index(self, offset: int) -> None:
        self.current_index = offset

    def get_number_of_retries(self) -> int:
        return self.number_of_retries
