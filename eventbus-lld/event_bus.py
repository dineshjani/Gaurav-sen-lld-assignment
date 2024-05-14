from typing import Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from queue import PriorityQueue
from collections import defaultdict
from enum import Enum
import time
import asyncio
from models.subscription import Subscription
from models.event import Event
from models.event_type import EventType
from models.exception import RetryLimitExceededException,UnsubscribedPollException
from utils.timer import Timer


from concurrent.futures import ThreadPoolExecutor, Future
from collections import defaultdict
from functools import partial
from typing import Callable, Dict, Any, List


class KeyedExecutor:
    def __init__(self):
        self.executor_map: Dict[str, ThreadPoolExecutor] = defaultdict(ThreadPoolExecutor)

    def get_thread_for(self, key: str, task: Callable[..., Any]) -> Future:
        executor = self.executor_map[key]
        return executor.submit(task)


class EventType:
    PRIORITY = "PRIORITY"
    LOGGING = "LOGGING"
    ERROR = "ERROR"


class FailureEvent(Event):
    def __init__(self, event: Event, throwable: Exception, failure_timestamp: int):
        super().__init__("dead-letter-queue", EventType.ERROR, throwable.__class__.__name__, failure_timestamp)
        self.event = event
        self.throwable = throwable



class EventBus:
    def __init__(self, event_executor: KeyedExecutor, broadcast_executor: KeyedExecutor):
        self.topics: Dict[str, List[Event]] = defaultdict(list)
        self.event_indexes: Dict[str, Dict[int, int]] = defaultdict(dict)
        self.event_timestamps: Dict[str, Dict[int, int]] = defaultdict(dict)
        self.pull_subscriptions: Dict[str, Dict[str, Subscription]] = defaultdict(dict)
        self.push_subscriptions: Dict[str, Dict[str, Subscription]] = defaultdict(dict)
        self.event_executor = event_executor
        self.broadcast_executor = broadcast_executor
        self.dead_letter_queue = None
        self.timer = Timer()

    def set_dead_letter_queue(self, dead_letter_queue: "EventBus") -> None:
        self.dead_letter_queue = dead_letter_queue

    def publish(self, topic: str, event: Event) -> Future:
        return self.event_executor.get_thread_for(topic, partial(self.publish_to_bus, topic, event))

    def publish_to_bus(self, topic: str, event: Event) -> None:
        if topic in self.event_indexes and event.id in self.event_indexes[topic]:
            return
        self.topics.setdefault(topic, []).append(event)
        index = len(self.topics[topic]) - 1
        self.event_indexes.setdefault(topic, {})[event.id] = index
        timestamp = self.timer.get_current_time()
        self.event_timestamps.setdefault(topic, {})[timestamp] = event.id
        self.notify_push_subscribers(topic, event)

    def notify_push_subscribers(self, topic: str, event: Event) -> None:
        if topic not in self.push_subscriptions:
            return
        subscribers = self.push_subscriptions[topic]
        notifications = [
            self.execute_event_handler(event, subscription)
            for subscription in subscribers.values()
            if subscription.get_precondition()(event)
        ]
        for notification in notifications:
            notification.result()  # Wait for notification to complete

    def execute_event_handler(self, event: Event, subscription: Subscription) -> Future:
        return self.broadcast_executor.get_thread_for(
            subscription.get_topic() + subscription.get_subscriber(),
            partial(
                self.do_with_retry, event, subscription.get_event_handler(),
                1, subscription.number_of_retries
            ).__wrapped__  # unwrap partial to get the original function
        )

    def do_with_retry(self, event: Event, task: Callable[[Event], Future],
                      cool_down_interval_in_millis: int, remaining_tries: int) -> Future:
        future = task(event)

        def handle_exception(_, throwable: Exception):
            if throwable is not None:
                if remaining_tries == 1:
                    raise RetryLimitExceededException(throwable)
                try:
                    import time
                    time.sleep(cool_down_interval_in_millis / 1000)
                except Exception as e:
                    raise Exception(e)
                return self.do_with_retry(event, task, max(cool_down_interval_in_millis * 2, 10), remaining_tries - 1)

        future.add_done_callback(partial(handle_exception, event))
        return future

    def poll(self, topic: str, subscriber: str) -> Future:
        return self.event_executor.get_thread_for(topic + subscriber, partial(self.poll_bus, topic, subscriber))

    def poll_bus(self, topic: str, subscriber: str) -> Event:
        subscription = self.pull_subscriptions.get(topic, {}).get(subscriber)
        if not subscription:
            raise UnsubscribedPollException()
        index = subscription.get_current_index()
        topic_events = self.topics.get(topic, [])
        while index < len(topic_events):
            event = topic_events[index]
            if subscription.get_precondition()(event):
                subscription.set_current_index(index + 1)
                return event
            index += 1
        return None

    def subscribe_to_events_after(self, topic: str, subscriber: str, timestamp: int) -> Future:
        return self.event_executor.get_thread_for(topic + subscriber, partial(self.move_index_at_timestamp, topic, subscriber, timestamp))

    def move_index_at_timestamp(self, topic: str, subscriber: str, timestamp: int) -> None:
        closest_event_after = next((ts for ts in sorted(self.event_timestamps[topic]) if ts > timestamp), None)
        if closest_event_after is None:
            index = len(self.event_indexes[topic])
        else:
            event_id = self.event_timestamps[topic][closest_event_after]
            index = self.event_indexes[topic][event_id]
        self.pull_subscriptions[topic][subscriber].set_current_index(index)

    def subscribe_to_events_after_event_id(self, topic: str, subscriber: str, event_id: int) -> Future:
        return self.event_executor.get_thread_for(topic + subscriber, partial(self.move_index_after_event, topic, subscriber, event_id))

    def move_index_after_event(self, topic: str, subscriber: str, event_id: int) -> None:
        if event_id is None:
            index = 0
        else:
            index = self.event_indexes[topic][event_id] + 1
        self.pull_subscriptions[topic][subscriber].set_current_index(index)

    def subscribe_for_push(self, topic: str, subscriber: str, precondition: Callable[[Event], bool],
                           handler: Callable[[Event], Future], number_of_retries: int) -> Future:
        return self.event_executor.get_thread_for(topic + subscriber,
                                                  partial(self.subscribe_for_push_events, topic, subscriber, precondition, handler, number_of_retries))

    def subscribe_for_push_events(self, topic: str, subscriber: str, precondition: Callable[[Event], bool],
                                  handler: Callable[[Event], Future], number_of_retries: int) -> None:
        self.add_subscriber(self.push_subscriptions, subscriber, precondition, topic, handler, number_of_retries)

    def add_subscriber(self, subscriptions: Dict[str, Dict[str, Subscription]], subscriber: str,
                       precondition: Callable[[Event], bool], topic: str,
                       handler: Callable[[Event], Future], number_of_retries: int) -> None:
        subscriptions.setdefault(topic, {})
        subscription = Subscription(topic, subscriber, precondition, handler, number_of_retries)
        subscription.set_current_index(len(self.topics.get(topic, [])))
        subscriptions[topic][subscriber] = subscription

    def subscribe_for_pull(self, topic: str, subscriber: str, precondition: Callable[[Event], bool]) -> Future:
        return self.event_executor.get_thread_for(topic + subscriber,
                                                  partial(self.subscribe_for_pull_events, topic, subscriber, precondition))

    def subscribe_for_pull_events(self, topic: str, subscriber: str, precondition: Callable[[Event], bool]) -> None:
        self.add_subscriber(self.pull_subscriptions, subscriber, precondition, topic, None, 0)

    def unsubscribe(self, topic: str, subscriber: str) -> Future:
        return self.event_executor.get_thread_for(topic + subscriber,
                                                  partial(self.unsubscribe_from_topic, topic, subscriber))

    def unsubscribe_from_topic(self, topic: str, subscriber: str) -> None:
        self.push_subscriptions.get(topic, {}).pop(subscriber, None)
        self.pull_subscriptions.get(topic, {}).pop(subscriber, None)


class Timer:
    @staticmethod
    def get_current_time() -> int:
        import time
        return int(time.time() * 1000)
