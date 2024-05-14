from models.event import Event
from models.event_type import EventType

class FailureEvent(Event):
    def __init__(self, event, throwable, failure_timestamp):
        super().__init__("dead-letter-queue", EventType.ERROR, str(throwable), failure_timestamp)
        self.event = event
        self.throwable = throwable

    def get_event(self):
        return self.event

    def get_throwable(self):
        return self.throwable
