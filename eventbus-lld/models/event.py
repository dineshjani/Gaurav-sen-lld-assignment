from enum import Enum
from uuid import uuid4

class EventType(Enum):
    # Define your event types here
    TYPE_1 = "Type 1"
    TYPE_2 = "Type 2"
    TYPE_3 = "Type 3"

class Event:
    def __init__(self, publisher: str, event_type: EventType, description: str, creation_time: int):
        self.id = str(uuid4())
        self.publisher = publisher
        self.event_type = event_type
        self.description = description
        self.creation_time = creation_time

    def getId(self) -> str:
        return self.id

    def getPublisher(self) -> str:
        return self.publisher

    def getEventType(self) -> EventType:
        return self.event_type

    def getCreationTime(self) -> int:
        return self.creation_time

    def getDescription(self) -> str:
        return self.description
