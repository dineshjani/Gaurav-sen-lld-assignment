from events.events import Event
from models.record import Record
from enum import Enum

from enum import Enum

class Type(Enum):
    EXPIRY = 'EXPIRY'
    REPLACEMENT = 'REPLACEMENT'

class Eviction(Event):
    def __init__(self, element: Record, eviction_type: Type, timestamp: int):
        super().__init__(element, timestamp)
        self.type = eviction_type

    def get_type(self):
        return self.type

    class Type(Enum):
        EXPIRY = 1
        REPLACEMENT = 2

    def __str__(self):
        return f"Eviction(type={self.type}, {super().__str__()})"
