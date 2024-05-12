from events.events import Event
from models.record import Record

class Write(Event):
    def __init__(self, element: Record, timestamp: int):
        super().__init__(element, timestamp)
