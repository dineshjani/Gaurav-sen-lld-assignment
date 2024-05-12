from events.events import Event
from models.record import Record

class Update(Event):
    def __init__(self, element: Record, previous_value: Record, timestamp: int):
        super().__init__(element, timestamp)
        self.previous_value = previous_value
