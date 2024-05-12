from models.record import Record
import uuid

class Event:
    def __init__(self, element: Record, timestamp: int):
        self.id = str(uuid.uuid4())
        self.element = element
        self.timestamp = timestamp

    def get_id(self):
        return self.id

    def get_element(self):
        return self.element

    def get_timestamp(self):
        return self.timestamp

    def __str__(self):
        return f"{self.__class__.__name__}(element={self.element}, timestamp={self.timestamp})\n"
