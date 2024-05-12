from datetime import datetime
from typing import Tuple

class AccessDetails:
    def __init__(self, last_access_time: int):
        self.access_count = 0
        self.last_access_time = last_access_time

    def get_last_access_time(self) -> int:
        return self.last_access_time

    def get_access_count(self) -> int:
        return self.access_count

    def update(self, last_access_time: int) -> 'AccessDetails':
        new_access_details = AccessDetails(last_access_time)
        new_access_details.access_count = self.access_count + 1
        return new_access_details

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AccessDetails):
            return False
        return self.last_access_time == other.last_access_time and self.access_count == other.access_count

    def __hash__(self) -> int:
        return hash((self.last_access_time, self.access_count))

    def __str__(self) -> str:
        return f"AccessDetails(access_count={self.access_count}, last_access_time={self.last_access_time})"
