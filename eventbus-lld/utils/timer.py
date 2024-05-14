from typing import Callable
import time


class Timer:
    def __init__(self):
        pass

    def get_current_time(self) -> int:
        return int(time.time() * 1e9)  # Convert seconds to nanoseconds
