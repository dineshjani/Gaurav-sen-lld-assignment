from enum import Enum

class EvictionAlgorithm(Enum):
    LRU = 'LRU'
    LFU = 'LFU'
