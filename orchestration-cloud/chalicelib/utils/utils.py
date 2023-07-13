from enum import Enum

class BaseEnum(Enum):
    @classmethod
    def values(cls):
        return [e.value for e in cls]