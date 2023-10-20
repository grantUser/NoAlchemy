from typing import Any

from .engine import _engine, InsertOne, UpdateOne, DeleteOne
from .types import Integer, Key, String

__version__ = "0.1.1"

class create_engine:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _engine(*args, **kwds)
