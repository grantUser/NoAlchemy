from typing import Any

from .engine import _engine, InsertOne, UpdateOne, DeleteOne
from .types import Integer, Key, String, Dict, List
from .version import __version__, version, __prod__

class create_engine:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _engine(*args, **kwds)
