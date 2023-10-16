from typing import Any

from .engine import _engine
from .types import Integer, Key, String


class create_engine:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _engine(*args, **kwds)
