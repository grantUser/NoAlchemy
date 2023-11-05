from typing import Any

from .engine import _engine
from .version import __version__, version


class create_engine:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _engine(*args, **kwds)
