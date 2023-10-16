from typing import Any

from .document import declarative_base
from .session import _scoped_session, _sessionmaker


class scoped_session:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _scoped_session(*args, **kwds)


class sessionmaker:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _sessionmaker(*args, **kwds)
