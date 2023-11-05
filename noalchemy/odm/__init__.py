from typing import Any

from .document import Document
# from .session import Session
from .session import scoped_session as _scoped_session
from .session import sessionmaker as _sessionmaker


class scoped_session:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _scoped_session(*args, **kwds)


class sessionmaker:
    def __new__(self, *args: Any, **kwds: Any) -> None:
        return _sessionmaker(*args, **kwds)
