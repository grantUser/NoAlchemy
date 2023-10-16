from noalchemy import Integer, Key, String

from utils.base import Base

# from utils.base import Base


class User(Base):
    __collection_name__ = "users"

    name: Key(String(255, uppercase=True, strip=True), required=True)
    age: Key(Integer(100))
