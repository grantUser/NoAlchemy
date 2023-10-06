from noalchemy.orm import Base
from noalchemy import Key, String

class User(Base):
    __collection_name__ = "users"

    name: Key(String(50, uppercase=True, strip=True))
    othername: str
