from typing import Any


def create_engine(*args, **kwargs):
    return NoAlchemy(*args, **kwargs)


class NoAlchemy:
    def __init__(self, url: str, mock: bool = False) -> None:
        self.url = url
        self.mock = mock
        self.client = None
        self._post_init()

    def _post_init(self):
        from pymongo import MongoClient

        if self.mock:
            from mongomock import MongoClient

        self.client = MongoClient(self.client)

    def query(self, *args):
        return self.Query(self, *args)

    class Query:
        def __init__(self, no_alchemy, *args):
            self.no_alchemy = no_alchemy

        def filter_by(self, **kwargs):
            return "filter"