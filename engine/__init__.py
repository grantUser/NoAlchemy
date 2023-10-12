import re

from ..exc import (MultipleCollectionsFound, MultipleObjectsDetected,
                   NoResultFoundException)


def create_engine(*args, **kwargs):
    return NoAlchemy(*args, **kwargs)


class NoAlchemy:
    def __init__(self, url: str, mock: bool = False) -> None:
        self.url = url
        self.mock = mock
        self.client = None
        self.database = None
        self._post_init()
        self.base = None

    def _post_init(self):
        url_parsed = self._parse_url()
        if url_parsed.get("name", "") == "mongodb" and url_parsed.get(
            "database", False
        ):

            from pymongo import MongoClient

            if self.mock:
                from mongomock import MongoClient

            self.client = MongoClient(self.url)
            self.database = self.client[url_parsed.get("database", None)]

    def _parse_url(self):
        pattern = re.compile(
            r"""
                (?P<name>[\w\+]+)://
                (?:
                    (?P<username>[^:/]*)
                    (?::(?P<password>[^@]*))?
                @)?
                (?:
                    (?:
                        \[(?P<ipv6host>[^/\?]+)\] |
                        (?P<ipv4host>[^/:\?]+)
                    )?
                    (?::(?P<port>[^/\?]*))?
                )?
                (?:/(?P<database>[^\?]*))?
                (?:\?(?P<query>.*))?
            """,
            re.X,
        )

        match = pattern.match(self.url)

        if match:
            uri_info = match.groupdict()
            return uri_info

        return False

    def query(self, *args, collection=None, object=None):
        return self.Query(self, collection, object, *args)

    class Query:
        def __init__(self, noalchemy, collection=None, object=None, *args):
            self.noalchemy = noalchemy

            self.projection = {}
            self.filter = {}

            self.collections = []
            self.collection = collection

            self.objects = []
            self.object = object

            self.process_args(*args)

        def process_args(self, *args):
            for arg in args:
                if hasattr(arg, "__collection_name__"):
                    self.collections.append(arg.__collection_name__)

                if hasattr(arg, "__key__"):
                    self.projection[arg.__key__] = 1

                if hasattr(arg, "__object__"):
                    self.objects.append(arg.__object__)
                elif issubclass(arg, self.noalchemy.base):
                    self.objects.append(arg)

                continue

            if self.collection:
                self.collections.append(self.collection)

            self.collections = list(set(self.collections))
            if len(self.collections) > 1:
                raise MultipleCollectionsFound()

            if self.object:
                self.objects.append(self.object)

            self.objects = list(set(self.objects))
            if len(self.objects) > 1:
                raise MultipleObjectsDetected()

            if len(self.collections) != 1 and len(self.objects) != 1:
                return

            self.collection = self.collections[0]
            self.object = self.objects[0]

            projection_len = len(self.projection)
            for name, type in self.object.__annotations__.items():
                if type.required or projection_len < 1:
                    self.projection[name] = 1

            return

        def filter_by(self, **kwargs):
            for key, value in kwargs.items():
                self.filter[key] = value

            return self

        def all(self):
            document_list = []
            if self.collection in self.noalchemy.database.list_collection_names():
                collection = self.noalchemy.database[self.collection]
                documents = collection.find(self.filter, self.projection)

                for document in documents:
                    document_list.append(self.object(**document))

            return document_list

        def one_or_none(self):
            if self.collection in self.noalchemy.database.list_collection_names():
                collection = self.noalchemy.database[self.collection]
                document = collection.find_one(self.filter, self.projection)

                if document:
                    return self.object(**document)

            return None

        def one(self):
            if self.collection in self.noalchemy.database.list_collection_names():
                collection = self.noalchemy.database[self.collection]
                document = collection.find_one(self.filter, self.projection)

                if document:
                    return self.object(**document)

            raise NoResultFoundException()
