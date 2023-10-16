import threading

from ..exc import *
from ..types import Key


class Session:
    def __init__(self, bind=None, autocommit=False, autoflush=False) -> None:
        self.bind = bind
        self.autocommit = autocommit
        self.autoflush = autoflush

    def query(self, *args, **kwds):
        kwds["bind"] = self.bind
        return self.Query(*args, **kwds)

    class Query:
        def __init__(self, *args, **kwds) -> None:
            self.bind = None

            self.projection = {}
            self.filter = {}

            self.collections = []
            self.collection = None

            self.objects = []
            self.object = None

            self.__post_init__(*args, **kwds)

        def __post_init__(self, *args, **kwds):
            if collection_kwd := kwds.get("collection", False):
                self.collection = collection_kwd

            if bind_kwd := kwds.get("bind", False):
                self.bind = bind_kwd

            if object_kwd := kwds.get("object", False):
                self.object = object_kwd

            for arg in args:
                if isinstance(arg, Key):
                    if hasattr(arg, "__collection_name__"):
                        self.collections.append(arg.__collection_name__)

                    if hasattr(arg, "__key__"):
                        self.projection[arg.__key__] = 1

                    if hasattr(arg, "__object__"):
                        self.objects.append(arg.__object__)

                elif isinstance(arg, object):
                    if hasattr(arg, "__collection_name__"):
                        self.collections.append(arg.__collection_name__)
                        self.objects.append(arg)

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
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                documents = collection.find(self.filter, self.projection)

                for document in documents:
                    document_list.append(self.object(**document))

            return document_list

        def one_or_none(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(self.filter, self.projection)

                if document:
                    return self.object(**document)

            return None

        def one(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(self.filter, self.projection)

                if document:
                    return self.object(**document)

            raise NoResultFoundException()

        def first(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(
                    self.filter, self.projection, sort=[("_id", 1)]
                )

                if document:
                    return self.object(**document)

        def last(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(
                    self.filter, self.projection, sort=[("_id", -1)]
                )

                if document:
                    return self.object(**document)


class _sessionmaker:
    def __init__(self, bind=None, autocommit=False, autoflush=False):
        self.bind = bind
        self.autocommit = autocommit
        self.autoflush = autoflush

    def __call__(self):
        if self.bind:
            return Session(self.bind, self.autocommit, self.autoflush)
        else:
            raise ValueError("No bind provided for sessionmaker")


class _scoped_session:
    def __init__(self, session_factory):
        self.session_factory = session_factory
        self._local = threading.local()

    def __call__(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session

    def query(self, *args, collection=None, object=None):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session.query(*args, collection=collection, object=object)

    def __enter__(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self._local, "session"):
            del self._local.session
