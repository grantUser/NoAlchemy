import threading

from bson import ObjectId

from ..exc import *
from ..types import Key
from .models import models


class Transaction:
    def __init__(self, bind=None, autocommit=False) -> None:
        self.__bind = bind
        self.__autocommit = autocommit
        self.__to_insert = {}
        self.__inserted = {}
        self.__to_update = {}
        self.__updated = {}
        self.__to_delete = {}
        self.__deleted = {}

    def delete_one(self, object):
        if not hasattr(object, "__from__"):
            raise Exception()

        if object.__from__ != 1:
            raise Exception()

        collection = object.__collection_name__
        if collection not in self.__to_delete:
            self.__to_delete[collection] = []

        if hasattr(object, "to_dict") and callable(object.to_dict):
            self.__to_delete[collection].append(object.to_dict())

    def delete_many(self, objects):
        for object in objects:
            self.delete_one(object)

    def insert_one(self, object):
        if hasattr(object, "__from__") and object.__from__ == 1:
            raise Exception()

        collection = object.__collection_name__
        if collection not in self.__to_insert:
            self.__to_insert[collection] = []

        for index, item in enumerate(self.__to_insert[collection]):
            if item["_id"] == object._id:
                self.__to_insert[collection][index] = object.to_dict()
                return

        if hasattr(object, "to_dict") and callable(object.to_dict):
            self.__to_insert[collection].append(object.to_dict())

    def insert_many(self, objects):
        for object in objects:
            self.insert_one(object)

    def update_one(self, object):
        if not hasattr(object, "__from__"):
            raise Exception()

        if object.__from__ != 1:
            raise Exception()

        if object.__eta__ != 2:
            return

        collection = object.__collection_name__
        if collection not in self.__to_update:
            self.__to_update[collection] = []

        if hasattr(object, "items") and callable(object.items):
            self.__to_update[collection].append(
                {
                    "object": object.to_dict(),
                    "$set": {
                        key: value
                        for key, value in object.items()
                        if key in object.__originals__.keys()
                    },
                }
            )

    def commit(self):
        # order -> INSERT, UPDATE, DELETE
        try:
            for collection, objects in self.__to_insert.items():
                collection = self.__bind.database[collection]

                match len(objects):
                    case n if n > 1:
                        collection.insert_many(objects)
                    case 1:
                        collection.insert_one(objects[0])
                    case 0:
                        pass

                if collection not in self.__inserted:
                    self.__inserted[collection] = []

                self.__inserted[collection].extend(objects)

            self.__to_insert = {}
            self.__inserted = {}

            # update part
            for collection, objects in self.__to_update.items():
                collection = self.__bind.database[collection]

                for object in objects:
                    set = object.get("$set", {})
                    object = object.get("object", None)

                    if document_id := object.get("_id", None):
                        collection.update_one({"_id": document_id}, {"$set": set})

                if collection not in self.__updated:
                    self.__updated[collection] = []

                self.__updated[collection].extend(objects)

            self.__to_update = {}
            self.__updated = {}

            # delete part

            for collection, objects in self.__to_delete.items():
                collection = self.__bind.database[collection]

                for object in objects:
                    if document_id_to_delete := object.get("_id", False):
                        collection.delete_one({"_id": document_id_to_delete})

                if collection not in self.__deleted:
                    self.__deleted[collection] = []

                self.__deleted[collection].extend(objects)

            self.__to_delete = {}
            self.__deleted = {}

        except Exception as e:
            self.rollback()
            raise Exception(e)

    def rollback(self):
        for collection, objects in self.__inserted.items():
            collection = self.__bind.database[collection]

            for object in objects:
                collection.delete_one(object)

        self.__to_insert = {}
        self.__inserted = {}

        for collection, objects in self.__updated.items():
            collection = self.__bind.database[collection]

            for object in objects:
                set = object.get("$set", {})
                object = object.get("object", None)

                if document_id := object.get("_id", None):
                    setset = {
                        key: value for key, value in object.items() if key in set.keys()
                    }

                    collection.update_one({"_id": document_id}, {"$set": setset})

        for collection, objects in self.__deleted.items():
            collection = self.__bind.database[collection]

            match len(objects):
                case n if n > 1:
                    collection.insert_many(objects)
                case 1:
                    collection.insert_one(objects[0])
                case 0:
                    pass

        self.__to_delete = {}
        self.__deleted = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.__autocommit:
            self.commit()


class Session:
    def __init__(self, bind=None, autocommit=False) -> None:
        self.bind = bind
        self.autocommit = autocommit
        self._transation = Transaction(bind=self.bind, autocommit=self.autocommit)

    def commit(self, *args, **kwds):
        if self._transation:
            with self._transation as transaction:
                transaction.commit(*args, **kwds)

    def rollback(self, *args, **kwds):
        if self._transation:
            with self._transation as transaction:
                transaction.rollback(*args, **kwds)

    def add(self, object):
        if object.__class__.__name__ not in models.instances:
            raise Exception(
                "The following object cannot be translated by NoAlchemy", object
            )

        if not object.__collection_name__:
            raise Exception(
                "The following object does not have a __collection_name__", object
            )

        if not hasattr(object, "_id") or not ObjectId.is_valid(object._id):
            raise Exception("_id incorrect", object)

        if self._transation:
            with self._transation as transaction:
                transaction.insert_one(object)

    def delete(self, object):
        if object.__class__.__name__ not in models.instances:
            raise Exception(
                "The following object cannot be translated by NoAlchemy", object
            )

        if not object.__collection_name__:
            raise Exception(
                "The following object does not have a __collection_name__", object
            )

        if not hasattr(object, "_id") or not ObjectId.is_valid(object._id):
            raise Exception("_id incorrect", object)

        if self._transation:
            with self._transation as transaction:
                transaction.delete_one(object)

    def update(self, object):
        if object.__class__.__name__ not in models.instances:
            raise Exception(
                "The following object cannot be translated by NoAlchemy", object
            )

        if not object.__collection_name__:
            raise Exception(
                "The following object does not have a __collection_name__", object
            )

        if not hasattr(object, "_id") or not ObjectId.is_valid(object._id):
            raise Exception("_id incorrect", object)

        if self._transation:
            with self._transation as transaction:
                transaction.update_one(object)

    def add_all(self, objects: list):
        if not isinstance(objects, list):
            raise Exception("An argument of type List is expected", object)

        for object in objects:
            if object.__class__.__name__ not in models.instances:
                raise Exception(
                    "The following object cannot be translated by NoAlchemy", object
                )

            if not object.__collection_name__:
                raise Exception(
                    "The following object does not have a __collection_name__", object
                )

            if not hasattr(object, "_id") or not ObjectId.is_valid(object._id):
                raise Exception("_id incorrect", object)

        if self._transation:
            with self._transation as transaction:
                transaction.insert_many(objects)

    def delete_all(self, objects: list):
        if not isinstance(objects, list):
            raise Exception("An argument of type List is expected", object)

        for object in objects:
            if object.__class__.__name__ not in models.instances:
                raise Exception(
                    "The following object cannot be translated by NoAlchemy", object
                )

            if not object.__collection_name__:
                raise Exception(
                    "The following object does not have a __collection_name__", object
                )

            if not hasattr(object, "_id") or not ObjectId.is_valid(object._id):
                raise Exception("_id incorrect", object)

        if self._transation:
            with self._transation as transaction:
                transaction.delete_many(objects)

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
            for arg in args:
                if isinstance(arg, Key):
                    if hasattr(arg, "__collection_name__"):
                        self.collections.append(arg.__collection_name__)
                    if hasattr(arg, "__key__"):
                        self.projection[arg.__key__] = 1
                    if hasattr(arg, "__object__"):
                        self.objects.append(arg.__object__)
                elif isinstance(arg, str):
                    if arg in models.instances:
                        model = models.instances.get(arg, None)
                        self.collections.append(model.__collection_name__)
                        self.objects.append(model)
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
            for name, instance in self.object.__annotations__.items():
                if instance.required or projection_len < 1:
                    self.projection[name] = 1

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
                    document_list.append(self.object(**document, __from__=1))
                return document_list

        def one_or_none(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(self.filter, self.projection)
                if document:
                    return self.object(**document, __from__=1)
            return None

        def one(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(self.filter, self.projection)
                if document:
                    return self.object(**document, __from__=1)
            raise NoResultFoundException()

        def first(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(
                    self.filter, self.projection, sort=[("_id", 1)]
                )
                if document:
                    return self.object(**document, __from__=1)

        def last(self):
            if self.collection in self.bind.database.list_collection_names():
                collection = self.bind.database[self.collection]
                document = collection.find_one(
                    self.filter, self.projection, sort=[("_id", -1)]
                )
                if document:
                    return self.object(**document, __from__=1)


class _sessionmaker:
    def __init__(self, bind=None, autocommit=False):
        self.bind = bind
        self.autocommit = autocommit

    def __call__(self):
        if self.bind:
            return Session(self.bind, self.autocommit)
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

    def add(self, object):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session.add(object)

    def delete(self, object):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session.delete(object)

    def add_all(self, objects):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session.add_all(objects)

    def delete_all(self, objects):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session.delete_all(objects)

    def update(self, object):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session.update(object)

    def commit(self):
        if not hasattr(self._local, "session"):
            raise Exception("No session detected, unable to commit.")
        return self._local.session.commit()

    def rollback(self):
        if not hasattr(self._local, "session"):
            raise Exception("No session detected, unable to rollback.")
        return self._local.session.rollback()

    def __enter__(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self._local, "session"):
            del self._local.session
