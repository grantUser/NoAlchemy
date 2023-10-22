import threading

from bson import ObjectId

from ..exc import *
from ..types import Key
from .models import models

from .. import InsertOne, UpdateOne, DeleteOne


class Transaction:
    def __init__(self, bind=None) -> None:
        self.__bind = bind
        self.bulk_operations = {}


    def _get_bulk_operations(self, collection_name):
        if collection_name not in self.bulk_operations:
            self.bulk_operations[collection_name] = []

        return self.bulk_operations[collection_name]

    def delete_one(self, object):
        if not hasattr(object, "__from__"):
            raise Exception()

        if object.__from__ != 1:
            raise Exception()

        collection = object.__collection_name__
        bulk_ops = self._get_bulk_operations(collection)
        bulk_ops.append(DeleteOne({"_id": object._id}))

    def delete_many(self, objects):
        for object in objects:
            self.delete_one(object)

    def insert_one(self, object):
        if hasattr(object, "__from__") and object.__from__ == 1:
            raise Exception()

        collection = object.__collection_name__
        bulk_ops = self._get_bulk_operations(collection)
        document = object.to_dict()
        bulk_ops.append(InsertOne(document))

    def insert_many(self, objects):
        for object in objects:
            self.insert_one(object)

    def update_one(self, object):
        if not hasattr(object, "__from__") or object.__from__ != 1:
            raise Exception()

        if object.__eta__ != 2:
            return

        collection = object.__collection_name__
        bulk_ops = self._get_bulk_operations(collection)
        document = object.to_dict()
        
        update_existing = None
        for op in bulk_ops:
            if op.__class__ == UpdateOne and op._filter.get("_id") == document.get("_id"):
                update_existing = op
                break
        
        if update_existing:
            update = update_existing._doc
            update["$set"].update({key: value for key, value in object.items() if key in object.__originals__.keys()})
        else:
            update = {
                "$set": {key: value for key, value in object.items() if key in object.__originals__.keys()}
            }
            bulk_ops.append(UpdateOne({"_id": document["_id"]}, update))

    def commit(self):
        for collection_name, bulk_ops in self.bulk_operations.items():
            if bulk_ops:
                try:
                    collection = self.__bind.database[collection_name]
                    collection.bulk_write(bulk_ops, ordered=True)
                except Exception as e:
                    self.rollback()
                    raise e

    def rollback(self):
        for collection_name, bulk_ops in self.bulk_operations.items():
            for operation in reversed(bulk_ops):
                if operation.__class__ == DeleteOne:
                    collection = self.__bind.database[collection_name]
                    collection.insert_one(operation.filter)
                elif operation.__class__ == InsertOne:
                    collection = self.__bind.database[collection_name]
                    collection.delete_one(operation.document)
                elif operation.__class__ == UpdateOne:
                    collection = self.__bind.database[collection_name]
                    collection.update_one(operation.filter, {"$set": {key: None for key in operation.update["$set"]}})
        self.bulk_operations = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Session:
    def __init__(self, bind=None, autocommit=False) -> None:
        self.bind = bind
        self.autocommit = autocommit
        self._transaction = Transaction(bind=self.bind)

    def commit(self, *args, **kwds):
        if self._transaction:
            with self._transaction as transaction:
                transaction.commit(*args, **kwds)

    def rollback(self, *args, **kwds):
        if self._transaction:
            with self._transaction as transaction:
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

        if self._transaction:
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

        if self._transaction:
            with self._transaction as transaction:
                transaction.delete_one(object)

    def _update(self, object):
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

        if self._transaction:
            with self._transaction as transaction:
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

        if self._transaction:
            with self._transaction as transaction:
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

        if self._transaction:
            with self._transaction as transaction:
                transaction.delete_many(objects)

    def query(self, *args, **kwds):
        kwds["bind"] = self.bind
        kwds["Session"] = self
        return self.Query(*args, **kwds)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.autocommit:
            self.commit()

    class Query:
        def __init__(self, *args, **kwds) -> None:
            self.bind = None
            self.Session = None
            self.projection = {}
            self.filter = {}
            self.limit_value = 1000
            self.offset_value = 0
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
            if Session_kwd := kwds.get("Session", False):
                self.Session = Session_kwd
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

        def collection_exists_decorator(function):
            def wrapper(self, *args, **kwds):
                if self.collection in self.bind.database.list_collection_names():
                    collection = self.bind.database[self.collection]
                    return function(self, collection, *args, **kwds)
                else:
                    raise Exception("Collection not found in the targeted database")
            return wrapper

        def filter_by(self, **kwargs):
            for key, value in kwargs.items():
                self.filter[key] = value

            return self

        def offset(self, skip_value):
            self.offset_value = skip_value
            return self

        def limit(self, limit_value):
            self.limit_value = limit_value
            return self

        @collection_exists_decorator
        def all(self, collection):
            document_list = []
            documents = collection.find(self.filter, self.projection).skip(self.offset_value).limit(self.limit_value)
            for document in documents:
                document_list.append(self.object(**document, __from__=1, Session=self.Session))

            return document_list

        @collection_exists_decorator
        def one_or_none(self, collection):
            document = collection.find_one(self.filter, self.projection)
            if document:
                return self.object(**document, __from__=1, Session=self.Session)
            return None

        @collection_exists_decorator
        def one(self, collection):
            document = collection.find_one(self.filter, self.projection)
            if document:
                return self.object(**document, __from__=1, Session=self.Session)
            raise NoResultFoundException()

        @collection_exists_decorator
        def first(self, collection):
            document = collection.find_one(
                self.filter, self.projection, sort=[("_id", 1)]
            )
            if document:
                return self.object(**document, __from__=1, Session=self.Session)
            raise NoResultFoundException()

        @collection_exists_decorator
        def last(self, collection):
            document = collection.find_one(
                self.filter, self.projection, sort=[("_id", -1)]
            )
            if document:
                return self.object(**document, __from__=1, Session=self.Session)
            raise NoResultFoundException()
        
        @collection_exists_decorator
        def count(self, collection, **kwds):
            return collection.count_documents(self.filter, **kwds)


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

    def _get_or_create_session(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.session_factory()
        return self._local.session

    def __call__(self, autocommit=False):
        session = self._get_or_create_session()
        session.autocommit = autocommit
        return session

    def query(self, *args, collection=None, object=None):
        session = self._get_or_create_session()
        return session.query(*args, collection=collection, object=object)

    def add(self, object):
        session = self._get_or_create_session()
        return session.add(object)

    def delete(self, object):
        session = self._get_or_create_session()
        return session.delete(object)

    def add_all(self, objects):
        session = self._get_or_create_session()
        return session.add_all(objects)

    def delete_all(self, objects):
        session = self._get_or_create_session()
        return session.delete_all(objects)

    def update(self, object):
        session = self._get_or_create_session()
        return session.update(object)

    def commit(self):
        session = self._get_or_create_session()
        return session.commit()

    def rollback(self):
        session = self._get_or_create_session()
        return session.rollback()

    def __enter__(self):
        session = self._get_or_create_session()
        return session

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self._local, "session"):
            del self._local.session