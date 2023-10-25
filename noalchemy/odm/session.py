import threading

from bson import ObjectId

from ..exc import *
from .models import models
from .query import Query
from .transaction import Transaction


class Session:
    def __init__(self, bind=None, autocommit=False) -> None:
        self.bind = bind
        self.autocommit = autocommit
        self._transaction = Transaction(bind=self.bind)
        self.list_collection_names = self.bind.database.list_collection_names()

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
            with self._transaction as transaction:
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
        return Query(*args, **kwds)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.autocommit:
            self.commit()

        self._transaction = Transaction(bind=self.bind)


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

    def __enter__(self):
        session = self._get_or_create_session()
        return session

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self._local, "session"):
            del self._local.session
