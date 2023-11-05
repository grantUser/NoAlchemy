import threading
from uuid import uuid4

from bson import ObjectId
from pymongo import MongoClient

from ..exc import BadReferenceException, TransactionException
from .document import Document, collection_registry
from .ops import *
from .query import Query, QueryResult, RemoveQuery
from .query_expression import FreeFormDoc


class sessionmaker:
    def __init__(self, bind=None):
        self.bind = bind

    def __call__(self):
        if self.bind:
            return Session(self.bind)
        else:
            raise ValueError("No bind provided for sessionmaker")

    def __enter__(self):
        if self.bind:
            return Session(self.bind)
        else:
            raise ValueError("No bind provided for sessionmaker")

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class scoped_session:
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


class Session:
    def __init__(self, engine):
        self.engine = engine

        self.auto_ensure = True
        self.queue = []
        self.cache = {}
        self.transactions = []

        self.__post_init__()

    def __post_init__(self):
        self.db = self.engine.database
        self.safe = self.engine.safe
        self.timezone = self.engine.timezone
        self.cache_size = self.engine.cache_size
        self.tz_aware = self.engine.tz_aware
        self._autocommit = self.engine.autocommit

    @property
    def autocommit(self):
        return not self.in_transaction and self._autocommit

    @property
    def in_transaction(self):
        return bool(self.transactions)

    def cache_write(self, obj, mongo_id=None):
        if mongo_id is None:
            mongo_id = obj.mongo_id

        if self.cache_size == 0:
            return
        if mongo_id not in self.cache:
            if self.cache_size is not None and len(self.cache) >= self.cache_size:
                key_to_delete = next(iter(self.cache))
                del self.cache[key_to_delete]
            assert isinstance(mongo_id, ObjectId), (
                "Currently, cached objects must use mongo_id as an ObjectId.  Got: %s"
                % type(mongo_id)
            )
            self.cache[mongo_id] = obj

    def cache_read(self, id):
        if self.cache_size == 0:
            return
        assert isinstance(
            id, ObjectId
        ), "Currently, cached objects must use mongo_id as an ObjectId"
        if id in self.cache:
            return self.cache[id]
        return None

    def close(self):
        self.cache = {}
        if self.transactions:
            raise TransactionException(
                "Tried to close session with an open " "transaction"
            )
        self.db.client.close()

    def add(self, item, safe=None):
        item._set_session(self)
        if safe is None:
            safe = self.safe
        self.queue.append(SaveOp(self.transaction_id, self, item, safe))
        self.cache_write(item)
        if self.autocommit:
            return self.commit()

    def update(
        self, item, id_expression=None, upsert=False, update_ops={}, safe=None, **kwargs
    ):
        if safe is None:
            safe = self.safe
        self.queue.append(
            UpdateDocumentOp(
                self.transaction_id,
                self,
                item,
                safe,
                id_expression=id_expression,
                upsert=upsert,
                update_ops=update_ops,
                **kwargs
            )
        )
        if self.autocommit:
            return self.commit()

    def query(self, type, exclude_subclasses=False):
        if isinstance(type, str):
            type = FreeFormDoc(type)
        return Query(type, self, exclude_subclasses=exclude_subclasses)

    def add_to_session(self, obj):
        obj._set_session(self)

    def execute_query(self, query, session):
        self.auto_ensure_indexes(query.type)

        kwargs = dict()
        if query._get_fields():
            kwargs["projection"] = query._fields_expression()

        collection = self.db[query.type.get_collection_name()]
        cursor = collection.find(query.query, **kwargs)

        if query._sort:
            cursor.sort(query._sort)
        elif query.type.config_default_sort:
            cursor.sort(query.type.config_default_sort)
        if query.hints:
            cursor.hint(query.hints)
        if query._get_limit() is not None:
            cursor.limit(query._get_limit())
        if query._get_skip() is not None:
            cursor.skip(query._get_skip())
        return QueryResult(
            session,
            cursor,
            query.type,
            raw_output=query._raw_output,
            fields=query._get_fields(),
        )

    def remove_query(self, type):
        return RemoveQuery(type, self)

    def remove(self, obj, safe=None):
        if safe is None:
            safe = self.safe
        remove = RemoveDocumentOp(self.transaction_id, self, obj, safe)
        self.queue.append(remove)
        if self.autocommit:
            return self.commit()

    def execute_remove(self, remove):
        safe = self.safe
        if remove.safe is not None:
            safe = remove.safe

        self.queue.append(
            RemoveOp(self.transaction_id, self, remove.type, safe, remove)
        )
        if self.autocommit:
            return self.commit()

    def execute_update(self, update, safe=False):
        assert len(update.update_data) > 0
        self.queue.append(
            UpdateOp(self.transaction_id, self, update.query.type, safe, update)
        )
        if self.autocommit:
            return self.commit()

    def execute_find_and_modify(self, fm_exp):
        if self.in_transaction:
            raise TransactionException("Cannot find and modify in a transaction.")
        self.commit()
        self.auto_ensure_indexes(fm_exp.query.type)

        collection = self.db[fm_exp.query.type.get_collection_name()]
        kwargs = {
            "query": fm_exp.query.query,
            "update": fm_exp.update_data,
            "upsert": fm_exp._get_upsert(),
        }

        if fm_exp.query._get_fields():
            kwargs["fields"] = fm_exp.query._fields_expression()
        if fm_exp.query._sort:
            kwargs["sort"] = fm_exp.query._sort
        if fm_exp._get_new():
            kwargs["new"] = fm_exp._get_new()
        if fm_exp._get_remove():
            kwargs["remove"] = fm_exp._get_remove()

        value = collection.find_one_and_update(**kwargs)

        if value is None:
            return None

        obj = self.cache_read(value["_id"])
        if obj is not None:
            return obj

        obj = self._unwrap(fm_exp.query.type, value, fields=fm_exp.query._get_fields())
        if not fm_exp.query._get_fields():
            self.cache_write(obj)
        return obj

    def _unwrap(self, type, obj, **kwargs):
        obj = type.transform_incoming(obj, session=self)
        return type.unwrap(obj, session=self, **kwargs)

    @property
    def transaction_id(self):
        if not self.transactions:
            return None
        return self.transactions[-1]

    def get_indexes(self, cls):
        return self.db[cls.get_collection_name()].index_information()

    def ensure_indexes(self, cls):
        collection = self.db[cls.get_collection_name()]
        for index in cls.get_indexes():
            index.ensure(collection)

    def auto_ensure_indexes(self, cls):
        if self.auto_ensure:
            self.ensure_indexes(cls)

    def clear_queue(self, trans_id=None):
        if not self.queue:
            return
        if trans_id is None:
            self.queue = []
            return

        for index, op in enumerate(self.queue):
            if op.trans_id == trans_id:
                break
        self.queue = self.queue[:index]

    def clear_cache(self):
        self.cache = {}

    def clear_collection(self, *classes):
        for c in classes:
            self.queue.append(ClearCollectionOp(self.transaction_id, self, c))
        if self.autocommit:
            self.commit()

    def commit(self, safe=None):
        result = None
        for index, op in enumerate(self.queue):
            try:
                result = op.execute()
            except:
                self.clear_queue()
                self.clear_cache()
                raise
        self.clear_queue()
        return result

    def dereference(self, ref, allow_none=False):
        if isinstance(ref, Document):
            return ref
        if not hasattr(ref, "type"):
            if ref.collection in collection_registry["global"]:
                ref.type = collection_registry["global"][ref.collection]
        assert hasattr(ref, "type")

        obj = self.cache_read(ref.id)
        if obj is not None:
            return obj
        if ref.database and self.db.name != ref.database:
            db = self.db.client[ref.database]
        else:
            db = self.db
        value = db.dereference(ref)
        if value is None and allow_none:
            obj = None
            self.cache_write(obj, mongo_id=ref.id)
        elif value is None:
            raise BadReferenceException("Bad reference: %r" % ref)
        else:
            obj = self._unwrap(ref.type, value)
            self.cache_write(obj)
        return obj

    def refresh(self, document):
        try:
            old_cache_size = self.cache_size
            self.cache_size = 0
            obj = self.query(type(document)).filter_by(mongo_id=document.mongo_id).one()
        finally:
            self.cache_size = old_cache_size
        self.cache_write(obj)
        return obj

    def clone(self, document):
        wrapped = document.wrap()
        if "_id" in wrapped:
            del wrapped["_id"]
        return type(document).unwrap(wrapped, session=self)

    def begin_trans(self):
        self.transactions.append(uuid4())
        return self

    def __enter__(self):
        return self.begin_trans()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.end_trans(exc_type, exc_val, exc_tb)

    def end_trans(self, exc_type=None, exc_val=None, exc_tb=None):
        id = self.transactions.pop()

        if exc_type:
            self.clear_queue(trans_id=id)

        if self.transactions:
            return False

        if not exc_type:
            self.commit()
            self.close()
        else:
            self.clear_queue()
            self.clear_cache()
        return False
