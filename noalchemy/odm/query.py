from copy import deepcopy

from pymongo import ASCENDING, DESCENDING

from ..exc import BadResultException
from ..util import resolve_name
from .query_expression import BadQueryException, QueryExpression, flatten
from .update_expression import FindAndModifyExpression, UpdateExpression


class Query:
    def __init__(self, type, session, exclude_subclasses=False):
        self.session = session
        self.type = type

        self.__query = type.base_query(exclude_subclasses)
        self._sort = []
        self._fields = None
        self.hints = []
        self._limit = None
        self._skip = None
        self._raw_output = False

    def __iter__(self):
        return self.__get_query_result()

    @property
    def query(self):
        return flatten(self.__query)

    def __get_query_result(self):
        return self.session.execute_query(self, self.session)

    def raw_output(self):
        self._raw_output = True
        return self

    def _get_fields(self):
        return self._fields

    def _get_limit(self):
        return self._limit

    def _get_skip(self):
        return self._skip

    def limit(self, limit):
        self._limit = limit
        return self

    def skip(self, skip):
        self._skip = skip
        return self

    def clone(self):
        qclone = Query(self.type, self.session)
        qclone.__query = deepcopy(self.__query)
        qclone._sort = deepcopy(self._sort)
        qclone._fields = deepcopy(self._fields)
        qclone.hints = deepcopy(self.hints)
        qclone._limit = deepcopy(self._limit)
        qclone._skip = deepcopy(self._skip)
        qclone._raw_output = deepcopy(self._raw_output)
        return qclone

    def one(self):
        iterator = iter(self)
        try:
            result = next(iterator)
        except StopIteration:
            raise BadResultException("Too few results for .one()")

        for _ in iterator:
            raise BadResultException("Too many results for .one()")

        return result

    def one_or_none(self):
        iterator = iter(self)
        try:
            result = next(iterator)
        except StopIteration:
            return None

        for _ in iterator:
            raise BadResultException("Too many results for .one_or_none()")

        return result

    def first(self):
        for doc in iter(self):
            return doc
        return None

    def __getitem__(self, index):
        return self.__get_query_result().__getitem__(index)

    def hint_asc(self, qfield):
        return self.__hint(qfield, ASCENDING)

    def hint_desc(self, qfield):
        return self.__hint(qfield, DESCENDING)

    def __hint(self, qfield, direction):
        qfield = resolve_name(self.type, qfield)
        name = str(qfield)
        for n, _ in self.hints:
            if n == name:
                raise BadQueryException("Already gave hint for %s" % name)
        self.hints.append((name, direction))
        return self

    def explain(self):
        return self.__get_query_result().cursor.explain()

    def all(self):
        return [obj for obj in iter(self)]

    def distinct(self, key):
        return self.__get_query_result().cursor.distinct(str(key))

    def filter(self, *query_expressions):
        for qe in query_expressions:
            if isinstance(qe, dict):
                self._apply_dict(qe)
            else:
                self._apply(qe)
        return self

    def filter_by(self, **filters):
        for name, value in filters.items():
            self.filter(resolve_name(self.type, name) == value)
        return self

    def count(self, with_limit_and_skip=False):
        return self.__get_query_result().cursor.count(
            with_limit_and_skip=with_limit_and_skip
        )

    def fields(self, *fields):
        if self._fields is None:
            self._fields = set()
        for f in fields:
            f = resolve_name(self.type, f)
            self._fields.add(f)
        self._fields.add(self.type.mongo_id)
        return self

    def _fields_expression(self):
        fields = {}
        for f in self._get_fields():
            fields[f.get_absolute_name()] = f.fields_expression
        return fields

    def _apply(self, qe):
        self._apply_dict(qe.obj)

    def _apply_dict(self, qe_dict):
        for k, v in qe_dict.items():
            k = resolve_name(self.type, k)
            if k not in self.__query:
                self.__query[k] = v
                continue
            if not isinstance(self.__query[k], dict) or not isinstance(v, dict):
                raise BadQueryException(
                    "Multiple assignments to a field must all be dicts."
                )
            self.__query[k].update(**v)

    def ascending(self, qfield):
        return self.__sort(qfield, ASCENDING)

    def descending(self, qfield):
        return self.__sort(qfield, DESCENDING)

    def sort(self, *sort_tuples):
        query = self
        for name, direction in sort_tuples:
            field = resolve_name(self.type, name)
            if direction in (ASCENDING, 1):
                query = query.ascending(field)
            elif direction in (DESCENDING, -1):
                query = query.descending(field)
            else:
                raise BadQueryException("Bad sort direction: %s" % direction)
        return query

    def __sort(self, qfield, direction):
        qfield = resolve_name(self.type, qfield)
        name = str(qfield)
        for n, _ in self._sort:
            if n == name:
                raise BadQueryException("Already sorting by %s" % name)
        self._sort.append((name, direction))
        return self

    def not_(self, *query_expressions):
        for qe in query_expressions:
            self.filter(qe.not_())
        return self

    def or_(self, first_qe, *qes):
        res = first_qe
        for qe in qes:
            res = res | qe
        self.filter(res)
        return self

    def in_(self, qfield, *values):
        qfield = resolve_name(self.type, qfield)
        self.filter(
            QueryExpression(
                {qfield: {"$in": [qfield.wrap_value(value) for value in values]}}
            )
        )
        return self

    def nin(self, qfield, *values):
        qfield = resolve_name(self.type, qfield)
        self.filter(
            QueryExpression(
                {qfield: {"$nin": [qfield.wrap_value(value) for value in values]}}
            )
        )
        return self

    def find_and_modify(self, new=False, remove=False):
        return FindAndModifyExpression(self, new=new, remove=remove)

    def set(self, *args, **kwargs):
        return UpdateExpression(self).set(*args, **kwargs)

    def unset(self, qfield):
        return UpdateExpression(self).unset(qfield)

    def inc(self, *args, **kwargs):
        return UpdateExpression(self).inc(*args, **kwargs)

    def append(self, qfield, value):
        return UpdateExpression(self).append(qfield, value)

    def extend(self, qfield, *value):
        return UpdateExpression(self).extend(qfield, *value)

    def remove(self, qfield, value):
        return UpdateExpression(self).remove(qfield, value)

    def remove_all(self, qfield, *value):
        return UpdateExpression(self).remove_all(qfield, *value)

    def add_to_set(self, qfield, value):
        return UpdateExpression(self).add_to_set(qfield, value)

    def pop_first(self, qfield):
        return UpdateExpression(self).pop_first(qfield)

    def pop_last(self, qfield):
        return UpdateExpression(self).pop_last(qfield)


class QueryResult:
    def __init__(self, session, cursor, type, raw_output=False, fields=None):
        self.cursor = cursor
        self.type = type
        self.fields = fields
        self.raw_output = raw_output
        self.session = session

    def next(self):
        return self._next_internal()

    __next__ = next

    def _next_internal(self):
        value = next(self.cursor)
        if not self.raw_output:
            db = self.cursor.collection.database
            conn = db.connection
            obj = self.session.cache_read(value["_id"])
            if obj:
                return obj
            value = self.session._unwrap(self.type, value, fields=self.fields)
            if not isinstance(value, dict):
                self.session.cache_write(value)
        return value

    def __getitem__(self, index):
        value = self.cursor.__getitem__(index)
        if not self.raw_output:
            db = self.cursor.collection.database
            conn = db.connection
            obj = self.session.cache_read(value["_id"])
            if obj:
                return obj
            value = self.session._unwrap(self.type, value)
            self.session.cache_write(value)
        return value

    def rewind(self):
        return self.cursor.rewind()

    def clone(self):
        return QueryResult(
            self.session,
            self.cursor.clone(),
            self.type,
            raw_output=self.raw_output,
            fields=self.fields,
        )

    def __iter__(self):
        return self


class RemoveQuery:
    def __init__(self, type, session):
        self.session = session
        self.type = type
        self.safe = None
        self.get_last_args = {}
        self.__query_obj = Query(type, session)

    @property
    def query(self):
        return self.__query_obj.query

    def set_safe(self, is_safe, **kwargs):
        self.safe = is_safe
        self.get_last_args.update(**kwargs)
        return self

    def execute(self):
        return self.session.execute_remove(self)

    def filter(self, *query_expressions):
        self.__query_obj.filter(*query_expressions)
        return self

    def filter_by(self, **filters):
        self.__query_obj.filter_by(**filters)
        return self

    def or_(self, first_qe, *qes):
        self.__query_obj.or_(first_qe, *qes)
        return self

    def in_(self, qfield, *values):
        self.__query_obj.in_(qfield, *values)
        return self

    def nin(self, qfield, *values):
        self.__query_obj.nin(qfield, *values)
        return self
