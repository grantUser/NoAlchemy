from functools import wraps

from ..util import resolve_name
from .query_expression import QueryExpression, flatten


class UpdateExpression:
    def __init__(self, query):
        self.query = query
        self.session = query.session
        self.update_data = {}
        self.__upsert = False
        self.__multi = False
        self.__safe = False

    def upsert(self):
        """If a document matching the query doesn't exist, create one"""
        self.__upsert = True
        return self

    def multi(self):
        """Update multiple documents. The Mongo default is to only update
        the first matching document"""
        self.__multi = True
        return self

    def safe(self, safe=True):
        """Mark the query as a "safe" query with pymongo.

        :param safe: Defaults to True. Force "safe" on or off
        """
        self.__safe = safe
        return self

    def set(self, *args, **kwargs):
        """Usage is either:
        set(self, qfield, value): Atomically set qfield to value
        OR
        set(key1=value1, key2=value2): Atomically set the named arguments
        on the current object to the values given. This form cannot
        update a sub-document
        """
        if len(args) == 2:
            qfield, value = args
            return self._atomic_op("$set", qfield, value)
        elif kwargs:
            ret = self
            for key, value in kwargs.items():
                ret = ret._atomic_op("$set", key, value)
            return ret
        else:
            raise UpdateException(
                "Invalid arguments for set. Requires either two positional arguments or at least one keyword argument"
            )

    def unset(self, qfield):
        """Atomically delete the field qfield"""
        return self._atomic_generic_op("$unset", qfield, True)

    def inc(self, *args, **kwargs):
        """Atomically increment qfield by value"""
        pairs = []
        if len(args) == 1:
            pairs.append((args[0], 1))
        elif len(args) == 2:
            pairs.append(args)
        elif kwargs:
            pairs.extend(kwargs.items())
        else:
            raise UpdateException(
                "Invalid arguments for inc. Requires either two positional arguments or at least one keyword argument"
            )

        ret = self
        for qfield, value in pairs:
            ret = self._atomic_op("$inc", qfield, value)
        return ret

    def append(self, qfield, value):
        """Atomically append value to qfield. The operation will
        fail if the field is not a list field"""
        return self._atomic_list_op("$push", qfield, value)

    def extend(self, qfield, *values):
        """Atomically append each value in values to the field qfield"""
        return self._atomic_list_op_multivalue("$pushAll", qfield, *values)

    def remove(self, qfield, value):
        """Atomically remove value from qfield"""
        if isinstance(value, QueryExpression):
            return self._atomic_expression_op("$pull", qfield, value)
        return self._atomic_list_op("$pull", qfield, value)

    def remove_all(self, qfield, *values):
        """Atomically remove each value in values from qfield"""
        return self._atomic_list_op_multivalue("$pullAll", qfield, *values)

    def add_to_set(self, qfield, value):
        """Atomically add value to qfield. The field represented by
        qfield must be a set

        .. note: Requires server version >= 1.3.0+.
        """
        return self._atomic_list_op("$addToSet", qfield, value)

    def pop_last(self, qfield):
        """Atomically pop the last item in qfield.
        .. note: Requires version 1.1+"""
        return self._atomic_generic_op("$pop", qfield, 1)

    def pop_first(self, qfield):
        """Atomically pop the first item in qfield.
        .. note: Requires version 1.1+"""
        return self._atomic_generic_op("$pop", qfield, -1)

    def _atomic_list_op_multivalue(self, op, qfield, *values):
        qfield = resolve_name(self.query.type, qfield)
        if op not in qfield.valid_modifiers:
            raise InvalidModifierException(qfield, op)
        wrapped = [qfield.get_type().item_type.wrap(v) for v in values]
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_absolute_name()] = wrapped
        return self

    def _atomic_list_op(self, op, qfield, value):
        qfield = resolve_name(self.query.type, qfield)
        if op not in qfield.valid_modifiers:
            raise InvalidModifierException(qfield, op)

        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_absolute_name()] = qfield.child_type().wrap(
            value
        )
        return self

    def _atomic_expression_op(self, op, qfield, value):
        qfield = resolve_name(self.query.type, qfield)
        if op not in qfield.valid_modifiers:
            raise InvalidModifierException(qfield, op)

        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_absolute_name()] = flatten(value.obj)
        return self

    def _atomic_op(self, op, qfield, value):
        qfield = resolve_name(self.query.type, qfield)
        if op not in qfield.valid_modifiers:
            raise InvalidModifierException(qfield, op)

        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_absolute_name()] = qfield.wrap(value)
        return self

    def _atomic_generic_op(self, op, qfield, value):
        qfield = resolve_name(self.query.type, qfield)
        if op not in qfield.valid_modifiers:
            raise InvalidModifierException(qfield, op)

        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_absolute_name()] = value
        return self

    def _get_upsert(self):
        return self.__upsert

    def _get_multi(self):
        return self.__multi

    def execute(self):
        """Execute the update expression on the database"""
        self.session.execute_update(self, safe=self.__safe)


class FindAndModifyExpression(UpdateExpression):
    def __init__(self, query, new, remove):
        self.__new = new
        self.__remove = remove
        super().__init__(query)

    def _get_remove(self):
        return self.__remove

    def _get_new(self):
        return self.__new

    def execute(self):
        """Execute the find and modify expression on the database"""
        return self.session.execute_find_and_modify(self)


class UpdateException(Exception):
    """Base class for exceptions related to updates"""

    pass


class InvalidModifierException(UpdateException):
    """Exception raised if a modifier was used which isn't valid for a field"""

    def __init__(self, field, op):
        super().__init(f"Invalid modifier for {field.__class__.__name__} field: {op}")


class ConflictingModifierException(UpdateException):
    """Exception raised if conflicting modifiers are being used in the
    update expression"""

    pass
