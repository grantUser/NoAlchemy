from __future__ import print_function

from ..exc import BadFieldSpecification
from .base import LIST_MODIFIERS, BadValueException, Field


class SequenceField(Field):
    is_sequence_field = True
    valid_modifiers = LIST_MODIFIERS

    def __init__(
        self,
        item_type,
        min_capacity=None,
        max_capacity=None,
        default_empty=False,
        **kwargs
    ):
        super(SequenceField, self).__init__(**kwargs)
        self.item_type = item_type
        self.min = min_capacity
        self.max = max_capacity
        self.default_empty = default_empty
        if not isinstance(item_type, Field):
            raise BadFieldSpecification("List item_type is not a field!")

    def schema_json(self):
        super_schema = super(SequenceField, self).schema_json()
        return {
            "item_type": self.item_type.schema_json(),
            "min_capacity": self.min,
            "max_capacity": self.max,
            "default_empty": self.default_empty,
            **super_schema,
        }

    @property
    def has_subfields(self):
        return self.item_type.has_subfields

    @property
    def has_autoload(self):
        return self.item_type.has_autoload

    def set_parent_on_subtypes(self, parent):
        self.item_type._set_parent(parent)

    def subfields(self):
        return self.item_type.subfields()

    def _dereference(self, session, ref, allow_none=False):
        return self.item_type.dereference(session, ref, allow_none=allow_none)

    def wrap_value(self, value):
        try:
            return self.item_type.wrap_value(value)
        except BadValueException:
            pass
        try:
            return self.wrap(value)
        except BadValueException:
            pass
        self._fail_validation(
            value,
            "Could not wrap value as the correct type.  Tried %s and %s"
            % (self.item_type, self),
        )

    def child_type(self):
        return self.item_type

    def _validate_child_wrap(self, value):
        self.item_type.validate_wrap(value)

    def _validate_child_unwrap(self, value, session=None):
        if self.has_autoload:
            self.item_type.validate_unwrap(value, session=session)
        else:
            self.item_type.validate_unwrap(value)

    def _length_valid(self, value):
        if self.min is not None and len(value) < self.min:
            self._fail_validation(value, "Value has too few elements")
        if self.max is not None and len(value) > self.max:
            self._fail_validation(value, "Value has too many elements")

    def validate_wrap(self, value):
        self._validate_wrap_type(value)
        self._length_valid(value)
        for v in value:
            self._validate_child_wrap(v)

    def validate_unwrap(self, value, session=None):
        self._validate_unwrap_type(value)
        self._length_valid(value)
        for v in value:
            if self.has_autoload:
                self._validate_child_unwrap(v, session=session)
            else:
                self._validate_child_unwrap(v)

    def set_value(self, instance, value):
        super(SequenceField, self).set_value(instance, value)

    def dirty_ops(self, instance):
        obj_value = instance._values[self._name]
        ops = super(SequenceField, self).dirty_ops(instance)
        if len(ops) == 0 and obj_value.set:
            ops = {"$set": {self.db_field: self.wrap(obj_value.value)}}
        return ops


class ListField(SequenceField):
    def __init__(self, item_type, **kwargs):
        if kwargs.get("default_empty"):
            kwargs["default_f"] = list
        super(ListField, self).__init__(item_type, **kwargs)

    def rel(self, ignore_missing=False):
        from noalchemy.fields import RefBase

        assert isinstance(self.item_type, RefBase)
        return ListProxy(self, ignore_missing=ignore_missing)

    def _validate_wrap_type(self, value):
        import types

        if not any(
            [
                isinstance(value, list),
                isinstance(value, tuple),
                isinstance(value, types.GeneratorType),
            ]
        ):
            self._fail_validation_type(value, list, tuple)

    _validate_unwrap_type = _validate_wrap_type

    def wrap(self, value):
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]

    def unwrap(self, value, session=None):
        kwargs = {}
        if self.has_autoload:
            kwargs["session"] = session
        self.validate_unwrap(value, **kwargs)
        return [self.item_type.unwrap(v, **kwargs) for v in value]


class SetField(SequenceField):
    def __init__(self, item_type, **kwargs):
        if kwargs.get("default_empty"):
            kwargs["default_f"] = set
        super(SetField, self).__init__(item_type, **kwargs)

    def rel(self, ignore_missing=False):
        return ListProxy(self, ignore_missing=ignore_missing)

    def _validate_wrap_type(self, value):
        if not isinstance(value, set):
            self._fail_validation_type(value, set)

    def _validate_unwrap_type(self, value):
        if not isinstance(value, list):
            self._fail_validation_type(value, list)

    def wrap(self, value):
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]

    def unwrap(self, value, session=None):
        self.validate_unwrap(value)
        return set([self.item_type.unwrap(v, session=session) for v in value])


class ListProxy(object):
    def __init__(self, field, ignore_missing=False):
        self.field = field
        self.ignore_missing = ignore_missing

    def __get__(self, instance, owner):
        if instance is None:
            return getattr(owner, self.field._name)
        session = instance._get_session()

        def iterator():
            for v in getattr(instance, self.field._name):
                if v is None:
                    yield v
                    continue
                value = self.field._dereference(
                    session, v, allow_none=self.ignore_missing
                )
                if value is None and self.ignore_missing:
                    continue
                yield value

        return iterator()
