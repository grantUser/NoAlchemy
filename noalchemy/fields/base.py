import functools

from ..exc import BadValueException, FieldNotRetrieved, InvalidConfigException
from ..odm.query_expression import QueryField
from ..util import UNSET

SCALAR_MODIFIERS = {"$set", "$unset"}
NUMBER_MODIFIERS = SCALAR_MODIFIERS | {"$inc"}
LIST_MODIFIERS = SCALAR_MODIFIERS | {
    "$push",
    "$addToSet",
    "$pull",
    "$pushAll",
    "$pullAll",
    "$pop",
}
ANY_MODIFIER = LIST_MODIFIERS | NUMBER_MODIFIERS


class FieldMeta(type):
    def __new__(mcs, classname, bases, class_dict):
        def wrap_unwrap_wrapper(fun):
            def wrapped(self, value, *args, **kwds):
                if self._allow_none and value is None:
                    return None
                return fun(self, value, *args, **kwds)

            functools.update_wrapper(wrapped, fun, ("__name__", "__doc__"))
            return wrapped

        def validation_wrapper(fun, kind):
            def wrapped(self, value, *args, **kwds):
                if self._allow_none and value is None:
                    return
                fun(self, value, *args, **kwds)

                if self.validator:
                    if self.validator(value) is False:
                        self._fail_validation(value, "user-supplied validator failed")

                if kind == "unwrap" and self.unwrap_validator:
                    if self.unwrap_validator(value) is False:
                        self._fail_validation(
                            value, "user-supplied unwrap_validator failed"
                        )

                elif kind == "wrap" and self.wrap_validator:
                    if self.wrap_validator(value) is False:
                        self._fail_validation(
                            value, "user-supplied wrap_validator failed"
                        )

            functools.update_wrapper(wrapped, fun, ("__name__", "__doc__"))
            return wrapped

        if "wrap" in class_dict:
            class_dict["wrap"] = wrap_unwrap_wrapper(class_dict["wrap"])
        if "unwrap" in class_dict:
            class_dict["unwrap"] = wrap_unwrap_wrapper(class_dict["unwrap"])

        if "validate_wrap" in class_dict:
            class_dict["validate_wrap"] = validation_wrapper(
                class_dict["validate_wrap"], "wrap"
            )

        if "validate_unwrap" in class_dict:
            class_dict["validate_unwrap"] = validation_wrapper(
                class_dict["validate_unwrap"], "unwrap"
            )

        return super(FieldMeta, mcs).__new__(mcs, classname, bases, class_dict)


class Field(metaclass=FieldMeta):
    auto = False
    has_subfields = False
    has_autoload = False
    is_sequence_field = False
    no_real_attributes = False

    valid_modifiers = SCALAR_MODIFIERS

    def __init__(
        self,
        required=True,
        default=UNSET,
        default_f=None,
        db_field=None,
        allow_none=False,
        on_update="$set",
        validator=None,
        unwrap_validator=None,
        wrap_validator=None,
        _id=False,
        proxy=None,
        iproxy=None,
        ignore_missing=False,
    ):
        if _id and db_field is not None:
            raise InvalidConfigException(
                "Cannot set db_field and _id on the same Field"
            )
        if _id:
            self.__db_field = "_id"
        else:
            self.__db_field = db_field
        self.is_id = self.__db_field == "_id"
        self.__value = UNSET
        self.__update_op = UNSET

        self.proxy = proxy
        self.iproxy = iproxy
        self.ignore_missing = ignore_missing

        self.validator = validator
        self.unwrap_validator = unwrap_validator
        self.wrap_validator = wrap_validator

        self._allow_none = allow_none

        self.required = required
        self._default = default
        self._default_f = default_f
        if self._default_f and self._default != UNSET:
            raise InvalidConfigException("Only one of default and default_f is allowed")

        if default is None:
            self._allow_none = True
        self._owner = None

        if on_update not in self.valid_modifiers and on_update != "ignore":
            raise InvalidConfigException("Unsupported update operation: %s" % on_update)
        self.on_update = on_update

        self._name = "Unbound_%s" % self.__class__.__name__

    @property
    def default(self):
        if self._default_f:
            return self._default_f()
        return self._default

    def schema_json(self):
        schema = dict(
            type=type(self).__name__,
            required=self.required,
            db_field=self.__db_field,
            allow_none=self._allow_none,
            on_update=self.on_update,
            validator_set=self.validator is not None,
            unwrap_validator=self.unwrap_validator is not None,
            wrap_validator=self.wrap_validator is not None,
            ignore_missing=self.ignore_missing,
        )
        if self._default == UNSET and self._default_f is None:
            schema["default_unset"] = True
        elif self._default_f:
            schema["default_f"] = repr(self._default_f)
        else:
            schema["default"] = self.wrap(self._default)
        return schema

    def __get__(self, instance, owner):
        if instance is None:
            return QueryField(self)
        obj_value = instance._values[self._name]

        if obj_value.set:
            return instance._values[self._name].value

        if self._default_f:
            self.set_value(instance, self._default_f())
            return instance._values[self._name].value
        elif self._default is not UNSET:
            self.set_value(instance, self._default)
            return instance._values[self._name].value

        if not obj_value.retrieved:
            raise FieldNotRetrieved(self._name)

        raise AttributeError(self._name)

    def __set__(self, instance, value):
        self.set_value(instance, value)

    def set_value(self, instance, value):
        self.validate_wrap(value)
        obj_value = instance._values[self._name]
        obj_value.value = value
        obj_value.dirty = True
        obj_value.set = True
        obj_value.from_db = False
        if self.on_update != "ignore":
            obj_value.update_op = self.on_update

    def dirty_ops(self, instance):
        obj_value = instance._values[self._name]

        if obj_value.update_op == "$unset":
            return {"$unset": {self._name: True}}
        if obj_value.update_op is None:
            return {}
        return {obj_value.update_op: {self.db_field: self.wrap(obj_value.value)}}

    def __delete__(self, instance):
        obj_value = instance._values[self._name]
        if not obj_value.set:
            raise AttributeError(self._name)
        obj_value.delete()

    def update_ops(self, instance, force=False):
        obj_value = instance._values[self._name]
        if obj_value.set and (obj_value.dirty or force):
            return {self.on_update: {self._name: self.wrap(obj_value.value)}}
        return {}

    def localize(self, session, value):
        return value

    @property
    def db_field(self):
        if self.__db_field is not None:
            return self.__db_field
        return self._name

    def wrap_value(self, value):
        return self.wrap(value)

    def _set_name(self, name):
        self._name = name

    def _set_parent(self, parent):
        self.parent = parent
        self.set_parent_on_subtypes(parent)

    def set_parent_on_subtypes(self, parent):
        pass

    def wrap(self, value):
        raise NotImplementedError()

    def unwrap(self, value, session=None):
        raise NotImplementedError()

    def validate_wrap(self, value):
        raise NotImplementedError()

    def validate_unwrap(self, value):
        self.validate_wrap(value)

    def _fail_validation(self, value, reason="", cause=None):
        raise BadValueException(self._name, value, reason, cause=cause)

    def _fail_validation_type(self, value, *type):
        types = "\n".join([str(t) for t in type])
        got = value.__class__.__name__
        raise BadValueException(
            self._name, value, "Value is not an instance of %s (got: %s)" % (types, got)
        )

    def is_valid_wrap(self, value):
        try:
            self.validate_wrap(value)
        except BadValueException:
            return False
        return True

    def is_valid_unwrap(self, value):
        try:
            self.validate_unwrap(value)
        except BadValueException:
            return False
        return True
