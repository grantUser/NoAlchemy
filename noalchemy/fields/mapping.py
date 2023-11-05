from __future__ import print_function

from ..exc import BadFieldSpecification, BadValueException
from .base import SCALAR_MODIFIERS, Field


class DictField(Field):
    """Stores string to 'value_type' dictionaries. For non-string keys, use 'KVField'.
    Strings must also obey the MongoDB key rules (no '.' or '$')."""

    valid_modifiers = SCALAR_MODIFIERS

    def __init__(self, value_type, default_empty=False, **kwargs):
        """Initialize the DictField.

        Args:
            value_type (Field): The field type to use for the values.
            default_empty (bool, optional): Whether to set an empty dictionary as the default value. Defaults to False.
            **kwargs: Additional keyword arguments.
        """
        if default_empty:
            kwargs["default_f"] = dict
        super(DictField, self).__init__(**kwargs)
        self.value_type = value_type
        self.default_empty = default_empty

        if not isinstance(value_type, Field):
            raise BadFieldSpecification("DictField value type is not a field!")

    def schema_json(self):
        super_schema = super(DictField, self).schema_json()
        return {
            "value_type": self.value_type.schema_json(),
            "default_empty": self.default_empty,
            **super_schema,
        }

    @property
    def has_autoload(self):
        return self.value_type.has_autoload

    def set_parent_on_subtypes(self, parent):
        self.value_type._set_parent(parent)

    def _validate_key_wrap(self, key):
        if not isinstance(key, str):
            self._fail_validation(key, "DictField keys must be of type str")
        if "." in key or "$" in key:
            self._fail_validation(
                key,
                'DictField keys cannot contain "." or "$". You may want a KVField instead',
            )

    def _validate_key_unwrap(self, key):
        self._validate_key_wrap(key)

    def validate_unwrap(self, value):
        """Checks that value is a dictionary, that every key is a valid MongoDB key,
        and that every value validates based on DictField.value_type.
        """
        if not isinstance(value, dict):
            self._fail_validation_type(value, dict)
        for k, v in value.items():
            self._validate_key_unwrap(k)
            try:
                self.value_type.validate_unwrap(v)
            except BadValueException as bve:
                self._fail_validation(value, "Bad value for key %s" % k, cause=bve)

    def validate_wrap(self, value):
        """Checks that value is a dictionary, that every key is a valid MongoDB key,
        and that every value validates based on DictField.value_type.
        """
        if not isinstance(value, dict):
            self._fail_validation_type(value, dict)
        for k, v in value.items():
            self._validate_key_wrap(k)
            try:
                self.value_type.validate_wrap(v)
            except BadValueException as bve:
                self._fail_validation(value, "Bad value for key %s" % k, cause=bve)

    def wrap(self, value):
        """Validates 'value' and returns a dictionary with each key in 'value' mapped to
        its value wrapped with DictField.value_type.
        """
        self.validate_wrap(value)
        ret = {}
        for k, v in value.items():
            ret[k] = self.value_type.wrap(v)
        return ret

    def unwrap(self, value, session=None):
        """Validates 'value' and returns a dictionary with each key in 'value' mapped to
        its value unwrapped using DictField.value_type.
        """
        self.validate_unwrap(value)
        ret = {}
        for k, v in value.items():
            ret[k] = self.value_type.unwrap(v, session=session)
        return ret


class KVField(DictField):
    """Like a DictField, except it allows arbitrary keys. The DB format for a 'KVField'
    is '[{ 'k': key, 'v': value }, ...]'. Queries on keys and values can be done with 'k' and 'v'.
    """

    has_subfields = True

    def __init__(self, key_type, value_type, default_empty=False, **kwargs):
        """Initialize the KVField.

        Args:
            key_type (Field): The field type to use for the keys.
            value_type (Field): The field type to use for the values.
            default_empty (bool, optional): Whether to set an empty dictionary as the default value. Defaults to False.
            **kwargs: Additional keyword arguments.
        """
        if default_empty:
            kwargs["default_f"] = dict
        super(KVField, self).__init__(value_type, **kwargs)
        self.default_empty = default_empty

        if not isinstance(key_type, Field):
            raise BadFieldSpecification("KVField key type is not a field!")
        self.key_type = key_type
        self.key_type._name = "k"
        self.value_type = value_type
        self.value_type._name = "v"

    def schema_json(self):
        super_schema = super(KVField, self).schema_json()
        return {"key_type": self.key_type.schema_json(), **super_schema}

    def set_parent_on_subtypes(self, parent):
        self.value_type._set_parent(parent)
        self.key_type._set_parent(parent)

    @property
    def has_autoload(self):
        return self.value_type.has_autoload or self.key_type.has_autoload

    def subfields(self):
        """Returns the k and v subfields, which can be accessed to do queries
        based on either of them.
        """
        return {
            "k": self.key_type,
            "v": self.value_type,
        }

    def _validate_key_wrap(self, key):
        try:
            self.key_type.validate_wrap(key)
        except BadValueException as bve:
            self._fail_validation(key, "Bad value for key", cause=bve)

    def validate_unwrap(self, value):
        """Expects a list of dictionaries with 'k' and 'v' set to the keys and values that
        will be unwrapped into the output Python dictionary.
        """
        if not isinstance(value, list):
            self._fail_validation_type(value, list)
        for value_dict in value:
            if not isinstance(value_dict, dict):
                cause = BadValueException(
                    "", value_dict, "Values in a KVField list must be dicts"
                )
                self._fail_validation(
                    value, "Values in a KVField list must be dicts", cause=cause
                )
            k = value_dict.get("k")
            v = value_dict.get("v")
            if k is None:
                self._fail_validation(value, "Value had None for a key")
            try:
                self.key_type.validate_unwrap(k)
            except BadValueException as bve:
                self._fail_validation(
                    value, "Bad value for KVField key %s" % k, cause=bve
                )

            try:
                self.value_type.validate_unwrap(v)
            except BadValueException as bve:
                self._fail_validation(
                    value, "Bad value for KVField value %s" % k, cause=bve
                )
        return True

    def wrap(self, value):
        """Expects a dictionary with the keys being instances of 'KVField.key_type'
        and the values being instances of 'KVField.value_type'. After validation, the dictionary
        is transformed into a list of dictionaries with 'k' and 'v' fields set to the keys and
        values from the original dictionary.
        """
        self.validate_wrap(value)
        ret = []
        for k, v in value.items():
            k = self.key_type.wrap(k)
            v = self.value_type.wrap(v)
            ret.append({"k": k, "v": v})
        return ret

    def unwrap(self, value, session=None):
        """Expects a list of dictionaries with 'k' and 'v' set to the keys and values that
        will be unwrapped into the output Python dictionary. Validates the input and then
        constructs the dictionary from the list.
        """
        self.validate_unwrap(value)
        ret = {}
        for value_dict in value:
            k = value_dict["k"]
            v = value_dict["v"]
            ret[self.key_type.unwrap(k, session=session)] = self.value_type.unwrap(
                v, session=session
            )
        return ret
