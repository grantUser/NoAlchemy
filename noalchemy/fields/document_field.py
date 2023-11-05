from ..exc import BadFieldSpecification
from .base import *


class DocumentField(Field):
    """A field which wraps a :class:`Document`"""

    has_subfields = True
    has_autoload = True

    def __init__(self, document_class, **kwargs):
        super().__init__(**kwargs)
        self.__type = document_class

    def schema_json(self):
        super_schema = super().schema_json()
        subtype = str(self.type.__name__)
        namespace = self.type.config_namespace
        type = "%s:%s" % (namespace, subtype)
        return dict(subtype=type, **super_schema)

    @property
    def type(self):
        from ..odm.document import Document, document_type_registry

        if not isinstance(self.__type, str) and issubclass(self.__type, Document):
            return self.__type
        if self.parent and self.parent.config_namespace is None:
            raise BadFieldSpecification(
                "Document namespace is None.  Strings are not allowed for DocumentFields"
            )
        type = document_type_registry[self.parent.config_namespace].get(self.__type)
        if type is None or not issubclass(type, Document):
            raise BadFieldSpecification(
                "No type found for %s.  Maybe it has not been imported yet and is not registered?"
                % self.__type
            )
        return type

    def dirty_ops(self, instance):
        """Returns a dict of the operations needed to update this object.
        See :func:`Document.get_dirty_ops` for more details."""
        obj_value = instance._values[self._name]
        if not obj_value.set:
            return {}

        if not obj_value.dirty and self.__type.config_extra_fields != "ignore":
            return {}

        ops = obj_value.value.get_dirty_ops()

        ret = {}
        for op, values in ops.items():
            ret[op] = {}
            for key, value in values.items():
                name = "%s.%s" % (self._name, key)
                ret[op][name] = value
        return ret

    def subfields(self):
        """Returns the fields that can be retrieved from the enclosed
        document.  This function is mainly used internally"""
        return self.type.get_fields()

    def sub_type(self):
        return self.type

    def is_valid_unwrap(self, value, fields=None):
        """ Always True.  Document-level validation errors will
            be handled during unwrappingself.

            :param value: The value to validate
            :param fields: The fields being returned if this is a partial \
                document. They will be ignored when validating the fields \
                of ``value``
        """
        return True

    def wrap(self, value):
        """Validate ``value`` and then use the document's class to wrap the
        value"""
        self.validate_wrap(value)
        return self.type.wrap(value)

    def unwrap(self, value, fields=None, session=None):
        """Validate ``value`` and then use the document's class to unwrap the
        value"""
        self.validate_unwrap(value, fields=fields, session=session)
        return self.type.unwrap(value, fields=fields, session=session)

    def validate_wrap(self, value):
        """Checks that ``value`` is an instance of ``DocumentField.type``.
        if it is, then validation on its fields has already been done and
        no further validation is needed.
        """
        if not isinstance(value, self.type):
            self._fail_validation_type(value, self.type)

    def validate_unwrap(self, value, fields=None, session=None):
        """Validates every field in the underlying document type.  If ``fields``
        is not ``None``, only the fields in ``fields`` will be checked.
        """
        return
