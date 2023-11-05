from __future__ import print_function

from bson import DBRef, ObjectId

from .base import Field


class RefBase(Field):
    def rel(self, allow_none=False):
        return Proxy(self, allow_none=allow_none)


class SRefField(RefBase):
    has_subfields = True
    has_autoload = True

    def __init__(self, type, db=None, **kwargs):
        from noalchemy.fields import DocumentField

        super(SRefField, self).__init__(**kwargs)

        self.type = type
        if not isinstance(type, DocumentField):
            self.type = DocumentField(type)
        self.db = db

    def schema_json(self):
        super_schema = super(SRefField, self).schema_json()
        return {"subtype": self.type.schema_json(), "db": self.db, **super_schema}

    def _to_ref(self, doc):
        return doc.mongo_id

    def dereference(self, session, ref, allow_none=False):
        ref = DBRef(
            id=ref, collection=self.type.type.get_collection_name(), database=self.db
        )
        ref.type = self.type.type
        return session.dereference(ref, allow_none=allow_none)

    def set_parent_on_subtypes(self, parent):
        self.type.parent = parent

    def wrap(self, value):
        self.validate_wrap(value)
        return value

    def unwrap(self, value, fields=None, session=None):
        self.validate_unwrap(value)
        return value

    def validate_unwrap(self, value, session=None):
        if not isinstance(value, ObjectId):
            self._fail_validation_type(value, ObjectId)

    validate_wrap = validate_unwrap


class RefField(RefBase):
    has_subfields = True
    has_autoload = True

    def __init__(
        self, type=None, db=None, db_required=False, namespace="global", **kwargs
    ):
        from noalchemy.fields import DocumentField

        if type and not isinstance(type, DocumentField):
            type = DocumentField(type)

        super(RefField, self).__init__(**kwargs)
        self.db_required = db_required
        self.type = type
        self.namespace = namespace
        self.db = db
        self.parent = None

    def schema_json(self):
        super_schema = super(RefField, self).schema_json()
        subtype = self.type
        if subtype is not None:
            subtype = subtype.schema_json()
        return {
            "db_required": self.db_required,
            "subtype": subtype,
            "namespace": self.namespace,
            "db": self.db,
            **super_schema,
        }

    def wrap(self, value):
        self.validate_wrap(value)
        value.type = self.type
        return value

    def _to_ref(self, doc):
        return doc.to_ref(db=self.db)

    def unwrap(self, value, fields=None, session=None):
        self.validate_unwrap(value)
        value.type = self.type
        return value

    def dereference(self, session, ref, allow_none=False):
        from ..odm.document import collection_registry

        ref.type = collection_registry["global"][ref.collection]
        obj = session.dereference(ref, allow_none=allow_none)
        return obj

    def set_parent_on_subtypes(self, parent):
        if self.type:
            self.type.parent = parent

    def validate_unwrap(self, value, session=None):
        if not isinstance(value, DBRef):
            self._fail_validation_type(value, DBRef)
        if self.type:
            expected = self.type.type.get_collection_name()
            got = value.collection
            if expected != got:
                self._fail_validation(
                    value,
                    """Wrong collection for reference: """
                    """got "%s" instead of "%s" """ % (got, expected),
                )
        if self.db_required and not value.database:
            self._fail_validation(value, "db_required=True, but not database specified")
        if self.db and value.database and self.db != value.database:
            self._fail_validation(
                value,
                """Wrong database for reference: """
                """ got "%s" instead of "%s" """ % (value.database, self.db),
            )

    validate_wrap = validate_unwrap


class Proxy(object):
    def __init__(self, field, allow_none=False):
        self.allow_none = allow_none
        self.field = field

    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        session = instance._get_session()
        ref = getattr(instance, self.field._name)
        if ref is None:
            return None
        return self.field.dereference(session, ref, allow_none=self.allow_none)

    def __set__(self, instance, value):
        assert instance is not None
        setattr(instance, self.field._name, self.field._to_ref(value))
