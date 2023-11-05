"""

A `noalchemy` document is used to define a mapping between a python object
and a document in a Mongo Database.  Mappings are defined by creating a
subclass of :class:`Document` with attributes to define
what maps to what.  The two main types of attributes are :class:`~noalchemy.fields.Field`
and :class:`Index`

A :class:`~noalchemy.fields.Field` is used to define the type of a field in
mongo document, any constraints on the values, and to provide methods for
transforming a value from a python object into something Mongo understands and
vice-versa.

A :class:`~Index` is used to define an index on the underlying collection
programmatically.  A document can have multiple indexes by adding extra
:class:`~Index` attributes


"""

import inspect
from collections import defaultdict

import pymongo
from bson import DBRef

from ..exc import (DocumentException, ExtraValueException, FieldNotRetrieved,
                   MissingValueException)
from ..fields import DocumentField, Field, ObjectIdField

document_type_registry = defaultdict(dict)
collection_registry = defaultdict(dict)


class DocumentMeta(type):
    def __new__(mcs, classname, bases, class_dict):
        class_dict["_subclasses"] = {}
        new_class = super(DocumentMeta, mcs).__new__(mcs, classname, bases, class_dict)

        if new_class.config_extra_fields not in ["error", "ignore"]:
            raise DocumentException(
                "config_extra_fields must be one of: 'error', 'ignore'"
            )

        new_id = False
        for name, value in class_dict.items():
            if not isinstance(value, Field):
                continue
            if value.is_id and name != "mongo_id":
                new_id = True
            value._set_name(name)
            value._set_parent(new_class)

        if new_id:
            new_class.mongo_id = None

        new_class._fields = {}
        for b in bases:
            if not hasattr(b, "get_fields"):
                continue
            for name, field in b.get_fields().items():
                if name == "mongo_id" and new_id:
                    continue
                new_class._fields[name] = field

        for name, maybefield in class_dict.items():
            if not isinstance(maybefield, Field):
                continue
            new_class._fields[name] = maybefield

        for b in bases:
            if "Document" in globals() and issubclass(b, Document):
                b.add_subclass(new_class)
            if not hasattr(b, "config_polymorphic_collection"):
                continue
            if (
                b.config_polymorphic_collection
                and "config_collection_name" not in class_dict
            ):
                new_class.config_collection_name = b.get_collection_name()

        if new_class.config_namespace is not None:
            name = new_class.config_full_name
            if name is None:
                name = new_class.__name__

            ns = new_class.config_namespace
            document_type_registry[ns][name] = new_class

            collection = new_class.get_collection_name()
            current = collection_registry[ns].get(collection)
            if current is None or issubclass(current, new_class):
                collection_registry[ns][collection] = new_class

        return new_class


class Document(metaclass=DocumentMeta):
    mongo_id = ObjectIdField(required=False, db_field="_id", on_update="ignore")

    config_namespace = "global"
    config_polymorphic = None
    config_polymorphic_collection = False
    config_polymorphic_identity = None
    config_full_name = None
    config_default_sort = None
    config_extra_fields = "error"

    def __init__(self, retrieved_fields=None, loading_from_db=False, **kwargs):
        self.partial = retrieved_fields is not None
        self.retrieved_fields = self.__normalize(retrieved_fields)

        self._values = {}
        self.__extra_fields = {}

        cls = self.__class__

        fields = self.get_fields()
        for name, field in fields.items():
            if self.partial and field.db_field not in self.retrieved_fields:
                self._values[name] = Value(field, self, retrieved=False)
            elif name in kwargs:
                field = getattr(cls, name)
                value = kwargs[name]
                self._values[name] = Value(field, self, from_db=loading_from_db)
                field.set_value(self, value)
            elif field.auto:
                self._values[name] = Value(field, self, from_db=False)
            else:
                self._values[name] = Value(field, self, from_db=False)

        for k in kwargs:
            if k not in fields:
                if self.config_extra_fields == "ignore":
                    self.__extra_fields[k] = kwargs[k]
                else:
                    raise ExtraValueException(k)

        self.__extra_fields_orig = dict(self.__extra_fields)

    @classmethod
    def schema_json(cls):
        ret = dict(
            fields={},
            config_namespace=cls.config_namespace,
            config_polymorphic=cls.config_polymorphic,
            config_polymorphic_collection=cls.config_polymorphic_collection,
            config_polymorphic_identity=cls.config_polymorphic_identity,
            config_full_name=cls.config_full_name,
            config_extra_fields=cls.config_extra_fields,
        )
        for f in cls.get_fields():
            ret["fields"][f] = getattr(cls, f).schema_json()
        return ret

    def __deepcopy__(self, memo):
        return type(self).unwrap(self.wrap(), session=self._get_session())

    @classmethod
    def add_subclass(cls, subclass):
        for superclass in inspect.getmro(cls)[1:]:
            if issubclass(superclass, Document):
                superclass.add_subclass(subclass)

        if hasattr(subclass, "config_polymorphic_identity"):
            attr = subclass.config_polymorphic_identity
            cls._subclasses[attr] = subclass

    @classmethod
    def base_query(cls, exclude_subclasses=False):
        if not cls.config_polymorphic:
            return {}
        if exclude_subclasses:
            if cls.config_polymorphic_identity:
                return {cls.config_polymorphic: cls.config_polymorphic_identity}
            return {}
        keys = [key for key in cls._subclasses]
        if cls.config_polymorphic_identity:
            keys.append(cls.config_polymorphic_identity)
        return {cls.config_polymorphic: {"$in": keys}}

    @classmethod
    def get_subclass(cls, obj):
        if cls.config_polymorphic is None:
            return

        value = obj.get(cls.config_polymorphic)
        value = cls._subclasses.get(value)
        if value == cls or value is None:
            return None

        sub_value = value.get_subclass(obj)
        if sub_value is None:
            return value
        return sub_value

    def __eq__(self, other):
        try:
            return self.mongo_id == other.mongo_id
        except:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_dirty_ops(self, with_required=False):
        update_expression = {}
        for name, field in self.get_fields().items():
            if field.db_field == "_id":
                continue
            dirty_ops = field.dirty_ops(self)
            if not dirty_ops and with_required and field.required:
                dirty_ops = field.update_ops(self, force=True)
                if not dirty_ops:
                    raise MissingValueException(name)

            for op, values in dirty_ops.items():
                update_expression.setdefault(op, {})
                for key, value in values.items():
                    update_expression[op][key] = value

        if self.config_extra_fields == "ignore":
            old_extrakeys = set(self.__extra_fields_orig.keys())
            cur_extrakeys = set(self.__extra_fields.keys())

            new_extrakeys = cur_extrakeys - old_extrakeys
            rem_extrakeys = old_extrakeys - cur_extrakeys
            same_extrakeys = cur_extrakeys & old_extrakeys

            update_expression.setdefault("$unset", {})
            for key in rem_extrakeys:
                update_expression["$unset"][key] = True

            update_expression.setdefault("$set", {})
            for key in new_extrakeys:
                update_expression["$set"][key] = self.__extra_fields[key]

            for key in same_extrakeys:
                if self.__extra_fields[key] != self.__extra_fields_orig[key]:
                    update_expression["$set"][key] = self.__extra_fields[key]
            if not update_expression["$unset"]:
                del update_expression["$unset"]

        return update_expression

    def get_extra_fields(self):
        return self.__extra_fields

    @classmethod
    def get_fields(cls):
        return cls._fields

    @classmethod
    def class_name(cls):
        return cls.__name__

    @classmethod
    def get_collection_name(cls):
        if not hasattr(cls, "config_collection_name"):
            return cls.__name__
        return cls.config_collection_name

    @classmethod
    def get_indexes(cls):
        ret = []
        for name in dir(cls):
            field = getattr(cls, name)
            if isinstance(field, Index):
                ret.append(field)
        return ret

    @classmethod
    def transform_incoming(self, obj, session):
        return obj

    @classmethod
    def __normalize(cls, fields):
        if not fields:
            return fields
        ret = {}
        for f in fields:
            strf = str(f)
            if "." in strf:
                first, _, second = strf.partition(".")
                ret.setdefault(first, []).append(second)
            else:
                ret[strf] = None
        return ret

    def has_id(self):
        try:
            getattr(self, "mongo_id")
        except AttributeError:
            return False
        return True

    def to_ref(self, db=None):
        return DBRef(
            id=self.mongo_id, collection=self.get_collection_name(), database=db
        )

    def wrap(self):
        res = {}
        for k, v in self.__extra_fields.items():
            res[k] = v
        cls = self.__class__
        for name in self.get_fields():
            field = getattr(cls, name)
            try:
                value = getattr(self, name)
                res[field.db_field] = field.wrap(value)
            except AttributeError as e:
                if field.required:
                    raise MissingValueException(name)
            except FieldNotRetrieved as fne:
                if field.required:
                    raise
        return res

    @classmethod
    def unwrap(cls, obj, fields=None, session=None):
        """ Returns an instance of this document class based on the mongo object
            ``obj``.  This is done by using the ``unwrap()`` methods of the
            underlying fields to set values.

            :param obj: a ``SON`` object returned from a mongo database
            :param fields: A list of :class:`noalchemy.query.QueryField` objects \
                    for the fields to load.  If ``None`` is passed all fields  \
                    are loaded
        """
        subclass = cls.get_subclass(obj)
        if subclass and subclass != cls:
            unwrapped = subclass.unwrap(obj, fields=fields, session=session)
            unwrapped._session = session
            return unwrapped
        # Get reverse name mapping
        name_reverse = {}
        for name, field in cls.get_fields().items():
            name_reverse[field.db_field] = name
        # Unwrap
        params = {}
        for k, v in obj.items():
            k = name_reverse.get(k, k)
            if not hasattr(cls, k) and cls.config_extra_fields:
                params[str(k)] = v
                continue

            field = getattr(cls, k)
            field_is_doc = fields is not None and isinstance(
                field.get_type(), DocumentField
            )

            extra_unwrap = {}
            if field.has_autoload:
                extra_unwrap["session"] = session
            if field_is_doc:
                normalized_fields = cls.__normalize(fields)
                unwrapped = field.unwrap(
                    v, fields=normalized_fields.get(k), **extra_unwrap
                )
            else:
                unwrapped = field.unwrap(v, **extra_unwrap)
            unwrapped = field.localize(session, unwrapped)
            params[str(k)] = unwrapped

        if fields is not None:
            params["retrieved_fields"] = fields
        obj = cls(loading_from_db=True, **params)
        obj._mark_clean()
        obj._session = session
        return obj

    _session = None

    def _get_session(self):
        return self._session

    def _set_session(self, session):
        self._session = session

    def _mark_clean(self):
        for k, v in self._values.items():
            v.clear_dirty()


class DictDoc(object):
    """Adds a mapping interface to a document. Supports __getitem__ and
    __contains__. Both methods will only retrieve values assigned to
    a field, not methods or other attributes.
    """

    def __getitem__(self, name):
        """Gets the field name from the document"""
        if name in self._values:
            return getattr(self, name)
        raise KeyError(name)

    def __setitem__(self, name, value):
        """Sets the field name on the document"""
        setattr(self, name, value)

    def setdefault(self, name, value):
        """if the name is set, return its value. Otherwise, set name to
        value and return value"""
        if name in self:
            return self[name]
        self[name] = value
        return self[name]

    def __contains__(self, name):
        """Return whether a field is present. Fails if name is not a
        field or name is not set on the document or if name was
        not a field retrieved from the database
        """
        try:
            self[name]
        except FieldNotRetrieved:
            return False
        except AttributeError:
            return False
        except KeyError:
            return False
        return True


class BadIndexException(Exception):
    pass


class Index(object):
    """This class is used in the class definition of a Document to
    specify a single, possibly compound, index. pymongo's ensure_index
    will be called on each index before a database operation is executed
    on the owner document class.

    Example

        class Donor(Document):
            name = StringField()
            age = IntField(min_value=0)
            blood_type = StringField()

            i_name = Index().ascending('name')
            type_age = Index().ascending('blood_type').descending('age')
    """

    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING

    def __init__(self):
        self.components = []
        self.__unique = False
        self.__drop_dups = False
        self.__min = None
        self.__max = None
        self.__bucket_size = None
        self.__expire_after = None

    def expire(self, after):
        """Add an expire after option to the index

        :param after: Number of seconds before expiration
        """
        self.__expire_after = after
        return self

    def ascending(self, name):
        """Add a descending index for name to this index.

        :param name: Name to be used in the index
        """
        self.components.append((name, Index.ASCENDING))
        return self

    def descending(self, name):
        """Add a descending index for name to this index.

        :param name: Name to be used in the index
        """
        self.components.append((name, Index.DESCENDING))
        return self

    def geo2d(self, name, min=None, max=None):
        """Create a 2d index. See:
        http://www.mongodb.org/display/DOCS/Geospatial+Indexing

        :param name: Name of the indexed column
        :param min: minimum value for the index
        :param max: minimum value for the index
        """
        self.components.append((name, pymongo.GEO2D))
        self.__min = min
        self.__max = max
        return self

    def geo_haystack(self, name, bucket_size):
        """Create a Haystack index. See:
        http://www.mongodb.org/display/DOCS/Geospatial+Haystack+Indexing

        :param name: Name of the indexed column
        :param bucket_size: Size of the haystack buckets (see mongo docs)
        """
        self.components.append((name, "geoHaystack"))
        self.__bucket_size = bucket_size
        return self

    def unique(self, drop_dups=False):
        """Make this index unique, optionally dropping duplicate entries.

        :param drop_dups: Drop duplicate objects while creating the unique
            index? Default to False
        """
        self.__unique = True
        if drop_dups and pymongo.version_tuple >= (2, 7, 5):
            raise BadIndexException("drop_dups is not supported on pymongo >= 2.7.5")
        self.__drop_dups = drop_dups
        return self

    def ensure(self, collection):
        """Call the pymongo method ensure_index on the passed collection.

        :param collection: the pymongo collection to ensure this index is on
        """
        components = []
        for c in self.components:
            if isinstance(c[0], Field):
                c = (c[0].db_field, c[1])
            components.append(c)

        extras = {}
        if self.__min is not None:
            extras["min"] = self.__min
        if self.__max is not None:
            extras["max"] = self.__max
        if self.__bucket_size is not None:
            extras["bucket_size"] = self.__bucket_size
        if self.__expire_after is not None:
            extras["expireAfterSeconds"] = self.__expire_after

        collection.ensure_index(
            components, unique=self.__unique, drop_dups=self.__drop_dups, **extras
        )
        return self


class Value(object):
    def __init__(self, field, document, from_db=False, extra=False, retrieved=True):
        self.field = field
        self.doc = document
        self.value = None

        self.from_db = from_db
        self.set = False
        self.extra = extra
        self.dirty = False
        self.retrieved = retrieved
        self.update_op = None

    def clear_dirty(self):
        self.dirty = False
        self.update_op = None

    def delete(self):
        self.value = None
        self.set = False
        self.dirty = True
        self.from_db = False
        self.update_op = "$unset"
