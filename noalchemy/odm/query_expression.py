import re


class BadQueryException(Exception):
    """Raised when a method would result in a query which is not well-formed."""

    pass


class FreeFormField:
    has_subfields = True
    no_real_attributes = True

    def __init__(self, name=None):
        self.__name = name
        self.db_field = name

    def __getattr__(self, name):
        return FreeFormField(name=name)

    def __getitem__(self, name):
        return getattr(self, name)

    @classmethod
    def wrap_value(cls, value):
        return value

    def subfields(self):
        return FreeFormField(name=None)

    def is_valid_wrap(*args):
        return True

    is_valid_unwrap = is_valid_wrap
    __contains__ = is_valid_wrap


class FreeFormDoc:
    config_default_sort = None

    def __init__(self, name):
        self.__name = name

    def __getattr__(self, name):
        return QueryField(FreeFormField(name))

    @classmethod
    def base_query(cls, *args, **kwargs):
        return {}

    @classmethod
    def unwrap(cls, value, *args, **kwargs):
        return value

    def get_collection_name(self):
        return self.__name

    def transform_incoming(self, obj, session):
        return obj

    def get_indexes(self):
        return []

    mongo_id = FreeFormField(name="_id")


Q = FreeFormDoc("")


class QueryField:
    def __init__(self, type, parent=None):
        self.__type = type
        self.__parent = parent
        self.__cached_id_value = None
        self.__matched_index = False
        self.__fields_expr = True

    @property
    def fields_expression(self):
        return flatten(self.__fields_expr)

    @property
    def __cached_id(self):
        if self.__cached_id_value is None:
            self.__cached_id_value = str(self)
        return self.__cached_id_value

    def _get_parent(self):
        return self.__parent

    def get_type(self):
        return self.__type

    def matched_index(self):
        self.__matched_index = True
        return self

    def __getattr__(self, name):
        if not self.__type.no_real_attributes and hasattr(self.__type, name):
            return getattr(self.__type, name)

        if not self.__type.has_subfields:
            raise AttributeError(name)

        fields = self.__type.subfields()
        if name not in fields:
            raise BadQueryException(
                "{} is not a field in {}".format(name, self.__type.sub_type())
            )
        return QueryField(fields[name], parent=self)

    def get_absolute_name(self):
        res = []
        current = self

        while type(current) != type(None):
            if current.__matched_index:
                res.append("$")
            res.append(current.get_type().db_field)
            current = current._get_parent()
        return ".".join(reversed(res))

    def startswith(self, prefix, ignore_case=False, options=None):
        return self.regex(
            "^" + re.escape(prefix), ignore_case=ignore_case, options=options
        )

    def endswith(self, suffix, ignore_case=False, options=None):
        return self.regex(
            re.escape(suffix) + "$", ignore_case=ignore_case, options=options
        )

    def regex(self, expression, ignore_case=False, options=None):
        regex = {"$regex": expression}
        if options is not None:
            regex["$options"] = options
        if ignore_case:
            regex["$options"] = regex.get("$options", "") + "i"
        expr = {self: regex}
        return QueryExpression(expr)

    def near(self, x, y, max_distance=None):
        expr = {self: {"$near": [x, y]}}
        if max_distance is not None:
            expr[self]["$maxDistance"] = max_distance
        return QueryExpression(expr)

    def near_sphere(self, x, y, max_distance=None):
        expr = {self: {"$nearSphere": [x, y]}}
        if max_distance is not None:
            expr[self]["$maxDistance"] = max_distance
        return QueryExpression(expr)

    def within_box(self, corner1, corner2):
        return QueryExpression(
            {
                self: {
                    "$within": {
                        "$box": [corner1, corner2],
                    }
                }
            }
        )

    def within_radius(self, x, y, radius):
        return QueryExpression(
            {
                self: {
                    "$within": {
                        "$center": [[x, y], radius],
                    }
                }
            }
        )

    def within_radius_sphere(self, x, y, radius):
        return QueryExpression(
            {
                self: {
                    "$within": {
                        "$centerSphere": [[x, y], radius],
                    }
                }
            }
        )

    def within_polygon(self, polygon):
        return QueryExpression(
            {
                self: {
                    "$within": {
                        "$polygon": polygon,
                    }
                }
            }
        )

    def in_(self, *values):
        return QueryExpression(
            {self: {"$in": [self.get_type().wrap_value(value) for value in values]}}
        )

    def nin(self, *values):
        return QueryExpression(
            {self: {"$nin": [self.get_type().wrap_value(value) for value in values]}}
        )

    def exists(self, exists=True):
        return QueryExpression({self: {"$exists": exists}})

    def __str__(self):
        return self.get_absolute_name()

    def __repr__(self):
        return "QueryField({})".format(str(self))

    def __hash__(self):
        return hash(self.__cached_id)

    def __eq__(self, value):
        return self.eq_(value)

    def eq_(self, value):
        if isinstance(value, QueryField):
            return self.__cached_id == value.__cached_id
        return QueryExpression({self: self.get_type().wrap_value(value)})

    def __lt__(self, value):
        return self.lt_(value)

    def lt_(self, value):
        return self.__comparator("$lt", value)

    def __le__(self, value):
        return self.le_(value)

    def le_(self, value):
        return self.__comparator("$lte", value)

    def __ne__(self, value):
        return self.ne_(value)

    def ne_(self, value):
        if isinstance(value, QueryField):
            return self.__cached_id != value.__cached_id
        return self.__comparator("$ne", value)

    def __gt__(self, value):
        return self.gt_(value)

    def gt_(self, value):
        return self.__comparator("$gt", value)

    def __ge__(self, value):
        return self.ge_(value)

    def ge_(self, value):
        return self.__comparator("$gte", value)

    def elem_match(self, value):
        self.__is_elem_match = True
        if not self.__type.is_sequence_field:
            raise BadQueryException(
                "elem_match called on a non-sequence field: " + str(self)
            )
        if isinstance(value, dict):
            self.__fields_expr = {"$elemMatch": value}
            return ElemMatchQueryExpression(self, {self: self.__fields_expr})
        elif isinstance(value, QueryExpression):
            self.__fields_expr = {"$elemMatch": value.obj}
            e = ElemMatchQueryExpression(self, {self: self.__fields_expr})
            return e
        raise BadQueryException(
            "elem_match requires a QueryExpression (to be type-safe) or a dict (which is not type-safe)"
        )

    def exclude(self):
        self.__fields_expr = False
        return self

    def __comparator(self, op, value):
        return QueryExpression({self: {op: self.get_type().wrap(value)}})


class QueryExpression:
    def __init__(self, obj):
        self.obj = obj

    def not_(self):
        ret_obj = {}
        for k, v in self.obj.items():
            if not isinstance(v, dict):
                ret_obj[k] = {"$ne": v}
                continue
            num_ops = len([x for x in v if x[0] == "$"])
            if num_ops != len(v) and num_ops != 0:
                raise BadQueryException("$ operator used in field name")

            if num_ops == 0:
                ret_obj[k] = {"$ne": v}
                continue

            for op, value in v.items():
                k_dict = ret_obj.setdefault(k, {})
                not_dict = k_dict.setdefault("$not", {})
                not_dict[op] = value

        return QueryExpression(ret_obj)

    def __invert__(self):
        return self.not_()

    def __or__(self, expression):
        return self.or_(expression)

    def or_(self, expression):
        if "$or" in self.obj:
            self.obj["$or"].append(expression.obj)
            return self
        self.obj = {"$or": [self.obj, expression.obj]}
        return self


class ElemMatchQueryExpression(QueryExpression):
    def __init__(self, field, obj):
        QueryExpression.__init__(self, obj)
        self._field = field

    def __str__(self):
        return str(self._field)

    def get_absolute_name(self):
        return self._field.get_absolute_name()

    @property
    def fields_expression(self):
        return self._field.fields_expression


def flatten(obj):
    if not isinstance(obj, dict):
        return obj
    ret = {}
    for k, v in obj.items():
        if not isinstance(k, str):
            k = str(k)
        if isinstance(v, dict):
            v = flatten(v)
        if isinstance(v, list):
            v = [flatten(x) for x in v]
        ret[k] = v
    return ret
