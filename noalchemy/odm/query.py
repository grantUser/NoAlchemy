import inspect

from ..exc import *
from ..types import Key
from .aggregation import Aggregation
from .models import models
from .relationship import relationship

class Query:
    def __init__(self, *args, **kwds) -> None:
        self.bind = kwds.get("bind", None)
        self.Session = kwds.get("Session", None)
        self.projection = {}
        self.filter = {}
        self.offset_value = 0
        self.limit_value = 1000
        self.collections = []
        self.collection = kwds.get("collection", None)
        self.objects = []
        self.object = None

        self.aggregation = Aggregation(self)

        self.__post_init__(*args, **kwds)

    def __post_init__(self, *args, **kwds):
        for arg in args:
            if isinstance(arg, Key) or isinstance(arg, relationship):
                if hasattr(arg, "__collection_name__"):
                    self.collections.append(arg.__collection_name__)
                if hasattr(arg, "__key__"):
                    self.projection[arg.__key__] = 1
                if hasattr(arg, "__object__"):
                    self.objects.append(arg.__object__)
            elif isinstance(arg, str):
                if arg in models.instances:
                    model = models.instances.get(arg, None)
                    self.collections.append(model.__collection_name__)
                    self.objects.append(model)
            elif isinstance(arg, object):
                if hasattr(arg, "__collection_name__"):
                    self.collections.append(arg.__collection_name__)
                    self.objects.append(arg)
        if self.collection:
            self.collections.append(self.collection)
        self.collections = list(set(self.collections))
        if len(self.collections) > 1:
            raise MultipleCollectionsFound()
        if self.object:
            self.objects.append(self.object)
        self.objects = list(set(self.objects))
        if len(self.objects) > 1:
            raise MultipleObjectsDetected()
        if len(self.collections) != 1 and len(self.objects) != 1:
            return
        self.collection = self.collections[0]
        self.object = self.objects[0]
        projection_len = len(self.projection)
        for name, instance in self.object.__annotations__.items():
            if hasattr(instance, "required"):
                if instance.required or projection_len < 1:
                    self.projection[name] = 1

    def collection_exists_decorator(function):
        def wrapper(self, *args, **kwds):
            if self.collection in self.Session.list_collection_names:
                collection = self.bind.database[self.collection]

                if not self.aggregation.pipeline:
                    return function(self, collection, *args, **kwds)
                else:
                    return getattr(self.aggregation, function.__name__)(collection)
            else:
                raise Exception("Collection not found in the targeted database")

        return wrapper

    def filter_by(self, **kwargs):
        for key, value in kwargs.items():
            if annotation := self.object.__annotations__.get(key, False):
                if isinstance(annotation, Key):
                    constructor = annotation.type.__init__
                    parameters = list(inspect.signature(constructor).parameters.keys())
                    current_parameter_values = {
                        param: getattr(annotation.type, param) for param in parameters
                    }
                    new_instance = type(annotation.type)(**current_parameter_values)
                    new_instance.content = value
                    self.filter[key] = new_instance.value
                    continue

            self.filter[key] = value

        return self

    def offset(self, skip_value):
        self.offset_value = skip_value
        return self

    def limit(self, limit_value):
        self.limit_value = limit_value
        return self

    def join(self, object):
        target = None
        if hasattr(object, "target"):
            target = object.target

        back_populates = None
        if hasattr(object, "back_populates"):
            back_populates = object.back_populates

        if target and back_populates:
            if target in models.instances:
                target = models.instances.get(target)
                if hasattr(target, back_populates) or back_populates == "_id":
                    _from = target.__collection_name__
                    localField = object.__key__
                    foreignField = back_populates
                    _as = object.__key__

                    self.aggregation.add(
                        [
                            {
                                "$lookup": {
                                    "from": _from,
                                    "localField": localField,
                                    "foreignField": foreignField,
                                    "as": _as,
                                }
                            },
                        ]
                    )

                    self.projection[_from] = 1

        return self

    @collection_exists_decorator
    def all(self, collection):
        document_list = []
        documents = (
            collection.find(self.filter, self.projection)
            .skip(self.offset_value)
            .limit(self.limit_value)
        )
        for document in documents:
            document_list.append(
                self.object(**document, __from__=1, Session=self.Session)
            )

        return document_list

    @collection_exists_decorator
    def one_or_none(self, collection):
        document = collection.find_one(self.filter, self.projection)
        if document:
            return self.object(**document, __from__=1, Session=self.Session)
        return None

    @collection_exists_decorator
    def one(self, collection):
        document = collection.find_one(self.filter, self.projection)
        if document:
            return self.object(**document, __from__=1, Session=self.Session)
        raise NoResultFoundException()

    @collection_exists_decorator
    def first(self, collection):
        document = collection.find_one(self.filter, self.projection, sort=[("_id", 1)])
        if document:
            return self.object(**document, __from__=1, Session=self.Session)
        raise NoResultFoundException()

    @collection_exists_decorator
    def last(self, collection):
        document = collection.find_one(self.filter, self.projection, sort=[("_id", -1)])
        if document:
            return self.object(**document, __from__=1, Session=self.Session)
        raise NoResultFoundException()

    @collection_exists_decorator
    def count(self, collection, **kwds):
        return collection.count_documents(self.filter, **kwds)
