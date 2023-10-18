from typing import Any, Union

from bson import ObjectId

from ..types import Integer, Key, String
from .models import models
import inspect

types = Union[Integer, String]

class declarative_base:
    def __new__(cls, Session=None) -> None:
        document = Document
        document.session = Session()
        return document

    def __call__(cls, Session=None) -> None:
        document = Document
        document.session = Session()
        return document


class Document:
    def __init_subclass__(cls) -> None:

        if not cls.__name__ in models.instances:
            models.add({cls.__name__: cls})

        for key, type in cls.__annotations__.items():
            if isinstance(type, Key):
                type.__collection_name__ = cls.__collection_name__
                type.__key__ = key
                type.__object__ = cls
                setattr(cls, key, type)

        if (
            cls.__collection_name__
            not in cls.session.bind.database.list_collection_names()
        ):
            cls.session.bind.database.create_collection(cls.__collection_name__)

    def __init__(self, *args, **kwds) -> None:
        self._id = None
        self.session = None

        self.__post_init__(*args, **kwds)

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.__init__(*args, **kwds)

    def __post_init__(self, *args, **kwds):
        if (
            self.__collection_name__
            not in self.session.bind.database.list_collection_names()
        ):
            self.session.bind.database.create_collection(self.__collection_name__)

        if not hasattr(self, "_id") or not ObjectId.is_valid(self._id):
            self.__dict__["_id"] = ObjectId()

        for key, instance in self.__class__.__annotations__.items():
            if isinstance(instance, Key) and key not in kwds:
                if instance.required:
                    raise Exception(f"{key} is required.")

            if isinstance(instance, Key) and key in kwds:
                constructor = instance.type.__init__
                parameters = list(inspect.signature(constructor).parameters.keys())
                current_parameter_values = {param: getattr(instance.type, param) for param in parameters}
                new_instance = type(instance.type)(**current_parameter_values)
                
                new_instance.content = kwds.get(key, None)
                self.__dict__[key] = new_instance

        if _id := kwds.get("_id", False):
            if ObjectId.is_valid(_id):
                self.__dict__["_id"] = _id

    def __setattr__(self, key, value):
        instances = self.__class__.__annotations__
        if instance := instances.get(key, False):
            if isinstance(instance, Key):
                instance.type.content = value
                self.__dict__[key] = instance.type
            else:
                self.__dict__[key] = value

    def to_dict(self):
        readable_dict = self.__dict__

        for _dict_key, _dict_class in readable_dict.items():
            if isinstance(_dict_class, types):
                readable_dict[_dict_key] = _dict_class.value

        return readable_dict
