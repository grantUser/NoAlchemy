import inspect
from typing import Any, Union

from bson import ObjectId

from ..types import Key
from .models import models


class declarative_base:
    def __new__(cls, Session=None) -> None:
        document = Document
        document.Session = Session
        return document

    def __call__(cls, Session=None) -> None:
        document = Document
        document.Session = Session
        return document


whitelist_key = ["__eta__", "__from__", "__originals__", "Session"]


class Document:
    def __init_subclass__(cls) -> None:

        if cls.__name__ not in models.instances:
            models.add({cls.__name__: cls})

        for key, type in cls.__annotations__.items():
            if isinstance(type, Key):
                type.__collection_name__ = cls.__collection_name__
                type.__key__ = key
                type.__object__ = cls

            setattr(cls, key, type)

        with cls.Session as session:
            if (
                cls.__collection_name__
                not in session.bind.database.list_collection_names()
            ):
                session.bind.database.create_collection(cls.__collection_name__)

    def __init__(self, *args, **kwds) -> None:
        self._id = None
        self.Session = kwds.get("Session", None)
        self.__eta__ = 0
        self.__from__ = kwds.get("__from__", 0)
        self.__originals__ = {}

        self.__post_init__(*args, **kwds)

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.__init__(*args, **kwds)

    def __post_init__(self, *args, **kwds):
        if not hasattr(self, "_id") or not ObjectId.is_valid(self._id):
            self.__dict__["_id"] = ObjectId()

        for key, instance in self.__class__.__annotations__.items():
            if isinstance(instance, Key) and key not in kwds:
                if instance.required:
                    raise Exception(f"{key} is required.")

            if isinstance(instance, Key) and key in kwds:
                constructor = instance.type.__init__
                parameters = list(inspect.signature(constructor).parameters.keys())
                current_parameter_values = {
                    param: getattr(instance.type, param) for param in parameters
                }
                new_instance = type(instance.type)(**current_parameter_values)

                new_instance.content = kwds.get(key, None)
                self.__dict__[key] = new_instance
            else:
                if value := kwds.get(key, False):
                    if not isinstance(value, instance):
                        raise Exception("Incorrect type, " + type(value))
                    self.__dict__[key] = value

        if _id := kwds.get("_id", False):
            if ObjectId.is_valid(_id):
                self.__dict__["_id"] = _id

        self.__eta__ = 1

    def __setattr__(self, key, value):
        if hasattr(self, "__eta__") and self.__eta__ == 1:
            self.__dict__["__eta__"] = 2

        instances = self.__class__.__annotations__
        if instance := instances.get(key, False):

            if isinstance(instance, Key):
                if self.__eta__ == 2 and (
                    hasattr(self, "__from__") and self.__from__ == 1
                ):
                    if key not in self.__originals__:
                        original = self.__dict__.get(key, None)
                        self.__originals__[key] = original.content
                        self.Session._update(self)

                instance.type.content = value
                self.__dict__[key] = instance.type
            else:
                self.__dict__[key] = value

        elif key in whitelist_key:
            self.__dict__[key] = value

    def to_dict(self, exclude: list = None):
        if not exclude:
            exclude = []

        readable_dict = {
            key: value
            for key, value in self.__dict__.items()
            if key not in whitelist_key and key not in exclude
        }

        for _dict_key, _dict_class in readable_dict.items():
            if (
                hasattr(_dict_class, "__noalchemy_type__")
                and _dict_class.__noalchemy_type__
            ):
                readable_dict[_dict_key] = _dict_class.value

        return readable_dict

    def items(self, exclude: list = None):
        if not exclude:
            exclude = []

        readable_dict = {
            key: value
            for key, value in self.__dict__.items()
            if key not in whitelist_key and key not in exclude
        }

        for _dict_key, _dict_class in readable_dict.items():
            if (
                hasattr(_dict_class, "__noalchemy_type__")
                and _dict_class.__noalchemy_type__
            ):
                readable_dict[_dict_key] = _dict_class.value

        return readable_dict.items()
