from typing import Union

from bson import ObjectId

from .. import Integer, Key, String

types = Union[Integer, String]


def declarative_base(noalchemy):
    base_class = Base
    base_class.noalchemy = noalchemy
    base_class.noalchemy.base = Base
    return base_class


class Base:
    def __init_subclass__(cls):
        cls.query = cls._create_query_instance(cls)
        for attr_name, attr_type in cls.__annotations__.items():
            if isinstance(attr_type, Key):
                attr_type.__collection_name__ = cls.__collection_name__
                attr_type.__key__ = attr_name
                attr_type.__object__ = cls
                setattr(cls, attr_name, attr_type)

        cls._create_collection(cls)

    def __init__(self, *args, **kwargs):
        self.noalchemy = None
        self.query = self._create_query_instance()
        self._validate_required_keys(kwargs)
        self._initialize_keys(kwargs)
        self._post_init()

    def _create_collection(self):
        if (
            self.__collection_name__
            not in self.noalchemy.database.list_collection_names()
        ):
            self.noalchemy.database.create_collection(self.__collection_name__)

    def _validate_required_keys(self, kwargs):
        for key, type_instance in self.__class__.__annotations__.items():
            if isinstance(type_instance, Key) and key not in kwargs:
                if type_instance.required:
                    raise Exception(f"{key} is required.")

    def _initialize_keys(self, kwargs):
        processed_dict = self._process_keys(kwargs)
        self.__dict__.update(processed_dict)

    def _process_keys(self, kwargs):
        processed_dict = {}
        for key, type_instance in self.__class__.__annotations__.items():
            if isinstance(type_instance, Key):
                if kwargs.get(key, None):
                    type_instance.key = key
                    type_instance.class_object = self.__class__
                    type_instance.type.content = kwargs.get(key, None)

                    processed_dict[key] = type_instance.type
            else:
                processed_dict[key] = kwargs.get(key, None)
        return processed_dict

    def _post_init(self):
        if not hasattr(self, "_id") or not ObjectId.is_valid(self._id):
            self._id = ObjectId()

    def __setattr__(self, key, value):
        class_annotations = self.__class__.__annotations__
        if annotation := class_annotations.get(key, False):
            if isinstance(annotation, Key):
                annotation.type.content = value
                self.__dict__[key] = annotation.type
            else:
                self.__dict__[key] = value

    def _create_query_instance(self):
        return self.noalchemy.query(collection=self.__collection_name__, object=self)

    def to_dict(self):
        readable_dict = self.__dict__

        for _dict_key, _dict_class in readable_dict.items():
            if isinstance(_dict_class, types):
                readable_dict[_dict_key] = _dict_class.value

        return readable_dict

    def __call__(self, *args, **kwargs):
        self.__init__(self, *args, **kwargs)
        return self
