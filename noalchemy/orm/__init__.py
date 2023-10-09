from bson import ObjectId
from .. import Key

class Base:
    def __init__(self, *args, **kwds):
        self._validate_required_keys(kwds)
        self._initialize_keys(kwds)
        self._post_init()

    def _validate_required_keys(self, kwds):
        for key, type_instance in self.__class__.__annotations__.items():
            if isinstance(type_instance, Key) and key not in kwds:
                if type_instance.required:
                    raise Exception(f"{key} is required.")

    def _initialize_keys(self, kwds):
        self.__dict__.update(self._process_keys(kwds))

    def _process_keys(self, kwds):
        processed_dict = {}
        for key, type_instance in self.__class__.__annotations__.items():
            value = kwds.get(key, None)
            if isinstance(type_instance, Key):
                type_instance.type.content = value
                processed_dict[key] = type_instance.type
            else:
                processed_dict[key] = value
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

    def __call__(self, *args, **kwds):
        self.__init__(self, *args, **kwds)
        return self
