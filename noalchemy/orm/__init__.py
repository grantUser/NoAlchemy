from bson import ObjectId

from .. import Key


class Base:
    def __init__(self, *args, **kwds):
        class_annotations = self.__class__.__annotations__

        valid_kwargs = {}
        for key, value in kwds.items():
            if annotation := class_annotations.get(key, False):
                if isinstance(annotation, Key):
                    annotation.type.content = value
                    valid_kwargs[key] = annotation.type
                else:
                    valid_kwargs[key] = value

        self.__dict__.update(valid_kwargs)
        self._post_init()

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
