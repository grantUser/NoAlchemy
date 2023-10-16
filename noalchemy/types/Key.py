from typing import Union


class Key:
    def __init__(
        self,
        type: object = None,
        required: bool = False,
        index: Union[bool, object] = False,
    ) -> None:
        self.__collection_name__ = None
        self.__key__ = None
        self.__object__ = None

        self.type = type
        self.required = required
        self.index = index
