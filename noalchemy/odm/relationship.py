class relationship:
    def __init__(self, target: str, back_populates: str) -> None:
        self.__collection_name__ = None
        self.__key__ = None
        self.__object__ = None

        self.target = target
        self.back_populates = back_populates
