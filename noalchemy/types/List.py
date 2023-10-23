class List:
    def __init__(self, native_type=None) -> None:
        self.__noalchemy_type__ = True
        self.content = []
    
        if not isinstance(native_type, (list, tuple)):
            native_type = [native_type]

        self.native_types = native_type

    def _check_native_type(self, data):
        if self.native_types is not None and not any(isinstance(data, t) for t in self.native_types):
            raise TypeError(f"Element must be one of the types {self.native_types}")
        
    @property
    def value(self):
        return self.content

    def append(self, data):
        self._check_native_type(data)
        self.content.append(data)

    def extend(self, data):
        if self.native_types is not None:
            if not all(any(isinstance(item, t) for t in self.native_types) for item in data):
                raise TypeError(f"All elements must be one of the types {self.native_types}")
        self.content.extend(data)

    def __len__(self):
        return len(self.content)

    def get(self, index):
        if 0 <= index < len(self.content):
            return self.content[index]
        else:
            raise IndexError("Index out of range")

    def __getitem__(self, index):
        return self.get(index)

    def __setitem__(self, index, value):
        if 0 <= index < len(self.content):
            self._check_native_type(value)
            self.content[index] = value
        else:
            raise IndexError("Index out of range")

    def __delitem__(self, index):
        if 0 <= index < len(self.content):
            del self.content[index]
        else:
            raise IndexError("Index out of range")

    def __iter__(self):
        return iter(self.content)

    def __contains__(self, item):
        return item in self.content

    def __str__(self):
        return str(self.content)

    def __repr__(self):
        return repr(self.content)
