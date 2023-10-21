class List:
    def __init__(self) -> None:
        self.__noalchemy_type__ = True
        self.content = []

    def append(self, data):
        self.content.append(data)

    def extend(self, data):
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
