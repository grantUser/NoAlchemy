class Dict:
    def __init__(self) -> None:
        self.__noalchemy_type__ = True
        self.content = {}

    def __len__(self):
        return len(self.content)

    def __getitem__(self, key):
        return self.content[key]

    def __setitem__(self, key, value):
        self.content[key] = value

    def __delitem__(self, key):
        del self.content[key]

    def get(self, key, default=None):
        return self.content.get(key, default)

    def __iter__(self):
        return iter(self.content)

    def __contains__(self, key):
        return key in self.content

    def __str__(self):
        return str(self.content)

    def __repr__(self):
        return repr(self.content)
