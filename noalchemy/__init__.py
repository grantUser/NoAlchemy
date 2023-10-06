from typing import Any


class String:
    MAX_LENGTH = 255

    def __init__(
        self,
        max_length: int = MAX_LENGTH,
        uppercase: bool = False,
        strip: bool = False,
    ):
        if not isinstance(max_length, int) or max_length <= 0:
            raise ValueError("Max length must be a positive integer.")
        if not isinstance(uppercase, bool):
            raise ValueError("Uppercase must be a boolean.")
        if not isinstance(strip, bool):
            raise ValueError("Strip must be a boolean.")

        self.max_length = max_length
        self.uppercase = uppercase
        self.strip = strip
        self.content = ""

    @property
    def has_content(self):
        return bool(self.content)

    @property
    def length(self):
        return len(self.content)

    def process_content(self, content):
        if self.uppercase:
            content = content.upper()

        if self.strip:
            content = content.strip()
        
        return content

    def check_content(self):
        if not self.has_content:
            raise Exception("No data available.")

        if self.length > self.MAX_LENGTH:
            raise Exception("Number of characters exceeds the maximum limit.")

        if self.length > self.max_length:
            raise Exception("Number of characters exceeds the defined limit.")

        return True

    def new_instance(self, content=None, max_length=None, uppercase=None, strip=None):
        content = content if content is not None else self.content
        max_length = max_length if max_length is not None else self.max_length
        uppercase = uppercase if uppercase is not None else self.uppercase
        strip = strip if strip is not None else self.strip
        new_instance = String(max_length, uppercase, strip)
        new_instance.content = content
        return new_instance

    def __str__(self):
        if self.check_content():
            return self.process_content(self.content)

    def __add__(self, other):
        if not isinstance(other, str):
            raise ValueError(
                "You can only concatenate a String object with another string."
            )

        new_instance = self.new_instance(self.content + other)
        return new_instance

class Key:
    def __init__(self, type: object) -> None:
        self.type = type

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        self.__init__(self, *args, **kwds)
        return self.type
