class String:
    MAX_LENGTH = 255

    def __init__(
        self,
        max_length: int = MAX_LENGTH,
        uppercase: bool = False,
        strip: bool = False,
    ):
        self.__noalchemy_type__ = True

        self._validate_parameters(max_length, uppercase, strip)

        self.max_length = max_length
        self.uppercase = uppercase
        self.strip = strip
        self.content = ""

    def _validate_parameters(self, max_length, uppercase, strip):
        if not isinstance(max_length, int) or max_length <= 0:
            raise ValueError("Max length must be a positive integer.")
        if not isinstance(uppercase, bool):
            raise ValueError("Uppercase must be a boolean.")
        if not isinstance(strip, bool):
            raise ValueError("Strip must be a boolean.")

    @property
    def has_content(self):
        return bool(self.content)

    @property
    def length(self):
        return len(self.content)

    @property
    def value(self):
        return self.process_content()

    def process_content(self):
        content = self.content

        if self.uppercase:
            content = content.upper()

        if self.strip:
            content = content.strip()

        return content

    def check_content(self):
        if not self.has_content:
            raise ValueError("No data available.")

        if self.length > self.MAX_LENGTH:
            raise ValueError("Number of characters exceeds the maximum limit.")

        if self.length > self.max_length:
            raise ValueError("Number of characters exceeds the defined limit.")

        return True

    def __str__(self):
        if self.check_content():
            return self.process_content()

    def __add__(self, other):
        if not isinstance(other, str):
            raise TypeError(
                "You can only concatenate a String object with another string."
            )

        return self.content + other

    def __radd__(self, other):
        if not isinstance(other, str):
            raise TypeError(
                "You can only concatenate a String object with another string."
            )

        return other + self.content
