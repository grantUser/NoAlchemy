class Integer:
    MAX_VALUE = 2147483647

    def __init__(
        self,
        max_value: int = MAX_VALUE,
        min_value: int = None
    ):
        self._validate_parameters(max_value, min_value)

        self.max_value = max_value
        self.min_value = min_value
        self.content = ""

    def _validate_parameters(self, max_value, min_value):
        if not isinstance(max_value, int) or max_value < 0:
            raise ValueError("Max value must be a non-negative integer.")
        if min_value is not None and (not isinstance(min_value, int) or min_value < 0):
            raise ValueError("Min value must be a non-negative integer.")

    @property
    def has_content(self):
        return bool(self.content)
    
    def process_content(self, content):
        return str(content)

    def check_content(self):
        if not self.has_content:
            raise ValueError("No data available.")
        
        if not isinstance(self.content, int):
            raise ValueError("No data available.")

        if self.min_value is not None and self.content < self.min_value:
            raise ValueError(f"Value must be greater than or equal to {self.min_value}.")

        if self.content > self.max_value:
            raise ValueError(f"Value must be less than or equal to {self.max_value}.")

        return True

    def __str__(self):
        if self.check_content():
            return self.process_content(self.content)

    def __add__(self, other):
        if not isinstance(other, int):
            raise TypeError("You can only concatenate a String object with another string.")

        return self.content + other

    def __radd__(self, other):
        if not isinstance(other, int):
            raise TypeError("You can only concatenate a String object with another string.")
        

        return other + self.content
    
    def __iadd__(self, other):
        if not isinstance(other, int):
            raise TypeError("You can only concatenate a String object with another string.")
        
        self.content += other
        
        return self.content

class String:
    MAX_LENGTH = 255

    def __init__(
        self,
        max_length: int = MAX_LENGTH,
        uppercase: bool = False,
        strip: bool = False,
    ):
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

    def process_content(self, content):
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
            return self.process_content(self.content)

    def __add__(self, other):
        if not isinstance(other, str):
            raise TypeError("You can only concatenate a String object with another string.")

        return self.content + other

    def __radd__(self, other):
        if not isinstance(other, str):
            raise TypeError("You can only concatenate a String object with another string.")

        return other + self.content


class Key:
    def __init__(self, type: object, required: bool = False):
        self.type = type
        self.required = required

    def __call__(self, *args, **kwargs):
        return self.type(*args, **kwargs)
