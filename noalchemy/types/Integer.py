class Integer:
    MAX_VALUE = 2147483647

    def __init__(self, max_value: int = MAX_VALUE, min_value: int = None):
        self.__noalchemy_type__ = True

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

    @property
    def value(self):
        return self.content

    def process_content(self):
        return str(self.content)

    def check_content(self):
        if not self.has_content:
            raise ValueError("No data available.")

        if not isinstance(self.content, int):
            raise ValueError("No data available.")

        if self.min_value is not None and self.content < self.min_value:
            raise ValueError(
                f"Value must be greater than or equal to {self.min_value}."
            )

        if self.content > self.max_value:
            raise ValueError(f"Value must be less than or equal to {self.max_value}.")

        return True

    def __str__(self):
        if self.check_content():
            return self.process_content()

    def __add__(self, other):
        if not isinstance(other, int):
            raise TypeError(
                "You can only concatenate a Integer object with another integer."
            )

        return self.content + other

    def __radd__(self, other):
        if not isinstance(other, int):
            raise TypeError(
                "You can only concatenate a Integer object with another integer."
            )

        return other + self.content

    def __iadd__(self, other):
        if not isinstance(other, int):
            raise TypeError(
                "You can only concatenate a Integer object with another integer."
            )

        self.content += other

        return self.content
