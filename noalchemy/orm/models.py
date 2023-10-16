class Models:
    def __init__(self) -> None:
        self._instances = {}

    def add(self, instance):
        if isinstance(instance, dict):
            for key, value in instance.items():
                self._instances[key] = value

    @property
    def instances(self):
        return self._instances
    

models = Models()
