class NoResultFoundException(Exception):
    def __init__(self, message="No result found"):
        self.message = message
        super().__init__(self.message)


class MultipleCollectionsFound(Exception):
    def __init__(self, message="Multiple collections were found during the search"):
        self.message = message
        super().__init__(self.message)


class MultipleObjectsDetected(Exception):
    def __init__(self, message="Detection of several objects searched simultaneously"):
        self.message = message
        super().__init__(self.message)
