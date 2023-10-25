from ..exc import *


class Aggregation:
    def __init__(self, Query=None) -> None:
        self.query = Query
        self.pipeline = None

    def add(self, data):
        if not self.pipeline:
            self.pipeline = []

        if isinstance(data, list):
            self.pipeline.extend(data)

    def build_pipeline(self, pipes: list):
        for pipe in pipes:
            match pipe:
                case "$match":
                    if self.query.filter:
                        self.pipeline.extend([{"$match": self.query.filter}])

                case "$project":
                    if self.query.projection:
                        self.pipeline.extend([{"$project": self.query.projection}])

                case "$skip":
                    if self.query.offset_value:
                        self.pipeline.extend([{"$skip": self.query.offset_value}])

                case "$limit":
                    if self.query.limit_value:
                        self.pipeline.extend([{"$limit": self.query.limit_value}])

                case _:
                    continue

    def all(self, collection):
        self.build_pipeline(["$match", "$project", "$skip", "$limit"])
        documents = list(collection.aggregate(self.pipeline))

        document_list = []
        for document in documents:
            document_list.append(
                self.query.object(**document, __from__=1, Session=self.query.Session)
            )

        return document_list

    def one_or_none(self, collection):
        self.build_pipeline(["$match", "$project"])
        self.add([{"$limit": 1}])

        documents = list(collection.aggregate(self.pipeline))
        if len(documents) == 1:
            return self.query.object(
                **documents[0], __from__=1, Session=self.query.Session
            )

        return None

    def one(self, collection):
        self.build_pipeline(["$match", "$project"])
        self.add([{"$limit": 1}])

        documents = list(collection.aggregate(self.pipeline))
        if len(documents) == 1:
            return self.query.object(
                **documents[0], __from__=1, Session=self.query.Session
            )

        raise NoResultFoundException()

    def first(self, collection):
        self.build_pipeline(["$match", "$project"])
        self.add([{"$sort": {"_id": 1}}])
        self.add([{"$limit": 1}])

        documents = list(collection.aggregate(self.pipeline))
        if len(documents) == 1:
            return self.query.object(
                **documents[0], __from__=1, Session=self.query.Session
            )

        raise NoResultFoundException()

    def last(self, collection):
        self.build_pipeline(["$match", "$project"])
        self.add([{"$sort": {"_id": -1}}])
        self.add([{"$limit": 1}])

        documents = list(collection.aggregate(self.pipeline))
        if len(documents) == 1:
            return self.query.object(
                **documents[0], __from__=1, Session=self.query.Session
            )

        raise NoResultFoundException()

    def count(self, collection):
        self.build_pipeline(["$match", "$project", "$skip", "$limit"])

        documents = list(collection.aggregate(self.pipeline))
        return len(documents)
