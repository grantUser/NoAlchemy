from .. import DeleteOne, InsertOne, UpdateOne


class Transaction:
    def __init__(self, bind=None) -> None:
        self.__bind = bind
        self.bulk_operations = {}

    def _get_bulk_operations(self, collection_name):
        if collection_name not in self.bulk_operations:
            self.bulk_operations[collection_name] = []

        return self.bulk_operations[collection_name]

    def delete_one(self, object):
        if not hasattr(object, "__from__"):
            raise Exception()

        if object.__from__ != 1:
            raise Exception()

        collection = object.__collection_name__
        bulk_ops = self._get_bulk_operations(collection)
        bulk_ops.append(DeleteOne({"_id": object._id}))

    def delete_many(self, objects):
        for object in objects:
            self.delete_one(object)

    def insert_one(self, object):
        if hasattr(object, "__from__") and object.__from__ == 1:
            raise Exception()

        collection = object.__collection_name__
        bulk_ops = self._get_bulk_operations(collection)
        document = object.to_dict()

        bulk_ops.append(InsertOne(document))

    def insert_many(self, objects):
        for object in objects:
            self.insert_one(object)

    def update_one(self, object):
        if not hasattr(object, "__from__") or object.__from__ != 1:
            raise Exception()

        if object.__eta__ != 2:
            return

        collection = object.__collection_name__
        bulk_ops = self._get_bulk_operations(collection)
        document = object.to_dict()

        update_existing = None
        for op in bulk_ops:
            if op.__class__ == UpdateOne and op._filter.get("_id") == document.get(
                "_id"
            ):
                update_existing = op
                break

        if update_existing:
            update = update_existing._doc
            update["$set"].update(
                {
                    key: value
                    for key, value in object.items()
                    if key in object.__originals__.keys()
                }
            )
        else:
            update = {
                "$set": {
                    key: value
                    for key, value in object.items()
                    if key in object.__originals__.keys()
                }
            }
            bulk_ops.append(UpdateOne({"_id": document["_id"]}, update))

    def commit(self):
        for collection_name, bulk_ops in self.bulk_operations.items():
            if bulk_ops:
                try:
                    collection = self.__bind.database[collection_name]
                    collection.bulk_write(bulk_ops, ordered=True)
                except Exception as e:
                    self.rollback()
                    raise e

    def rollback(self):
        for collection_name, bulk_ops in self.bulk_operations.items():
            for operation in reversed(bulk_ops):
                if operation.__class__ == DeleteOne:
                    collection = self.__bind.database[collection_name]
                    collection.insert_one(operation._filter)
                elif operation.__class__ == InsertOne:
                    collection = self.__bind.database[collection_name]
                    collection.delete_one(operation._doc)
                elif operation.__class__ == UpdateOne:
                    collection = self.__bind.database[collection_name]
                    collection.update_one(
                        operation.filter,
                        {"$set": {key: None for key in operation.update["$set"]}},
                    )
        self.bulk_operations = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
