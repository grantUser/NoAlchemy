from pymongo import MongoClient
from mongomock import MongoClient as MockMongoClient

def create_engine(url, **kwargs):
    strategy = kwargs.get("strategy", "default")

    if strategy == "mock":
        return create_mock_engine(url)
    elif strategy == "default":
        return MongoClient(url)
    else:
        raise Exception("Unknown strategy: %r" % strategy)

def create_mock_engine(url):
    return MockMongoClient(url)
