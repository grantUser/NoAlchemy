# Import necessary modules
from noalchemy import create_engine


def test_create_engine_with_url():
    # Create the engine with a MongoDB URL
    engine = create_engine("mongodb://localhost:27017/tests", mock=True)

    # Check that the database is accessible
    assert engine.isConnected()

    # Check that the private attributes are not directly accessible
    assert not hasattr(engine, "__username")
    assert not hasattr(engine, "__password")
    assert not hasattr(engine, "__host")
    assert not hasattr(engine, "__port")


def test_create_engine_with_arguments():
    # Engine settings provided as a dictionary
    engine_settings = {"host": "localhost", "port": 27017, "database": "tests"}

    # Create the engine using arguments
    engine = create_engine(**engine_settings, mock=True)

    # Check that the database is accessible
    assert engine.isConnected()

    # Check that the private attributes are not directly accessible
    assert not hasattr(engine, "__username")
    assert not hasattr(engine, "__password")
    assert not hasattr(engine, "__host")
    assert not hasattr(engine, "__port")


def test_create_engine_with_arguments_and_credentials():
    # Engine settings with credentials
    engine_settings = {
        "username": "admin",
        "password": "test",
        "host": "localhost",
        "port": 27017,
        "database": "tests",
    }

    # Create the engine using arguments and credentials
    engine = create_engine(**engine_settings, mock=True)

    # Check that the database is accessible
    assert engine.isConnected()

    # Check that the private attributes are not directly accessible
    assert not hasattr(engine, "__username")
    assert not hasattr(engine, "__password")
    assert not hasattr(engine, "__host")
    assert not hasattr(engine, "__port")
