# Import necessary modules
import pytest

from noalchemy import Integer, Key, String, create_engine
from noalchemy.odm import declarative_base, scoped_session, sessionmaker

# Create a MongoDB engine with a mock connection for testing
engine = create_engine("mongodb://localhost:27017/tests", mock=True)
assert engine.isConnected()

# Create a session using the engine and ensure that the Session is created
Session = scoped_session(sessionmaker(engine))
assert Session
assert callable(Session)
assert hasattr(Session, "__enter__")
assert hasattr(Session, "__exit__")

Base = declarative_base(Session)
assert Base

# Define a class for the User document
class User(Base):
    __collection_name__ = "users"

    name: Key(String(255, uppercase=True, strip=True), required=True)
    age: Key(Integer(100))


# Create a class for testing sessions and User document operations
class TestClassDatabase:
    # Test the opening of a session
    def test_open_session(self):
        # Open a session using a "with" statement
        with Session() as session:
            # Check if the session is created
            assert session

    def test_check_subclass(self):
        # Check if User is a subclass of Base
        assert issubclass(User, Base)

    def test_create_user(self):
        # Create a User document and perform various checks
        user = User(name="Bot", age=5)
        assert user

        assert isinstance(user.name, String)
        assert isinstance(user.age, Integer)

        assert user.name != "Bot"
        assert user.name == "BOT"
        assert len(user.name) == 3

        assert user.age == 5
        assert user.age <= 5
        assert user.age >= 5

        assert user.age < 6
        assert user.age > 4

    def test_create_user_but_missing_required(self):
        with pytest.raises(Exception) as excinfo:
            User(age=5)
        assert str(excinfo.value) == "name is required."

    def test_update_user(self):
        # Create a User document and update its attributes
        user = User(name="Bot", age=5)

        user.name = "grantUser"
        assert user.name == "GRANTUSER"

        user.age = 99
        assert user.age == 99

    def test_add_user_to_session(self):
        # Create a User document and add it to the session
        with Session() as session:
            user = User(name="grantUser", age=99)
            session.add(user)

    def test_add_users_to_session(self):
        # Create multiple User documents and add them to the session
        with Session() as session:
            user1 = User(name="Bot", age=5)
            user2 = User(name="grantUser", age=99)
            session.add_all([user1, user2])

    def test_commit_user_to_database(self):
        # Create a User document, add it to the session, and commit it to the database
        with Session() as session:
            user = User(name="grantUser", age=99)
            session.add(user)
            session.commit()

    def test_commit_users_to_database(self):
        # Create multiple User documents, add them to the session, and commit them to the database
        with Session() as session:
            user1 = User(name="Bot", age=5)
            user2 = User(name="grantUser", age=45)
            session.add_all([user1, user2])
            session.commit()

    def test_count_users_with_string_class(self):
        # Test counting users with a string class name
        with Session() as session:
            count = session.query("User").count()
            assert count == 3

    def test_query_users_with_string_class(self):
        # Test querying users with a string class name
        with Session() as session:
            users = session.query("User").all()
            assert len(users) == 3

            for user in users:
                assert isinstance(user, User)

    def test_query_users_with_class(self):
        # Test querying users with the User class
        with Session() as session:
            users = session.query(User).all()
            assert len(users) == 3

            for user in users:
                assert isinstance(user, User)

    def test_query_users_with_class_and_projection(self):
        # Test querying users with the User class and projection
        with Session() as session:
            users = session.query(User.name).all()
            assert len(users) == 3

            for user in users:
                assert isinstance(user, User)
                assert "age" not in user.to_dict()

    def test_query_one_user_with_class(self):
        # Test querying a single user with the User class
        with Session() as session:
            user = session.query(User).one()
            assert isinstance(user, User)

    def test_query_one_user_with_class_and_projection(self):
        # Test querying a single user with the User class and projection
        with Session() as session:
            user = session.query(User.name).one()
            assert isinstance(user, User)

    def test_query_one_user_with_class_projection_and_filter(self):
        # Test querying a single user with the User class, projection, and filter
        with Session() as session:
            user = session.query(User).filter_by(age=45).one()
            assert isinstance(user, User)

    def test_query_one_user_with_class_projection_and_multiple_filter(self):
    # Test querying a single user with the User class, projection, and filter
        with Session() as session:
            user = session.query(User).filter_by(name="grantUser", age=45).one()
            assert isinstance(user, User)

    def test_query_one_or_none_user_with_class(self):
        # Test querying a single user or None with the User class
        with Session() as session:
            user = session.query(User).filter_by(age=45).one_or_none()
            assert isinstance(user, User)

            user = session.query(User).filter_by(age=999).one_or_none()
            assert user is None

    def test_query_first_user_with_class(self):
        # Test querying the first user with the User class
        with Session() as session:
            user = session.query(User).filter_by(name="grantUser").first()
            assert isinstance(user, User)

    def test_query_last_user_with_class(self):
        # Test querying the last user with the User class
        with Session() as session:
            user = session.query(User).filter_by(name="grantUser").last()
            assert isinstance(user, User)

    def test_update_user_with_class(self):
        # Test querying a single user or None with the User class
        with Session() as session:
            user = session.query(User).filter_by(name="Bot").one()
            user.age = 70
            session.commit()

            refresh_user = session.query(User).filter_by(name="Bot").one()
            assert user.age == refresh_user.age

    def test_delete_user_with_class(self):
        # Test querying a single user or None with the User class
        with Session() as session:
            user = session.query(User).filter_by(name="Bot").one()
            session.delete(user)
            session.commit()

            refresh_user = session.query(User).filter_by(name="Bot").one_or_none()
            assert refresh_user is None
