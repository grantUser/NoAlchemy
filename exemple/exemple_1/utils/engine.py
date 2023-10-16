from noalchemy import create_engine
from noalchemy.orm import scoped_session, sessionmaker

engine = create_engine("mongodb://localhost:27017/exemple1")
Session = scoped_session(sessionmaker(engine))
