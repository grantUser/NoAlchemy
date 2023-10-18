from noalchemy.odm import declarative_base

from utils.engine import Session

Base = declarative_base(Session)
