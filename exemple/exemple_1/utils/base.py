from noalchemy.orm import declarative_base

from utils.engine import Session

Base = declarative_base(Session)
