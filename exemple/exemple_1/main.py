from models.User import User
from utils.engine import Session as noalchemy

users = noalchemy.query(User.name, User.age).filter_by(age=44).first()
print(users.to_dict())