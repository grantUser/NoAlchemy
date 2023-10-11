from models.User import User
from utils.noalchemy import noalchemy

user = User(name="truc", age=12, othername="test")
print(user.age)

test1 = User.query.filter_by(name="Mickael").all()
test1 = test1[0]
print(test1.to_dict())

test2 = noalchemy.query(User).filter_by(name="Mickael").all()
test2 = test2[0]
print(test2.to_dict())

test3 = noalchemy.query(User.name, User.age).filter_by(name="Mickael").all()
test3 = test3[0]
print(test3.to_dict())

test4 = noalchemy.query(User.name).filter_by(age=44).all()
test4 = test4[0]
print(test4.to_dict())

test5 = noalchemy.query(User.age).filter_by(name="Mickael").filter_by(age=44).all()
test5 = test5[0]
print(test5.to_dict())
