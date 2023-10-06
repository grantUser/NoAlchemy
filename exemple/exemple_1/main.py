from models.User import User

user = User(name="aa    a    ", othername="test")
print(user.name)

user.name = "wow super"
print(user.name)