
# ranger user defination
class User:
    def __init__(self, name = "", password = "", first_name = "", last_name = ""):
        self.name = name
        self.password = password
        self.first_name = first_name
        self.last_name = last_name

# ranger service
class Service:
    def __init__(self, id, name):
        self.id = id
        self.name = name
