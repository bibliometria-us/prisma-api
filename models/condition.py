class Condition:
    def __init__(self, query=None, attribute=None, type=None, value=None) -> None:
        self.query = query
        self.attribute = attribute
        self.type = type
        self.value = value

    def generate_query(self) -> str:
        return self.query
