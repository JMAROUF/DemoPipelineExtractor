class ClassTest:
    attr: str
    def __init__(self):
        self.attr = "instance attribute"

    def show(self):
        return self.attr