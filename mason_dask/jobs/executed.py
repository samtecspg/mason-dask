
class ExecutedJob():
    def __init__(self, message: str):
        self.message = message


class InvalidJob():
    def __init__(self, reason: str):
        self.message = reason

