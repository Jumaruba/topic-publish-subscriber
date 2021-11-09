from .client import Client


class Publisher(Client):
    def __init__(self):
        super().__init__()

    def put(self, topic: str) -> None:
        pass
