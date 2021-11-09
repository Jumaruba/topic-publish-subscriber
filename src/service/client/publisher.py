from .client import Client


class Publisher(Client):
    def __init__(self):
        Client.__init__()

    def put(self, topic: str) -> None:
        pass
