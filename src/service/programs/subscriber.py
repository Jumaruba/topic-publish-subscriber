from .client import Client


class Subscriber(Client):
    def __init__(self):
        super().__init__()

    def get(self, topic: str) -> None:
        pass

    def subscribe(self, topic: str) -> None:
        pass

    def unsubscribe(self, topic: str) -> None:
        pass

    def run(self):
        pass
