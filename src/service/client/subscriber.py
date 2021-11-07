from .client import Client

class Subscriber(Client):
    def __init__(self):
        Client.__init__(self)

    def get(self, topic: str) -> None:
        pass

    def subscribe(self, topic: str) -> None:
        pass

    def unsubscribe(self, topic: str) -> None:
        pass