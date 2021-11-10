from random import randrange

import zmq

from .client import Client
from .program import SocketCreationFunction


class Publisher(Client):
    def __init__(self):
        super().__init__()

    def put(self, topic: str) -> None:
        pass

    def run(self):
        socket = self.create_socket(zmq.PUB, SocketCreationFunction.CONNECT, 'localhost:5556')

        while True:
            zipcode = randrange(0, 100000)
            temperature = randrange(-80, 135)
            relative_humidity = randrange(10, 60)

            socket.send_string(f"{zipcode} {temperature} {relative_humidity}")