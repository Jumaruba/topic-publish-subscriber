import zmq

from .client import Client
from .program import SocketCreationFunction


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
        socket = self.create_socket(zmq.SUB, SocketCreationFunction.CONNECT, 'localhost:5557')
        socket.setsockopt_string(zmq.SUBSCRIBE, "10001")

        total_temperature = 0
        for update_number in range(1, 10 + 1):
            string = socket.recv_string()
            zipcode, temperature, relative_humidity = string.split()
            total_temperature += int(temperature)

            print(f"Average temperature: {total_temperature / update_number} ({temperature})")
