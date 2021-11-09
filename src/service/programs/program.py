from abc import ABC, abstractmethod

import zmq


class Program(ABC):
    def __init__(self) -> None:
        self.context = zmq.Context()
        self.sockets = {}

    def create_socket(self, name: str, socket_type: zmq.TYPE, address: str) -> None:
        new_socket = self.context.socket(socket_type)
        new_socket.connect("tcp://" + address)
        self.sockets[name] = new_socket
        return self.sockets[name]

    @abstractmethod
    def run(self):
        pass
