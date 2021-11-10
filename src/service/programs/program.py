from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum

import zmq
from zmq.backend import Socket
from .excpt.create_socket import CreateSocket

class SocketCreationFunction(Enum):
    CONNECT = 0
    BIND = 1


class Program(ABC):
    def __init__(self) -> None:
        self.context = zmq.Context()

    def create_socket(self, socket_type: zmq.TYPE, function: SocketCreationFunction, address: str) -> Socket | None:
        new_socket = self.context.socket(socket_type)
        if function == SocketCreationFunction.CONNECT:
            new_socket.connect("tcp://" + address)
        elif function == SocketCreationFunction.BIND:
            new_socket.bind("tcp://" + address)
        else:
            raise CreateSocket()
        return new_socket

    @abstractmethod
    def run(self):
        pass
