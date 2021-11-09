import zmq

from .program import Program


class Client(Program):
    def __init__(self) -> None:
        super().__init__()
