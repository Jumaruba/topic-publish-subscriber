from abc import ABC

from .program import Program


class Client(Program, ABC):
    def __init__(self) -> None:
        super().__init__()
