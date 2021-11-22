from abc import ABC

from .program import Program


class Client(Program, ABC):

    client_id: str # Number of topics

    def __init__(self, client_id: str) -> None:
        super().__init__()
        self.id = client_id
