from dataclasses import dataclass


@dataclass
class Acknowledgement:
    receiver_id: int             
    message_id: int
    # TODO: add topic