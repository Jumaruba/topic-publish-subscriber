from dataclasses import dataclass


@dataclass
class Subscription:
    subscriber_id: int             
    topic: str 

