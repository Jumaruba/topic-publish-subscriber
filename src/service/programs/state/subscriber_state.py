from __future__ import annotations

import json

from .state import State

class SubscriberState(State):

    # --------------------------------------------------------------------------
    # Initialization
    # --------------------------------------------------------------------------

    topics: list
    messages_received: dict  # messages_received[topic] = message_id
    last_get: str | None
    data_persitence_file: str

    def __init__(self, data_path: str, topics_json: str) -> None:
        super().__init__(data_path)
        self.last_get = None
        self.messages_received = {}
        self.topics = self.get_topics(topics_json)

    def get_topics(self, topics_json: str):
        f = open(topics_json + ".json")
        topics = json.load(f).get("topics")
        f.close()
        return topics

    def add_message(self, topic: str, msg_id: int):
        self.messages_received[topic] = msg_id

    @staticmethod
    def read_state(path: str, topics_json: str):
        state = State.get_state_from_file(path)
        if state is None:
            return SubscriberState(data_path, topics_json)



