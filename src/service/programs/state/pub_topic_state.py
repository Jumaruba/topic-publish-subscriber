class PubTopicState:

    def __init__(self):
        self.last_msg = -1
        self.waiting_messages = []

    def remove_waiting(self, msg_id: int):
        self.waiting_messages.remove(msg_id)

    def add_waiting(self, msg_id: int):
        self.waiting_messages.append(msg_id)

    def is_waiting(self, msg_id: int):
        if msg_id in self.waiting_messages:
            return True
        return False

    def __str__(self):
        return f"""
        === PUB TOPIC STATE
            [WAITING]
            {self.waiting_messages}

            [LAST MSG]
            {self.last_msg}
        """
