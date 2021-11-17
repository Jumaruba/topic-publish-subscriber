from __future__ import annotations

class ServerState:

    # --------------------------------------------------------------------------
    # Initialization
    # --------------------------------------------------------------------------

    topic_dict: dict       # topic_dict[<topic>][<message id>] = message
    client_dict: dict      # client_dict[<client id>][<topic>] = last message received
    pending_clients: dict  # pending_clients[<topic>] = list of clients waiting

    def __init__(self) -> None:
        self.topic_dict = {}
        self.client_dict = {}
        self.pending_clients = {}

    # --------------------------------------------------------------------------
    # Get data
    # --------------------------------------------------------------------------

    def last_message_of_topic(self, topic: str) -> int:
        """
        Returns the id of the last message of the topic that was received
        from a publisher
        """
        if len(self.topic_dict[topic]) == 0:
            return -1
        return list(self.topic_dict[topic].keys())[-1]

    def check_client_subscription(self, client_id: int, topic: str) -> int | None:
        """
        Returns the position to the last message a client received.
        Checks if the client exists and if it is subscribed to the topic
        """
        position = self.client_dict.get(client_id, {}).get(topic)
        return position

    def message_for_client(self, client_id: int, topic: str) -> list:
        """
        Returns the next message that needs to be send to the client,
        in the following format: [client_id, topic, msg_id, msg_content]
        """
        last_message_id = self.client_dict[client_id][topic]
        next_message_id = last_message_id + 1

        # There's no message for this client,
        # it needs to wait for a new message from a publisher
        if next_message_id not in self.topic_dict[topic]:
            return None

        next_message = self.topic_dict[topic][next_message_id]
        return [client_id, topic, next_message_id, next_message]

    def get_waiting_list(self, topic: str) -> list:
        return self.pending_clients[topic]

    def is_unsubscribed_topic(self, topic: str) -> None:
        for _, topics in client_dict:
            if topic in topics:
                return True
        return False

    def is_unsubscribed_client(seld, client_id: str) -> None:
        pass
        # if client_dict[client_id]

    # --------------------------------------------------------------------------
    # Add data
    # --------------------------------------------------------------------------

    def add_topic(self, topic: str) -> None:
        """
        Adds a topic to the topics data structure if it is not in it already
        """
        if topic not in self.topic_dict:
            self.topic_dict[topic] = {}
        if topic not in self.pending_clients:
            self.pending_clients[topic] = []

    def add_client(self, client_id: int) -> None:
        """
        Adds a client to the clients data structure if it is not in it already
        """
        if client_id not in self.client_dict:
            self.client_dict[client_id] = {}

    def add_message(self, topic: str, message: str) -> int:
        """
        Adds a message to the data structure and returns the id created for it
        """
        self.add_topic(topic)
        # The ids are sequential
        new_id = self.last_message_of_topic(topic) + 1
        self.topic_dict[topic][new_id] = message
        return new_id

    def add_subscriber(self, client_id: int, topic: str) -> None:
        """
        Adds a subscriber to the topics structure
        """
        self.add_topic(topic)
        self.add_client(client_id)
        # The next message this client needs to receive is the next of the topic
        self.client_dict[client_id][topic] = self.last_message_of_topic(topic)

    def add_to_waiting_list(self, client_id: int, topic: str) -> None:
        self.pending_clients[topic].append(client_id)

    # --------------------------------------------------------------------------
    # Update data
    # --------------------------------------------------------------------------

    def update_client_last_message(self, client_id: int, topic: str, message_id: int) -> None:
        self.client_dict[client_id][topic] = message_id

    # --------------------------------------------------------------------------
    # Remove data
    # --------------------------------------------------------------------------

    def remove_subscriber(self, client_id: int, topic: str) -> None:
        """
        Removes a subscriber from the topics structure
        """
        self.client_dict[client_id].pop(topic)

        # TODO: remove messages if the client is the last one of the topic

    def empty_waiting_list(self, topic: str) -> None:
        self.pending_clients[topic] = []