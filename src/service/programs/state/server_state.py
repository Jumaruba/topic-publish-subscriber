from __future__ import annotations
import json 

from .state import State


class ServerState(State):

    # --------------------------------------------------------------------------
    # Initialization
    # --------------------------------------------------------------------------

    topic_dict: dict       # topic_dict[<topic>][<message id>] = message
    client_dict: dict      # client_dict[<client id>][<topic>] = last message received 
    publish_dict: dict     # publish_dict[<topic>][<pub_id>] 
    pending_clients: dict  # pending_clients[<topic>] = list of clients waiting
    

    def __init__(self, data_path: str) -> None:
        super().__init__(data_path)
        self.topic_dict = {}
        self.client_dict = {}
        self.pending_clients = {}

    @staticmethod
    def read_state(data_path: str):
        state = State.get_state_from_file(data_path)
        if state is None:
            return ServerState(data_path)
        return state

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

    def is_unsubscribed_topic(self, topic: str) -> bool:
        for client in self.client_dict.keys():
            client_topics = self.client_dict[client].keys()
            if topic in client_topics:
                return False
        return True

    def is_unsubscribed_client(self, client_id: str) -> bool:
        return self.client_dict[client_id] == {}

    def first_message(self, topic: str) -> int:
        if not self.topic_dict[topic]:
            return -1
        return next(iter(self.topic_dict[topic].keys()))

    def last_message_received_by_all(self, topic: str) -> int:
        result = float('inf')
        for topics in self.client_dict.values():
            pass
            # TODO - fix this (issue #9)
            #result = min(result, topics[topic])
        return result

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
        self.collect_garbage(topic)

    # --------------------------------------------------------------------------
    # Remove data
    # --------------------------------------------------------------------------

    def delete_messages_until(self, topic: str, limit: int) -> None:
        key_list = list(iter(self.topic_dict[topic].keys()))
        for key in key_list:
            if key > limit:
                break
            self.topic_dict[topic].pop(key)

    def collect_garbage(self, topic: str) -> None:
        first_message = self.first_message(topic)
        # TODO: this last message is always
        last_message = self.last_message_received_by_all(topic)

        # No messages to delete
        if last_message == -1:
            return

        if last_message > first_message:
            self.delete_messages_until(topic, last_message - 1)

    def remove_topic(self, topic: str) -> None:
        self.topic_dict.pop(topic)
        self.pending_clients.pop(topic)

    def remove_subscriber(self, client_id: int, topic: str) -> None:
        """
        Removes a subscriber from the topics structure
        """
        self.client_dict[client_id].pop(topic)

        if self.is_unsubscribed_topic(topic):
            self.remove_topic(topic)

    def empty_waiting_list(self, topic: str) -> None:
        self.pending_clients[topic] = [] 


    def __str__(self):
        str_topic_dict = json.dumps(self.topic_dict)
        str_client_dict = json.dumps(self.client_dict)
        str_pending_clients = json.dumps(self.pending_clients)
        return f"""
            [TOPICS] topic_dict[<topic>][<message_id>] = message
            {str_topic_dict}

            [CLIENTS] client_dict[<client_id>][<topic>] = last_message_received
            {str_client_dict}

            [PENDING CLIENTS] pending_client[<topic>] = list of clients waiting
            {str_pending_clients}
        """

