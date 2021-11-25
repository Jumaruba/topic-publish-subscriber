import pickle
import random
import time

import json
import os
import zmq

from .client import Client
from .log.logger import Logger
from .message.message_parser import MessageParser
from .program import SocketCreationFunction


class Publisher(Client):
    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    publisher: zmq.Socket
    messages: dict  # List of messages to send
    fault_server: zmq.Socket  # Error messages that comes from the server
    put_topic_dict: dict  # Last_topic_msg[topic] = message_id   # last message sent from each topic
    topic_names: list  # Possible topics
    n_topics: int  # Number of topics

    # --------------------------------------------------------------------------
    # Initialization of publisher
    # --------------------------------------------------------------------------

    def __init__(self, messages_json: str, client_id: str) -> None:
        super().__init__(client_id)

        self.put_topic_dict = {}
        self.get_state()

        self.init_sockets()
        self.get_messages(messages_json)
        self.topic_names = list(self.messages.keys())
        self.n_topics = len(self.topic_names)

    def init_sockets(self) -> None:
        self.publisher = self.create_socket(zmq.PUB, SocketCreationFunction.CONNECT, 'localhost:5556')
        self.fault_server = self.create_socket(zmq.SUB, SocketCreationFunction.CONNECT, 'localhost:5552')
        # Subscribe to receive fault messages from server.
        self.fault_server.setsockopt_string(zmq.IDENTITY, self.id)
        self.fault_server.setsockopt_string(zmq.SUBSCRIBE, self.id)

    def get_messages(self, messages_json: str):
        f = open(messages_json + ".json")
        self.messages = json.load(f)
        f.close()

    def put(self, topic: str, msg_id: int, content: str) -> None:
        self.publisher.send_multipart(MessageParser.encode([topic, self.id, content, msg_id]))
        Logger.put_message(self.id, topic, msg_id, content)

    def handle_fault(self):
        try:
            message = self.fault_server.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again as e:
            return

        Logger.new_message(message)
        _, topic, msg_id = MessageParser.decode(message)
        content = self.messages[topic][int(msg_id) % len(self.messages[topic])]
        self.put(topic, msg_id, content)

    def publication(self):
        # Get random topic
        topic = self.topic_names[random.randint(0, self.n_topics - 1)]

        # Get id of the next message message to send
        msg_id = self.get_next_message(topic)
        content = self.messages[topic][msg_id % len(self.messages[topic])]

        self.put_topic_dict[topic] = msg_id
        self.put(topic, msg_id, str(content))

    # -------------------------------------------------------------------------
    # State Functions
    # -------------------------------------------------------------------------

    def get_next_message(self, topic: str) -> int:
        if topic not in self.put_topic_dict:
            return 0
        return self.put_topic_dict[topic] + 1

    def save_state(self) -> None:
        current_path = os.path.dirname(__file__) + "/../../data/"
        data_path = os.path.join(current_path, f"publisher_{self.id}.pkl")
        f = open(data_path, "wb+")
        pickle.dump(self.put_topic_dict, f)
        f.close()

    def get_state(self) -> None:
        current_path = os.path.dirname(__file__) + "/../../data/"
        data_path = os.path.join(current_path, f"publisher_{self.id}.pkl")
        if os.path.exists(data_path):
            f = open(data_path, "rb")
            self.put_topic_dict = pickle.load(f)
            f.close()

    # --------------------------------------------------------------------------
    # Main function of publisher
    # --------------------------------------------------------------------------

    def run(self) -> None:

        # TODO check if socket is connected before starting to send messages

        while True:
            try:
                # Send publication
                self.publication()

                # Handles lost messages from the server.
                self.handle_fault()

                # TODO: delete this?
                time.sleep(2)
                self.save_state()

            except KeyboardInterrupt:
                self.save_state()
                Logger.err("Keyboard interrupt")
                exit()
