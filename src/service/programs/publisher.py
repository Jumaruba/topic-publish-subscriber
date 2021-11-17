import random
import time
import json

import zmq
from zmq.sugar.socket import Socket

from .log.logger import Logger

from .client import Client
from .program import SocketCreationFunction

class Publisher(Client):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    publisher: zmq.Socket
    messages: list
    topic: str

    # --------------------------------------------------------------------------
    # Initialization of publisher
    # --------------------------------------------------------------------------

    def __init__(self, messages_json: str) -> None:
        super().__init__()
        self.init_sockets()
        self.get_messages(messages_json)

    def init_sockets(self) -> None:
        self.publisher = self.create_socket(zmq.PUB, SocketCreationFunction.CONNECT, 'localhost:5556')

    def get_messages(self, messages_json: str):
        f = open(messages_json + ".json")
        self.messages = json.load(f).get("topics")
        f.close()

    def put(self, topic: str, msg_id: int, content: str) -> None:
        self.publisher.send_multipart([topic.encode('utf-8'), str(content).encode('utf-8'), str(msg_id).encode('utf-8')])

        Logger.put_message(topic, msg_id, content)

    # --------------------------------------------------------------------------
    # Main function of publisher
    # --------------------------------------------------------------------------

    def run(self) -> None:
        msg_id = 0
        n_topics = len(self.messages)

        ## TODO check if socket is connected before starting to send messages

        while True:
            # Get random message from random topic
            topic_idx = random.randint(0, n_topics-1)
            topic = self.messages[topic_idx]
            content_idx = random.randint(0, len(topic["messages"])-1)
            content = topic["messages"][content_idx]

            self.put(topic["name"] , msg_id, str(content))

            time.sleep(2)

            msg_id += 1