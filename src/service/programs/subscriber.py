from __future__ import annotations

import os

import zmq
import pickle
import random
from time import sleep
import json

from .log.logger import Logger
from .message.message_parser import MessageParser

from .client import Client
from .program import SocketCreationFunction



class Subscriber(Client):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    data_path: str
    client_id: int
    topics: list
    messages_received: dict  # messages_received[topic] = message_id
    last_get: str | None

    # --------------------------------------------------------------------------
    # Initialization of subscriber
    # --------------------------------------------------------------------------

    def __init__(self, topics_json: str, client_id: int):
        super().__init__()
        persistent_data_path = f"data/client_status_{client_id}.bin"
        current_path = os.path.dirname(__file__)
        self.data_path = os.path.join(current_path, persistent_data_path)
        self.client_id = client_id
        self.last_get = None
        self.messages_received = {}

        self.create_sockets()
        self.get_topics(topics_json)
        self.get_state()
        self.subscribe_topics()

    def get_state(self):
        if os.path.exists(self.data_path):
            f = open(self.data_path, 'rb')
            self.messages_received = pickle.load(f)
            f.close()
            self.handle_crash()

    def handle_crash(self):
        if self.last_get is not None:
            msg_id = self.messages_received[self.last_get]
            self.dealer.send_multipart(MessageParser.encode(["ACK", self.last_get, msg_id]))

    def delete_state(self):
        pass

    def create_sockets(self) -> None:
        self.subscriber = self.context.socket(zmq.XSUB)
        self.subscriber.connect("tcp://localhost:5557")

        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.dealer.connect("tcp://localhost:5554")

    def get_topics(self, topics_json):
        f = open(topics_json + ".json")
        self.topics = json.load(f).get("topics")
        f.close()


    def subscribe_topics(self):
        for topic in self.topics:
            self.subscribe(topic)
            Logger.subscribe(topic)

    def unsubscribe_topics(self):
        for topic in self.topics:
            self.unsubscribe(topic)
            self.topics.pop(topic)
            Logger.unsubscribe(topic)

    # --------------------------------------------------------------------------
    # Subscrition functions
    # --------------------------------------------------------------------------

    def subscribe(self, topic: str) -> None:
        self.subscriber.send_multipart(
            [b'\x10' + self.client_id.encode('utf-8'), b'\x01' + topic.encode('utf-8')])
        print("SENT SUBSCRITPION")

    def unsubscribe(self, topic: str) -> None:
        self.subscriber.send_multipart(
            [b'\x10' + self.client_id.encode('utf-8'), b'\x00' + topic.encode('utf-8')])

    # --------------------------------------------------------------------------
    # Message handling functions
    # --------------------------------------------------------------------------

    def get(self, topic: str) -> None:
        self.last_get = topic
        self.dealer.send_multipart(MessageParser.encode(['GET', topic]))
        Logger.get(self.client_id, topic)

    def handle_msg(self) -> None:
        """ This function receive a message of a topic and sends the ACK. """

        [topic, msg_id, content] = MessageParser.decode(
            self.dealer.recv_multipart())

        Logger.topic_message(topic, msg_id, content)

        self.messages_received[topic] = msg_id

        data_persitence_file = open(self.data_path, "wb")
        pickle.dump(self.messages_received, data_persitence_file)
        data_persitence_file.close()

        self.dealer.send_multipart(
            MessageParser.encode(['ACK', topic, msg_id]))

    # --------------------------------------------------------------------------
    # Main function of subscriber
    # --------------------------------------------------------------------------

    def run(self):
        # TODO - if 'limit' arg is specified, unsubscribe after sending 'limit' GETs, otherwise use and infinite loop
        while True:
            # Get random subscribed topic
            topic_idx = random.randint(0, len(self.topics)-1)
            topic = self.topics[topic_idx]

            # Get message from a topic
            self.get(topic)

            # Send ACK
            self.handle_msg()

        #self.unsubscribe_topics()
