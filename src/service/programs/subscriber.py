from __future__ import annotations

import os
import zmq
import random
from time import sleep
import json

from .log.logger import Logger
from .message.message_parser import MessageParser

from .client import Client
from .state.subscriber_state import SubscriberState
from .program import SocketCreationFunction



class Subscriber(Client):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    data_path: str
    client_id: int
    topics: list
    state: SubscriberState

    # --------------------------------------------------------------------------
    # Initialization of subscriber
    # --------------------------------------------------------------------------

    def __init__(self, topics_json: str, client_id: int):
        super().__init__()

        # State
        current_data_path = os.path.abspath(os.getcwd())   
        persistent_data_path = f"/data/client_status_{client_id}.pkl" 
        data_path = current_data_path + persistent_data_path
        self.state = SubscriberState.read_state(data_path, topics_json)

        self.client_id = client_id
        self.create_sockets()

        # Subscribe if the subscriber is new, handle crash otherwise
        if self.state.is_new_subscriber():
            self.subscribe_topics()
        else:
            self.handle_crash()

    def create_sockets(self) -> None:
        self.subscriber = self.context.socket(zmq.XSUB)
        self.subscriber.connect("tcp://localhost:5557")

        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.dealer.connect("tcp://localhost:5554")

    # --------------------------------------------------------------------------
    # Subscrition functions
    # --------------------------------------------------------------------------

    def subscribe_topics(self):
        for topic in self.state.topics:
            self.subscribe(topic)
            Logger.subscribe(topic)
            ack = self.dealer.recv_multipart()
            print(ack)

    def unsubscribe_topics(self):
        for topic in self.state.topics:
            self.unsubscribe(topic)
            # NOTE if we are doing unsubsribe we should delete the state
            #self.state.topics.remove(topic)
            Logger.unsubscribe(topic)

    def subscribe(self, topic: str) -> None:
        self.subscriber.send_multipart(
            [b'\x10' + self.client_id.encode('utf-8'), b'\x01' + topic.encode('utf-8')])

    def unsubscribe(self, topic: str) -> None:
        self.subscriber.send_multipart(
            [b'\x10' + self.client_id.encode('utf-8'), b'\x00' + topic.encode('utf-8')])

    # --------------------------------------------------------------------------
    # Message handling functions
    # --------------------------------------------------------------------------

    def get(self, topic: str) -> None:
        self.state.set_last_get(topic)
        self.dealer.send_multipart(MessageParser.encode(['GET', topic]))
        Logger.get(self.client_id, topic)

    def handle_crash(self):
        """ Send ACK to the last topic requested with a GET before crashing """
        message = self.state.get_last_ack()
        self.dealer.send_multipart(MessageParser.encode(message))

    def handle_msg(self) -> None:
        """ This function receive a message of a topic and sends the ACK. """

        [topic, msg_id, content] = MessageParser.decode(
            self.dealer.recv_multipart())

        Logger.topic_message(topic, msg_id, content)
        self.state.add_message(topic, msg_id)
        self.state.save_state()
        self.dealer.send_multipart(
            MessageParser.encode(['ACK', topic, msg_id]))

    # --------------------------------------------------------------------------
    # Main function of subscriber
    # --------------------------------------------------------------------------

    def run(self):
        # TODO - if 'limit' arg is specified, unsubscribe after sending 'limit' GETs, otherwise use and infinite loop

        for i in range(5):
            # Get random subscribed topic
            topic_idx = random.randint(0, len(self.state.topics)-1)
            topic = self.state.topics[topic_idx]

            # Get message from a topic
            self.get(topic)

            # Send ACK
            self.handle_msg()

        self.unsubscribe_topics()
