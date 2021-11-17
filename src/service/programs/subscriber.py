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
        current_data_path = os.path.abspath(os.getcwd())   
        persistent_data_path = f"/data/client_status_{client_id}.bin" 
        data_path = current_data_path + persistent_data_path

        self.client_id = client_id
        self.state = SubscriberState(data_path, topics_json)

        self.create_sockets()
        self.subscribe_topics()


    def create_sockets(self) -> None:
        self.subscriber = self.context.socket(zmq.XSUB)
        self.subscriber.connect("tcp://localhost:5557")

        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.dealer.connect("tcp://localhost:5554")

    # --------------------------------------------------------------------------
    # Handle Crash
    # --------------------------------------------------------------------------

    def handle_crash(self):
        if self.state.last_get is not None:
            msg_id = self.state.messages_received[self.state.last_get]
            self.dealer.send_multipart(MessageParser.encode(["ACK", self.state.last_get, msg_id]))

    # --------------------------------------------------------------------------
    # Subscrition functions
    # --------------------------------------------------------------------------

    def subscribe_topics(self):
        for topic in self.state.topics:
            self.subscribe(topic)
            Logger.subscribe(topic)

    def unsubscribe_topics(self):
        for topic in self.state.topics:
            self.unsubscribe(topic)
            self.state.topics.pop(topic)
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
        self.state.last_get = topic
        self.dealer.send_multipart(MessageParser.encode(['GET', topic]))
        Logger.get(self.client_id, topic)

    def handle_msg(self) -> None:
        """ This function receive a message of a topic and sends the ACK. """

        [topic, msg_id, content] = MessageParser.decode(
            self.dealer.recv_multipart())

        Logger.topic_message(topic, msg_id, content)

        self.state.add_message(topic, msg_id)
        
        # TODO - change this in order to save the SubscriberState(de x em x tempo) 
        self.state.save_state()

        self.dealer.send_multipart(
            MessageParser.encode(['ACK', topic, msg_id]))

    # --------------------------------------------------------------------------
    # Main function of subscriber
    # --------------------------------------------------------------------------

    def run(self):
        # TODO - if 'limit' arg is specified, unsubscribe after sending 'limit' GETs, otherwise use and infinite loop
        while True:
            # Get random subscribed topic
            topic_idx = random.randint(0, len(self.state.topics)-1)
            topic = self.state.topics[topic_idx]

            # Get message from a topic
            self.get(topic)

            # Send ACK
            self.handle_msg()

        #self.unsubscribe_topics()
