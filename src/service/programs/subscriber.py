import os

import zmq
import pickle
import random
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
    messages_received: dict  # messages_received[topic][message_id] = message
    topics: list

    # --------------------------------------------------------------------------
    # Initialization of subscriber
    # --------------------------------------------------------------------------

    def __init__(self, topics_json: str, client_id: int):
        super().__init__() 
        persistent_data_path = f"data/client_status_{client_id}.bin"
        current_path = os.path.dirname(__file__)
        self.data_path = os.path.join(current_path, persistent_data_path)
        self.client_id = client_id 

        self.get_state()
        self.create_sockets()
        self.create_poller()
        self.get_topics(topics_json)

        self.messages_received = {}
        self.subscribe_topics()
    
    def get_state(self):
        if os.path.exists(self.data_path):
            f = open(self.data_path, 'rb')
            content = pickle.load(f)        # Probably going to open as dict
            print(content)
            f.close() 
            self.handle_crash(content)

    def handle_crash(self, state: dict):  
        message = ["CRASH"]
        for topic in state.items():
            pass
        self.dealer.send_multipart(MessageParser.encode(message))
    
    def delete_state(self):
        pass

    def create_sockets(self) -> None:
        self.subscriber = self.context.socket(zmq.XSUB)
        self.subscriber.connect("tcp://localhost:5557")

        # TODO check if client ID already defined to restart client with the same ID instead of ceating a new one
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.dealer.connect("tcp://localhost:5554")


    def create_poller(self) -> None:
        self.poller = zmq.Poller()
        self.poller.register(self.dealer, zmq.POLLIN)


    def get_topics(self, topics_json):
        f = open(topics_json + ".json")
        self.topics = json.load(f).get("topics")
        f.close()


    def subscribe_topics(self):
        for topic in self.topics:
            self.subscribe(topic)
            Logger.subscribe(topic)

    # --------------------------------------------------------------------------
    # Subscrition functions
    # --------------------------------------------------------------------------

    def subscribe(self, topic: str) -> None:
        # TODO

        self.identity = str(zmq.IDENTITY).encode('utf-8')
        print(f'decoded ID - {zmq.IDENTITY}')

        self.subscriber.send_multipart(
            [b'\x10' + self.identity, b'\x01' + topic.encode('utf-8')])

    def unsubscribe(self, topic: str) -> None:
        # TODO
        pass

    # --------------------------------------------------------------------------
    # Message handling functions
    # --------------------------------------------------------------------------

    def get(self, topic: str) -> None:
        self.dealer.send_multipart(MessageParser.encode(['GET', topic]))
        Logger.get(zmq.IDENTITY, topic)

        self.poller.poll()
        self.handle_msg()

    def handle_msg(self) -> None:
        """ This function is responsible for receiving the topic messages and send the acks. """

        [topic, msg_id, content] = MessageParser.decode(
            self.dealer.recv_multipart())

        Logger.topic_message(topic, msg_id, content)

        self.messages_received[topic] = msg_id

        data_persitence_file = open(self.data_path, "wb")
        pickle.dump(self.messages_received, data_persitence_file)
        data_persitence_file.close()

        self.dealer.send_multipart(
            MessageParser.encode(['ACK', topic, msg_id]))

    def recv_status(self) -> None:
        """
        TODO receive message from the router
        """
        msg = self.dealer.recv()
        print(msg)

    # --------------------------------------------------------------------------
    # Main function of subscriber
    # --------------------------------------------------------------------------

    def run(self):

        for i in range(5):
            # Get random subscribed topic
            topic_idx = random.randint(0, len(self.topics)-1)
            topic = self.topics[topic_idx]

            self.get(topic)
