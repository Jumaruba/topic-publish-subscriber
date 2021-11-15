import os
import zmq
import pickle
import random

from .log.logger import Logger
from .message.message_parser import MessageParser

from .client import Client
from .program import SocketCreationFunction

PERSISTENT_DATA_PATH = "data/client_status.bin"


class Subscriber(Client):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    data_path: str
    client_id: int
    messages_received: dict  # messages_received[topic][message_id] = message

    # --------------------------------------------------------------------------
    # Initialization of subscriber
    # --------------------------------------------------------------------------

    def __init__(self):
        super().__init__()
        current_path = os.path.dirname(__file__)
        self.data_path = os.path.join(current_path, PERSISTENT_DATA_PATH)

        self.create_sockets()
        self.create_poller()

        self.topic = "sdle"  # to change, receive multiple topics
        self.subscribe(self.topic)
        Logger.subscribe(self.topic)

        self.messages_received = {}
        self.messages_received[self.topic] = {}

    def create_sockets(self) -> None:
        self.subscriber = self.context.socket(zmq.XSUB)
        self.subscriber.connect("tcp://localhost:5557")

        # TODO id to be generated in __init__ or retreieved from persistent data
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt_string(zmq.IDENTITY, str(random.randint(0, 8000))) 
        self.dealer.connect("tcp://localhost:5554")


    def create_poller(self) -> None:
        self.poller = zmq.Poller()
        self.poller.register(self.dealer, zmq.POLLIN)
        #self.poller.register(self.subscriber, zmq.POLLIN)

    # --------------------------------------------------------------------------
    # Subscrition functions
    # --------------------------------------------------------------------------

    def subscribe(self, topic: str) -> None:
        # TODO
        self.subscriber.send_multipart(
            [b'\x10' + str(zmq.IDENTITY).encode('utf-8'), b'\x01' + topic.encode('utf-8')])

    def unsubscribe(self, topic: str) -> None:
        # TODO
        pass

    # --------------------------------------------------------------------------
    # Message handling functions
    # --------------------------------------------------------------------------

    def get(self, topic: str) -> None:
        self.dealer.send_multipart(MessageParser.encode(['GET', topic]))

        self.poller.poll()
        self.handle_msg()

    def handle_msg(self) -> None:
        """ This function is responsible for receiving the topic messages and send the acks. """

        [topic, msg_id, content] = MessageParser.decode(
            self.dealer.recv_multipart())

        Logger.topic_message(topic, msg_id, content)
        self.dealer.send_multipart(
            MessageParser.encode(['ACK', topic, msg_id]))

        self.messages_received[topic][msg_id] = content

        data_persitence_file = open(self.data_path, "wb")
        pickle.dump(self.messages_received, data_persitence_file)
        data_persitence_file.close()


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
            self.get(self.topic)
