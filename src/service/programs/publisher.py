import random
import time
import json

import zmq
from zmq.sugar.socket import Socket

from .log.logger import Logger

from .client import Client
from .program import SocketCreationFunction
from .message.message_parser import MessageParser

class Publisher(Client):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    publisher: zmq.Socket
    messages: list              # list of messages to send
    ack_topic_dict: dict            # topic_dict[topic] = message_id  # confirmed puts
    last_put: str               # topic published in the last put call
    put_topic_dict: dict         # last_topic_msg[topic] = message_id   # last message sent from each topic
    ack_server: zmq.Socket      
    
    # --------------------------------------------------------------------------
    # Initialization of publisher
    # --------------------------------------------------------------------------

    def __init__(self, messages_json: str) -> None:
        super().__init__() 
        # TODO: change to receive id from the input
        self.id = str(random.randint(0, 8000)).encode('utf-8')
        self.ack_topic_dict = {}
        self.put_topic_dict = {}
        self.last_put = None  

        self.init_sockets()
        self.get_messages(messages_json)
        self.n_topics = len(self.messages)

    def init_sockets(self) -> None:
        self.publisher = self.create_socket(zmq.PUB, SocketCreationFunction.CONNECT, 'localhost:5556') 
        self.ack_server = self.create_socket(zmq.SUB, SocketCreationFunction.CONNECT, 'localhost:5552') 
        self.ack_server.setsockopt(zmq.IDENTITY, self.id) # Subscribe to receive acks. 
        self.ack_server.setsockopt(zmq.SUBSCRIBE, self.id)

    def get_messages(self, messages_json: str):
        f = open(messages_json + ".json")
        self.messages = json.load(f).get("topics")
        f.close()

    def put(self, topic: str, msg_id: int, content: str) -> None: 
        self.last_put = topic
        self.publisher.send_multipart(MessageParser.encode([topic, self.id, content, msg_id]))
        Logger.put_message(self.id, topic, msg_id, content)
    

    def handle_ack(self):
        """
        Updates state if ACK is received
        """
        try:
            message = self.ack_server.recv_multipart(flags=zmq.NOBLOCK)
            msg_id, topic = MessageParser.decode(message)
            self.topic_dict[topic] = int(msg_id)
            self.last_put = None
            Logger.acknowledgement_pub(topic, msg_id)
        except zmq.Again as e:
            return

    def publication(self):
        # Get random topic
        topic = self.messages[random.randint(0, self.n_topics-1)]
        
        # Get id of the next message message to send
        msg_id = self.get_next_message(topic["name"])
        content = topic["messages"][msg_id]

        self.put_topic_dict[topic["name"]] = msg_id
        self.put(topic["name"] , msg_id, str(content))

    # -------------------------------------------------------------------------
    # State Functions
    # -------------------------------------------------------------------------

    def get_next_message(self, topic: str) -> int:
        if topic not in self.put_topic_dict:
            return 0
        return self.put_topic_dict[topic] + 1
    
    # --------------------------------------------------------------------------
    # Main function of publisher
    # --------------------------------------------------------------------------

    def run(self) -> None:

        ## TODO check if socket is connected before starting to send messages

        while True:
            # Send publication
            self.publication()

            # Receive ACKS
            self.handle_ack()

            time.sleep(2)