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
    messages: list               # list of messages to send
    put_topic_dict: dict         # last_topic_msg[topic] = message_id   # last message sent from each topic
    fault_server: zmq.Socket     # Error messages that comes from the server
    
    # --------------------------------------------------------------------------
    # Initialization of publisher
    # --------------------------------------------------------------------------

    def __init__(self, messages_json: str) -> None:
        super().__init__() 
        # TODO: change to receive id from the input
        self.id = str(random.randint(0, 8000))
        self.put_topic_dict = {} 

        self.init_sockets()
        self.get_messages(messages_json)
        self.n_topics = len(self.messages)

    def init_sockets(self) -> None:
        self.publisher = self.create_socket(zmq.PUB, SocketCreationFunction.CONNECT, 'localhost:5556') 
        self.fault_server = self.create_socket(zmq.SUB, SocketCreationFunction.CONNECT, 'localhost:5552') 
        self.fault_server.setsockopt(zmq.IDENTITY, self.id.encode('utf-8')) # Subscribe to receive fault messages from server. 
        self.fault_server.setsockopt(zmq.SUBSCRIBE, self.id.encode('utf-8'))

    def get_messages(self, messages_json: str):
        f = open(messages_json + ".json")
        self.messages = json.load(f).get("topics")
        f.close()

    def put(self, topic: str, msg_id: int, content: str) -> None:   
        # TODO: delete this
        if msg_id == 2:
            return 
        self.publisher.send_multipart(MessageParser.encode([topic, self.id, content, msg_id]))
        Logger.put_message(self.id, topic, msg_id, content)
    
    def handle_fault(self):
        try:
            message = self.fault_server.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again as e:
            return

        Logger.new_message(message)
        print(self.messages)
        pub_id, topic, msg_id = MessageParser.decode(message) 
        content = self.messages[0]["messages"][int(msg_id) % len(topic['messages'])]        
        self.put(topic, msg_id, content)


    def publication(self):
        # Get random topic
        topic = self.messages[random.randint(0, self.n_topics-1)]
        
        # Get id of the next message message to send
        msg_id = self.get_next_message(topic["name"])
        content = topic["messages"][msg_id % len(topic["messages"])]

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

        # TODO check if socket is connected before starting to send messages
        # TODO save the state in memory 

        while True:
            # Send publication
            self.publication()

            # Handles lost messages from the server.
            self.handle_fault()

            time.sleep(2)