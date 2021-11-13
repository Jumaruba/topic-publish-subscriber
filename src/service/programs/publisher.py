import random
import time

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
    topic: str

    # --------------------------------------------------------------------------
    # Initialization of publisher
    # --------------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__() 
        self.init_sockets()
        self.topic = "sdle" # to change, receive multiple topics

    def init_sockets(self) -> None:
        self.publisher = self.create_socket(zmq.PUB, SocketCreationFunction.CONNECT, 'localhost:5556')

    def put(self, topic: str, msg_id: int, content: str) -> None:
        self.publisher.send_multipart([self.topic.encode('utf-8'), str(content).encode('utf-8'), str(msg_id).encode('utf-8')])
        
        Logger.topic_message(topic, msg_id, content)
    
    # --------------------------------------------------------------------------
    # Main function of publisher
    # --------------------------------------------------------------------------

    def run(self) -> None: 
        msg_id = 0

        ## TODO check if socket is connected before starting to send messages
        
        while True:  
            content = random.randint(10, 1000)
            self.put(self.topic, msg_id, str(content))

            time.sleep(2)

            msg_id += 1