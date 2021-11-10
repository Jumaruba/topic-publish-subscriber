import zmq
from zmq import backend
from zmq.sugar.constants import XPUB

from .program import SocketCreationFunction
from .program import Program
from .log.logger import Logger
from .excpt.create_socket import CreateSocket

class Server(Program): 
    poller: zmq.Poller
    xpub: zmq.Socket
    xsub: zmq.Socket 

    def __init__(self) -> None:
        super().__init__() 
        self.init_sockets()
        self.create_poller()
    
    def create_poller(self) -> zmq.Poller:
        self.poller = zmq.Poller() 
        self.poller.register(self.frontend, zmq.POLLIN)
        self.poller.register(self.backend, zmq.POLLIN)

    def init_sockets(self) -> None :
        self.backend = self.create_socket(zmq.XSUB, SocketCreationFunction.BIND, '*:5556')   
        self.frontend = self.create_socket(zmq.XPUB, SocketCreationFunction.BIND, '*:5557')
    
    def run(self):
        while True: 
            socks = dict(self.poller.poll()) 
            
            # Receives Subscriptions
            if socks.get(self.frontend) == zmq.POLLIN:
                message = self.frontend.recv_multipart()
                Logger.frontend(message)
                self.backend.send_multipart(message)
                
            # Receives content from publishers
            if socks.get(self.backend) == zmq.POLLIN:
                message = self.backend.recv_multipart()
                Logger.backend(message)
                self.frontend.send_multipart(message)