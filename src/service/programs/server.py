import zmq

from zmq import backend
from zmq.sugar.socket import Socket

from .program import SocketCreationFunction
from .program import Program
from .log.logger import Logger
from .excpt.create_socket import CreateSocket
from .message.message_parser import MessageParser


class Server(Program):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    # Sockets
    poller: zmq.Poller
    backend: zmq.Socket
    frontend: zmq.Socket
    router: zmq.Socket

    # Dictionaries
    topic_dict: dict    # topic_dict[<topic>][<message id>] = message
    # client_dict[<client id>][<topic>] = last message received
    client_dict: dict

    # --------------------------------------------------------------------------
    # Initialization of server
    # --------------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__()
        self.init_sockets()
        self.create_poller()
        self.topic_dict = {}
        self.client_dict = {}

    def create_poller(self) -> None:
        self.poller = zmq.Poller()
        self.poller.register(self.frontend, zmq.POLLIN)
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.router, zmq.POLLIN)

    def init_sockets(self) -> None:
        self.backend = self.create_socket(
            zmq.XSUB, SocketCreationFunction.BIND, '*:5556')
        self.frontend = self.create_socket(
            zmq.XPUB, SocketCreationFunction.BIND, '*:5557')
        self.router = self.create_socket(
            zmq.ROUTER, SocketCreationFunction.BIND, '*:5554')

    # --------------------------------------------------------------------------
    # Data structure functions
    # --------------------------------------------------------------------------

    def last_message_of_topic(self, topic: str) -> int:
        """
        Returns the id of the last message of the topic that was received
        from a publisher
        """
        if len(self.topic_dict[topic]) == 0:
            return -1
        return self.topic_dict[topic].keys()[-1]

    def add_topic(self, topic: str) -> None:
        """
        Adds a topic to the topics data structure if it is not in it already
        """
        if topic not in self.topic_dict:
            self.topic_dict[topic] = {}

    def add_client(self, client_id: int) -> None:
        """
        Adds a client to the clients data structure if it is not in it already
        """
        if client_id not in self.client_dict:
            self.client_dict[client_id] = {}

    def add_message(self, topic: str, message: str) -> int:
        """
        Adds a message to the data structure and returns the id created for it
        """
        self.add_topic(topic)
        # The ids are sequential
        new_id = self.last_message_of_topic(topic) + 1
        self.topic_dict[topic][new_id] = message
        return new_id

    def add_subscriber(self, topic: str, client_id: int) -> None:
        """
        Adds a client to the topics structure and returns the id created for it
        """
        self.add_topic(topic)
        self.add_client(client_id)
        # The next message this client needs to receive is the next of the topic
        self.client_dict[client_id][topic] = self.last_message_of_topic()

    # --------------------------------------------------------------------------
    # Handlers of messages
    # --------------------------------------------------------------------------

    def handle_subscription(self) -> None:
        """
        Reads the message from the frontend socket, forwards it to the
        publishers and adds the new subscription to the data structures
        """
        message = self.frontend.recv_multipart()
        Logger.frontend(message)
        self.backend.send_multipart(message)
        # TODO: add_subscriber() - maybe convert the identity to int after decoding

    def handle_publication(self) -> None:
        """
        Reads the message from the backend socket, created a new id for it,
        sends it to the subscribers and adds the new message to the data
        structures
        """
        message = self.backend.recv_multipart()
        Logger.backend(message)
        # TODO: new_id = add_message()
        # TODO: create new message to send to client
        self.frontend.send_multipart(message)

    def handle_dealer(self) -> None:
        identity, message_type, topic, * \
            message_id = MessageParser.decode(self.router.recv_multipart())

        if message_type == "GET":
            self.handle_get(header, identity, topic)
        if message_type == "ACK":
            self.handle_acknowledgement(identity, message_id, topic)

    def handle_get(self, client_id: int, topic: str) -> None:
        Logger.get(client_id, topic)

        # TODO fetch message from local data sctructure
        # to_send = MessageParser.encode([topic, msg_id, msg_content])

        to_send = MessageParser.encode([client_id, topic, 2, "test message"])
        self.router.send_multipart(to_send)

    def handle_acknowledgement(self, client_id: int, message_id: int, topic: str) -> None:
        Logger.ack(client_id, topic, message_id)

        print(f'CLIENT ID - {client_id}')

        current_pointer = self.client_dict[client_id][topic]
        if message_id == (current_pointer + 1):
            self.client_dict[client_id][topic] = message_id
        else:
            pass
            # TODO deal with mismatch message id in ACK (if we choose to do this other than re-send when no ACK is received)

    # --------------------------------------------------------------------------
    # Main function of server
    # --------------------------------------------------------------------------

    def run(self) -> None:
        """
        Runs the server, which includes handling subscriptions, publications,
        acknowledgements and error treatment
        """
        while True:
            socks = dict(self.poller.poll())

            # Receives subscription
            if socks.get(self.frontend) == zmq.POLLIN:
                self.handle_subscription()

            # Receives content from publishers
            if socks.get(self.backend) == zmq.POLLIN:
                self.handle_publication()

            # Receives message from subscribers
            if socks.get(self.router) == zmq.POLLIN:
                self.handle_dealer()
