from __future__ import annotations

import zmq

from zmq import backend
from zmq.sugar.socket import Socket

from .program import SocketCreationFunction
from .program import Program
from .log.logger import Logger
from .excpt.create_socket import CreateSocket
from .message.message_parser import MessageParser
from typing import List

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
    topic_dict: dict       # topic_dict[<topic>][<message id>] = message
    client_dict: dict      # client_dict[<client id>][<topic>] = last message received
    pending_clients: dict  # pending_clients[<topic>] = list of clients waiting

    # --------------------------------------------------------------------------
    # Initialization of server
    # --------------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__()
        self.init_sockets()
        self.create_poller()
        self.topic_dict = {}
        self.client_dict = {}
        self.pending_clients = {}

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
        return list(self.topic_dict[topic].keys())[-1]

    def check_client_subscription(self, client_id: int, topic: str) -> int | None:
        """
        Returns the position to the last message a client received.
        Checks if the client exists and if it is subscribed to the topic
        """
        position = self.client_dict.get(client_id, {}).get(topic)
        return position

    def message_for_client(self, client_id: int, topic: str) -> list:
        """
        Returns the next message that needs to be send to the client,
        in the following format: [client_id, topic, msg_id, msg_content]
        """
        last_message_id = self.client_dict[client_id][topic]
        next_message_id = last_message_id + 1

        # There's no message for this client,
        # it needs to wait for a new message from a publisher
        if next_message_id not in self.topic_dict[topic]:
            return None

        next_message = self.topic_dict[topic][next_message_id]
        return [client_id, topic, next_message_id, next_message]

    def add_topic(self, topic: str) -> None:
        """
        Adds a topic to the topics data structure if it is not in it already
        """
        if topic not in self.topic_dict:
            self.topic_dict[topic] = {}
        if topic not in self.pending_clients:
            self.pending_clients[topic] = []

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

    def add_subscriber(self, client_id: int, topic: str) -> None:
        """
        Adds a client to the topics structure and returns the id created for it
        """
        self.add_topic(topic)
        self.add_client(client_id)
        # The next message this client needs to receive is the next of the topic
        self.client_dict[client_id][topic] = self.last_message_of_topic(topic)

    # --------------------------------------------------------------------------
    # Handling of messages
    # --------------------------------------------------------------------------

    def update_pending_clients(self, topic: str) -> None:
        """
        If there are any pending clients for a topic, goes through the list and sends the last received message
        """
        pending_clients = self.pending_clients[topic]
        if not pending_clients:
            return

        Logger.success(f"    Send message to the waiting subscribers:", end=" ")
        for client_id in pending_clients:
            # Send message to pending client
            message = self.message_for_client(client_id, topic)
            self.router.send_multipart(MessageParser.encode(message))
            Logger.success(client_id, end=" ")

        Logger.success()
        self.pending_clients[topic] = []

    def handle_subscription(self) -> None:
        """
        Reads the message from the frontend socket, forwards it to the
        publishers and adds the new subscription to the data structures
        """
        # Parse the message
        message = self.frontend.recv_multipart()
        Logger.new_message(message)
        sub_type = message[1][0]

        # TODO - find a way to ignore auto sent unsubscribe
        client_id = int(message[0][1:])
        topic = message[1][1:].decode()

        if sub_type == 1:
            Logger.subscription(client_id, topic)
            # Forward to publishers and add to data structure
            self.backend.send_multipart(message)
            self.add_subscriber(client_id, topic)
        elif sub_type == 0:
            Logger.unsubscription(client_id, topic)
            self.backend.send_multipart(message)
            # TODO - unsubscribe client

    def handle_publication(self) -> None:
        """
        Reads the message from the backend socket, creates a new id for it,
        sends it to the subscribers and adds the new message to the data
        structures
        """
        raw_message = self.backend.recv_multipart()
        Logger.new_message(raw_message)

        topic, message = raw_message[0].decode(), raw_message[1].decode()
        # TODO - save original message id to send ack to publisher
        message_id = self.add_message(topic, message)
        Logger.publication(topic, message_id, message)

        self.update_pending_clients(topic)

    def handle_dealer(self) -> None:
        message = MessageParser.decode(self.router.recv_multipart())
        identity = int(message[0])
        message_type = message[1]

        if message_type == "GET":
            self.handle_get(identity, topic = message[2])
        elif message_type == "ACK":
            message_id = int(message[3])
            topic = message[2]
            self.handle_acknowledgement(identity, message_id, topic)
        elif message_type == "CRASH":
            self.handle_crash(identity, message[2:])

    def handle_get(self, client_id: int, topic: str) -> None:
        Logger.request(client_id, topic)

        # Verify if client exists and is subscribed
        if self.check_client_subscription(client_id, topic) is None:
            return 
        # Gets and verifies message
        message = self.message_for_client(client_id, topic)

        if message is None:
            # Adds to the pending clients, as there's no message to be send
            self.pending_clients[topic].append(client_id)
            Logger.warning(f"    Added {client_id} to the waiting list for '{topic}'")
            return
        # Send to client
        self.router.send_multipart(MessageParser.encode(message))
        Logger.success(f"    The message {int(message[2])} was sent to the subscriber")

    def handle_acknowledgement(self, client_id: int, message_id: int, topic: str) -> None:
        Logger.acknowledgement(client_id, topic, message_id)

        if self.check_client_subscription(client_id, topic) is not None:
            self.client_dict[client_id][topic] = message_id
        else:
            Logger.warning(f"    {client_id} is not a subscriber of '{topic}'")
        
        # current_pointer = self.client_dict[client_id][topic]
        # if message_id == (current_pointer + 1):
        #     self.client_dict[client_id][topic] = message_id
        # else:
        #     pass
            # TODO deal with mismatch message id in ACK (if we choose to do this other than re-send when no ACK is received)
    
    def handle_crash(self, identity: int, subscriber_state: List[str]): 
        for i in range(0, len(subscriber_state), 2): 
            topic = subscriber_state[i] 
            last_client = subscriber_state[i+1]

            # Get lost messages.  
            topic_messages = self.topic_dict[topic] 
            for msg_id in topic_messages.keys():
                if int(msg_id) > last_client:   
                    content = self.topic_dict[topic][msg_id]
                    to_send = MessageParser.encode([topic, msg_id, content])  
                    Logger.put_message(to_send)
                    self.router.send_multipart(identity + to_send)

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
