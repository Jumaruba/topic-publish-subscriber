from __future__ import annotations

import zmq

from zmq import backend
from zmq.sugar.socket import Socket

from .program import SocketCreationFunction
from .program import Program
from .log.logger import Logger
from .excpt.create_socket import CreateSocket
from .message.message_parser import MessageParser
from .state.server_state import ServerState
from typing import List
import threading 
import os

class Server(Program):

    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    # Sockets
    poller: zmq.Poller
    backend: zmq.Socket
    router: zmq.Socket
    state: ServerState
    ack_server: zmq.Socket 


    # --------------------------------------------------------------------------
    # Initialization of server
    # --------------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__()
        self.init_sockets()
        self.create_poller()
        # How many messages must be received to the state be saved. 
        self.save_frequency = 5
        # State  

        current_data_path = os.path.abspath(os.getcwd())   
        persistent_data_path = f"/data/server_status.pkl" 
        data_path = current_data_path + persistent_data_path
        self.state = ServerState.read_state(data_path)

    def create_poller(self) -> None:
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.router, zmq.POLLIN)

    def init_sockets(self) -> None:
        self.backend = self.create_socket(
            zmq.XSUB, SocketCreationFunction.BIND, '*:5556')

        self.router = self.create_socket(
            zmq.ROUTER, SocketCreationFunction.BIND, '*:5554')
        self.ack_server = self.create_socket(
            zmq.PUB, SocketCreationFunction.BIND, '*:5552')

    # --------------------------------------------------------------------------
    #  Handling of messages
    # --------------------------------------------------------------------------
    def update_pending_clients(self, topic: str) -> None:
        """
        If there are any pending clients for a topic, goes through the list and sends the last received message
        """
        pending_clients = self.state.get_waiting_list(topic)
        if not pending_clients:
            return

        Logger.success(f"    Send message to the waiting subscribers:", end=" ")
        for client_id in pending_clients:
            # Send message to pending client
            message = self.state.message_for_client(client_id, topic)
            self.router.send_multipart(MessageParser.encode(message))
            Logger.success(client_id, end=" ")

        Logger.success()
        self.state.empty_waiting_list(topic)

    def handle_publication(self) -> None:
        """
        Reads the message from the backend socket, creates a new id for it,
        sends it to the subscribers and adds the new message to the data
        structures
        """
        raw_message = self.backend.recv_multipart()
        Logger.new_message(raw_message)

        topic, pub_id, message, msg_id = MessageParser.decode(raw_message)
        # Send the ACK to the server
        self.ack_server.send_multipart(MessageParser.encode([pub_id, msg_id, topic])) 

        # TODO - save original message id to send ack to publisher
        message_id = self.state.add_message(topic, message)
        Logger.publication(topic, message_id, message)

        self.update_pending_clients(topic)

    def handle_dealer(self) -> None:
        message = MessageParser.decode(self.router.recv_multipart())
        identity = int(message[0])
        message_type = message[1]
        topic = message[2]

        if message_type == "GET":  
            self.handle_get(identity, topic)
        elif message_type == "ACK": 
            message_id = int(message[3])
            self.handle_acknowledgement(identity, message_id, topic)
        elif message_type == "SUB":         # Testing router/dealer for subscription (missing unsubscription)
            Logger.subscription(identity, topic)
            # Forward to publishers and add to data structure
            subscribe_msg = b'\x01' + topic.encode('utf-8')
            self.backend.send(subscribe_msg)
            self.state.add_subscriber(identity, topic)
        elif message_type == "UNSUB": 
            Logger.unsubscription(identity, topic)
            unsubscribe_msg = b'\x00' + topic.encode('utf-8')
            self.backend.send(unsubscribe_msg)
            self.state.remove_subscriber(identity, topic)

    def handle_get(self, client_id: int, topic: str) -> None:
        Logger.request(client_id, topic)

        # Verify if client exists and is subscribed
        if self.state.check_client_subscription(client_id, topic) is None:
            # TODO - Send error message?
            return
        # Gets and verifies message
        message = self.state.message_for_client(client_id, topic)

        if message is None:
            # Adds to the pending clients, as there's no message to be send
            self.state.add_to_waiting_list(client_id, topic)
            Logger.warning(f"    Added {client_id} to the waiting list for '{topic}'")
            return
        # Send to client
        self.router.send_multipart(MessageParser.encode(message))
        Logger.success(f"    The message {int(message[2])} was sent to the subscriber")

    def handle_acknowledgement(self, client_id: int, message_id: int, topic: str) -> None:
        Logger.acknowledgement(client_id, topic, message_id)

        if self.state.check_client_subscription(client_id, topic) is not None:
            self.state.update_client_last_message(client_id, topic, message_id)
        else:
            Logger.warning(f"    {client_id} is not a subscriber of '{topic}'")

    # --------------------------------------------------------------------------
    # Main function of server
    # --------------------------------------------------------------------------

    def run(self) -> None:
        """
        Runs the server, which includes handling subscriptions, publications,
        acknowledgements and error treatment
        """  
        # When this number achieves to 0, it saves the state in the file. It's decremented for each ACK.
        self.msg_counter = self.save_frequency 
        while True:
            socks = dict(self.poller.poll())

            # Receives content from publishers
            if socks.get(self.backend) == zmq.POLLIN:
                self.handle_publication()

            # Receives message from subscribers
            if socks.get(self.router) == zmq.POLLIN:
                self.handle_dealer()
                #print(self.state)

            # Saves the state
            if self.msg_counter == 0:
                self.msg_counter = self.save_frequency
                t = threading.Thread(target=self.state.save_state)
                t.start()

            self.msg_counter -= 1   