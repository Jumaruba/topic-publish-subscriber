from __future__ import annotations

import os
import zmq

from .log.logger import Logger
from .message.message_parser import MessageParser
from .program import Program
from .program import SocketCreationFunction
from .state.server_state import ServerState


class Server(Program):
    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    # Sockets
    poller: zmq.Poller
    backend: zmq.Socket
    fault_pub: zmq.Socket
    router: zmq.Socket
    sync_sub:zmq.Socket
    state: ServerState
    msg_counter: int

    # --------------------------------------------------------------------------
    # Initialization of server
    # --------------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__()
        self.init_sockets()
        self.create_poller()

        # How many messages must be received to the state be saved.
        self.save_frequency = 5
        # When this number achieves to 0, it saves the state in the file. It's decremented for each ACK.
        self.msg_counter = self.save_frequency

        # State
        current_data_path = os.path.abspath(os.getcwd())
        persistent_data_path = f"/data/server_status.pkl"
        data_path = current_data_path + persistent_data_path
        self.state = ServerState.read_state(data_path)

    def init_sockets(self) -> None:
        self.backend = self.create_socket(zmq.XSUB, SocketCreationFunction.BIND, '*:5556')
        self.router = self.create_socket(zmq.ROUTER, SocketCreationFunction.BIND, '*:5554')
        self.fault_pub = self.create_socket(zmq.PUB, SocketCreationFunction.BIND, '*:5552')
        self.sync_sub = self.create_socket(zmq.ROUTER, SocketCreationFunction.BIND, '*:5553')

    def create_poller(self) -> None:
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.router, zmq.POLLIN)
        self.poller.register(self.sync_sub, zmq.POLLIN)

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

        Logger.success(f"      Send message to the waiting subscribers:", end=" ")
        for client_id in pending_clients:
            # Send message to pending client
            message = self.state.message_for_client(client_id, topic)
            self.router.send_multipart(MessageParser.encode(message))
            Logger.success(client_id, end=" ")

        Logger.success()
        self.state.empty_waiting_list(topic)

    def handle_pub_fault(self, pub_id: int, topic: str, pub_msg_id: int) -> bool:
        """
        Return true if not duplicated (if the message is to be resend to the subscriber).
        Send Fault Message to publishers if a message is missing.
        """
        pub_topic_state = self.state.get_publish_dict(pub_id, topic)

        # Check if the message is in the waiting list and remove if in waiting list.
        if pub_topic_state.is_waiting(pub_msg_id):
            # Remove from waiting list
            pub_topic_state.remove_waiting(pub_msg_id)
            return True
        # Checks if duplicated.
        elif pub_msg_id <= pub_topic_state.last_msg:
            return False

        if pub_msg_id - pub_topic_state.last_msg > 1:
            for lost_msg_id in range(pub_topic_state.last_msg + 1, pub_msg_id):
                # Add to waiting set
                pub_topic_state.add_waiting(lost_msg_id)
                # Send fo message to the publisher.
                self.fault_pub.send_multipart(MessageParser.encode([pub_id, topic, lost_msg_id]))

        # Update last message received from the publisher on the topic
        pub_topic_state.last_msg = pub_msg_id
        return True

    def handle_publication(self) -> None:
        """
        Reads the message from the backend socket, creates a new id for it,
        and adds the new message to the data structures
        """
        raw_message = self.backend.recv_multipart()
        Logger.new_message(raw_message)

        topic, pub_id, message, pub_msg_id = MessageParser.decode(raw_message)

        # Handle missing/duplicate publications
        fault_message = self.handle_pub_fault(int(pub_id), topic, int(pub_msg_id))

        if fault_message:
            message_id = self.state.add_message(topic, message)
            Logger.publication(topic, message_id, message)

        self.update_pending_clients(topic)

    def handle_acknowledgement(self, client_id: int, message_id: int, topic: str) -> None:
        Logger.acknowledgement(client_id, topic, message_id)

        if self.state.check_client_subscription(client_id, topic) is not None:
            self.state.update_client_last_message(client_id, topic, message_id)
        else:
            Logger.warning(f"      {client_id} is not a subscriber of '{topic}'")

    def handle_get(self, client_id: int, topic: str, msg_id: int) -> None:
        Logger.request(client_id, topic)

        # Verify if client exists and is subscribed
        if self.state.check_client_subscription(client_id, topic) is None:
            # TODO - Send error message?
            return
        # Gets and verifies message
        message = self.state.message_for_client(client_id, topic, msg_id)

        if message is None:
            # Adds to the pending clients, as there's no message to be send
            self.state.add_to_waiting_list(client_id, topic)
            Logger.warning(f"      Added {client_id} to the waiting list for '{topic}'")
            return

        self.router.send_multipart(MessageParser.encode(message))
        Logger.success(f"      The message {int(message[2])} was sent to the subscriber")

    def handle_subscription(self, client_id: int, topic: str) -> None:
        Logger.subscription(client_id, topic)
        # Forward to publishers and add to data structure
        subscribe_msg = b'\x01' + topic.encode('utf-8')
        self.backend.send(subscribe_msg)
        self.state.add_subscriber(client_id, topic)

    def handle_unsubscription(self, client_id: int, topic: str) -> None:
        Logger.unsubscription(client_id, topic)
        unsubscribe_msg = b'\x00' + topic.encode('utf-8')
        self.backend.send(unsubscribe_msg)
        self.state.remove_subscriber(client_id, topic)

    def handle_sub_sync(self) -> None:
        message = MessageParser.decode(self.sync_sub.recv_multipart())
        client_id = int(message[0])
        topic = message[1]

        if self.state.is_sub_waiting(client_id, topic):
            Logger.sync(client_id, topic, True)
            self.sync_sub.send_multipart(MessageParser.encode([client_id, "WAITING"]))
        else:
            Logger.sync(client_id, topic, False)
            self.sync_sub.send_multipart(MessageParser.encode([client_id, "NOT WAITING"]))

    def handle_dealer(self) -> None:
        message = MessageParser.decode(self.router.recv_multipart())

        # Message parsing
        client_id = int(message[0])
        message_type = message[1]
        topic = message[2]
        if len(message) >= 4:
            message_id = int(message[3])

        if message_type == "ACK":
            self.handle_acknowledgement(client_id, message_id, topic)
        elif message_type == "GET":
            self.handle_get(client_id, topic, message_id)
        elif message_type == "SUB":
            self.handle_subscription(client_id, topic)
        elif message_type == "UNSUB":
            self.handle_unsubscription(client_id, topic)

    # --------------------------------------------------------------------------
    # Main function of server
    # --------------------------------------------------------------------------

    def run(self) -> None:
        """
        Runs the server, which includes handling subscriptions, publications,
        acknowledgements and error treatment
        """
        while True:
            try:
                socks = dict(self.poller.poll())

                # Receives content from publishers
                if socks.get(self.backend) == zmq.POLLIN:
                    self.handle_publication()

                # Receives message from subscribers
                if socks.get(self.router) == zmq.POLLIN:
                    self.handle_dealer()

                # Receives synchronization requests from subscribers
                if socks.get(self.sync_sub) == zmq.POLLIN:
                    self.handle_sub_sync()

                # Saves the state
                if self.msg_counter == 0:
                    self.msg_counter = self.save_frequency
                    self.state.save_state()
                    # TODO: use locks and save state asynchronously
                    # t = threading.Thread(target=self.state.save_state)
                    # t.start()

                self.msg_counter -= 1
            except KeyboardInterrupt:
                self.state.save_state()
                Logger.err("Keyboard interrupt")
                exit()
