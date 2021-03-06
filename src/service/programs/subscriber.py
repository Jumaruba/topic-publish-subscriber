from __future__ import annotations

import random
import os
import zmq

from .client import Client
from .log.logger import Logger
from .message.message_parser import MessageParser
from .state.subscriber_state import SubscriberState


class Subscriber(Client):
    # --------------------------------------------------------------------------
    # Attributes
    # --------------------------------------------------------------------------

    data_path: str
    topics: list
    state: SubscriberState

    # --------------------------------------------------------------------------
    # Initialization of subscriber
    # --------------------------------------------------------------------------

    def __init__(self, topics_json: str, client_id: str):
        super().__init__(client_id)

        # State
        current_data_path = os.path.abspath(os.getcwd())
        persistent_data_path = f"/data/subscriber_status_{self.id}.pkl"
        data_path = current_data_path + persistent_data_path
        self.state = SubscriberState.read_state(data_path, topics_json)

        self.create_sockets()

        # Subscribe if the subscriber is new, handle crash otherwise
        if self.state.is_new_subscriber(data_path):
            self.subscribe_topics()
            self.state.save_state()
        else:
            self.handle_crash()
    
    def create_sockets(self) -> None:
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt_string(zmq.IDENTITY, self.id)
        self.dealer.connect("tcp://localhost:5554")
        
        self.sync = self.context.socket(zmq.DEALER)
        self.sync.setsockopt_string(zmq.IDENTITY, self.id)
        self.sync.connect("tcp://localhost:5553")

    # --------------------------------------------------------------------------
    # Subscrition functions
    # --------------------------------------------------------------------------

    def subscribe_topics(self):
        for topic in self.state.topics:
            self.subscribe(topic)
            Logger.subscribe(topic)

    def unsubscribe_topics(self):
        for topic in self.state.topics:
            self.unsubscribe(topic)
            # NOTE if we are doing unsubsribe we should delete the state
            # self.state.topics.remove(topic)
            Logger.unsubscribe(topic)

    def subscribe(self, topic: str) -> None:
        self.dealer.send_multipart(MessageParser.encode(["SUB", topic]))

    def unsubscribe(self, topic: str) -> None:
        self.dealer.send_multipart(MessageParser.encode(["UNSUB", topic]))

    # --------------------------------------------------------------------------
    # Message handling functions
    # --------------------------------------------------------------------------

    def get(self, topic: str) -> None:
        self.state.set_last_get(topic)
        msg_id = self.state.get_next_message(topic)
        self.dealer.send_multipart(MessageParser.encode(['GET', topic, msg_id]))
        Logger.get(self.id, topic)

    def handle_crash(self):
        """ Send ACK to the last topic requested with a GET before crashing.
        Send SYNC message to know if he crashed while waiting for an answer to GET.
        """
        # Send last ACK
        ack_message = self.state.get_last_ack()
        if ack_message is not None:
            self.dealer.send_multipart(MessageParser.encode(ack_message))

        # SYNC with the server
        self.sync_with_server()

    def sync_with_server(self):
        if self.state.last_get is None:
            return

        self.sync.send(self.state.last_get.encode("utf-8"))
        answer = self.sync.recv().decode("utf-8")
        
        if answer == "WAITING":
            # Wait for answer to GET
            self.handle_msg()
            return
        elif answer == "NOT WAITING":
            # Check if a GET response is in the queue
            poller = zmq.Poller()
            poller.register(self.dealer, zmq.POLLIN)
            socks = dict(poller.poll(250))

            for i in range(3):
                if socks.get(self.sync) == zmq.POLLIN:
                    self.handle_msg()
                    return

    def handle_msg(self) -> None:
        """ This function receive a message of a topic and sends the ACK. """

        [topic, msg_id, content] = MessageParser.decode(
            self.dealer.recv_multipart())

        # Duplicated message [extreme case]
        if int(msg_id) < self.state.get_next_message(topic):
            return

        Logger.topic_message(topic, msg_id, content)
        self.state.add_message(topic, int(msg_id))
        self.state.save_state()

        self.dealer.send_multipart(
            MessageParser.encode(['ACK', topic, msg_id]))

    # --------------------------------------------------------------------------
    # Main function of subscriber
    # --------------------------------------------------------------------------

    def run(self):
        
        for i in range(5):
            try:
                # Get random subscribed topic
                topic_idx = random.randint(0, len(self.state.topics) - 1)
                topic = self.state.topics[topic_idx]

                # Get message from a topic
                self.get(topic)

                # Send ACK
                self.handle_msg()

            except KeyboardInterrupt:
                self.state.save_state()
                Logger.err("Keyboard interrupt")
                exit()

        self.unsubscribe_topics()
        self.state.delete()
