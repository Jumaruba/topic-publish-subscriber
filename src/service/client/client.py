import zmq

class Client:
    def __init__(self) -> None:
        self.context = zmq.Context()
        self.sockets = {}

    def create_socket(self, name: str, type: zmq.TYPE, address: str) -> None:
        new_socket = self.context.socket(type)
        new_socket.connect("tcp://" + address)
        self.sockets[name] = new_socket
        return self.sockets[name]