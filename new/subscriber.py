import zmq

context = zmq.Context()
socket = context.socket(zmq.DEALER)
socket.bind("tcp://*:5555")

for _ in range(10):
    socket.send_string("Hello")
    message = socket.recv_string()
    print("Received", message)
