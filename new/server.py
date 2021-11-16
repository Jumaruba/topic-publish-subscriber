import time

import zmq

context = zmq.Context()
socket = context.socket(zmq.ROUTER)
socket.connect("tcp://localhost:5555")


while True:
    message = socket.recv_multipart()
    print("Received", message)

    time.sleep(1)

    socket.send_string("World")
