import zmq
from zmq.sugar.constants import POLLIN
import codecs

context = zmq.Context()

# For the backend to publish
backend = context.socket(zmq.XSUB)
backend.bind("tcp://*:5557")

# For the frontend to subscribe
frontend = context.socket(zmq.XPUB)
frontend.bind("tcp://*:5558")

# Polling 
poller = zmq.Poller()
poller.register(backend, zmq.POLLIN)
poller.register(frontend, zmq.POLLIN)

while True:
    socks = dict(poller.poll())

    # Forward published messages to the frontend
    if socks.get(backend) == zmq.POLLIN:
        msg = backend.recv_string()
        print("[BACK]", msg)
        zipcode, temp, identity = msg.split()
        # print("[BROKER] A publisher sent the message: %r" % msg[1])
        frontend.send_string(f"{zipcode} {temp}")

    if socks.get(frontend) == zmq.POLLIN:
        msg = frontend.recv_multipart()
        msg_id = frontend.recv_multipart()
        print("[FRONT]", msg, msg_id)
        backend.send_multipart(msg)
        # print("[BROKER] A subscriber sent the message: %r" % msg)