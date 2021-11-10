import zmq
from zmq.sugar.constants import POLLIN
import codecs
from message_type import TypeMessage 


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
    message_id = 0
    if socks.get(backend) == zmq.POLLIN:
        msg = backend.recv_string()
        print("[BACK]", msg)
        zipcode, temp, identity = msg.split()
        # print("[BROKER] A publisher sent the message: %r" % msg[1])
        message_id += 1
        frontend.send_string(f"{zipcode} {temp} {message_id}")

    if socks.get(frontend) == zmq.POLLIN:
        #total = frontend.recv()
        #total = [frontend.recv_# string()]
        # while frontend.getsoc# kopt(zmq.RCVMORE):
        #     total.append(fronte#nd.recv_string())

        #print(len(total))
        #total = frontend.recv()
        #print(total)
       
        msg = frontend.recv_multipart()
        msg_type = msg[0][:1]

        if msg_type == TypeMessage.ID:
            # ID
            print("[ID]", msg)
        elif msg_type == TypeMessage.ACK:
            # ACK
            print("[ACK]", msg) 
        else:
            # Unsubscribe or Subscribe
            backend.send_multipart(msg)
            print("[SUB/UNSUB]", msg)
                