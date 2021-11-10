import zmq
from random import randrange

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://localhost:5557")

i = 0
while True:
    zipcode = randrange(1, 100000)
    temperature = randrange(-80, 135)
    socket.send_string(f"{zipcode} {temperature} {zmq.ROUTING_ID}")
    i += 1
