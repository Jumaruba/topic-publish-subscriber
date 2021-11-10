import zmq
import sys
from zmq import Frame

context = zmq.Context()
socket = context.socket(zmq.XSUB)
socket.connect("tcp://localhost:5558")

zip_filter = sys.argv[1] if len(sys.argv) > 1 else "10001"
subscribe = bytes('\x01' + zip_filter, 'ascii') 
unsubscribe = bytes('\x10' + str(zmq.ROUTING_ID), 'ascii')

# socket.setsockopt(zmq.SUBSCRIBE, zip_filter)
# socket.send_multipart([('\x01' + zip_filter).encode('ascii'), ('\x10' + str(zmq.ROUTING_ID)).encode('ascii')])
# socket.send_multipart([subscribe, unsubscribe])
socket.send_string('\x01' + zip_filter, zmq.SNDMORE)
socket.send_string('\x10'+ str(zmq.ROUTING_ID))

total_temp = 0
for update_nbr in range(10):
    message = socket.recv_string()
    zipcode, temperature, message_id = message.split()
    #message_id, zipcode, temperature = socket.recv_multipart()
    print(message_id, zipcode, temperature)
    socket.send_string(f'\x11 {message_id}')
