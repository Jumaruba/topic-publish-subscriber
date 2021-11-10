import zmq
import sys

from subscription import Subscription
from acknowledgement import Acknowledgement
from message_type import TypeMessage

context = zmq.Context()
socket = context.socket(zmq.XSUB)
socket.connect("tcp://localhost:5558")

zip_filter = sys.argv[1] if len(sys.argv) > 1 else "10001"
# socket.setsockopt_string(zmq.SUBSCRIBE, zip_filter)

# socket.send_pyobj(Subscription(zmq.ROUTING_ID, zip_filter))
socket.send_string('\x01' + zip_filter, zmq.SNDMORE)
socket.send_string(str('\x10'+ str(zmq.ROUTING_ID)))

total_temp = 0
for update_nbr in range(10):
    message = socket.recv_string()
    zipcode, temperature, message_id = message.split()
    # message_id, zipcode, temperature = socket.recv_multipart()
    print(message_id, zipcode, temperature)
    socket.send_pyobj(Acknowledgement(zmq.ROUTING_ID, message_id))
 