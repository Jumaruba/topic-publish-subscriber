import zmq
import sys

context = zmq.Context()
socket = context.socket(zmq.XSUB)
socket.connect("tcp://localhost:5558")

zip_filter = sys.argv[1] if len(sys.argv) > 1 else "10001"
# socket.setsockopt_string(zmq.SUBSCRIBE, zip_filter)
socket.send_string('\x01' + zip_filter, zmq.SNDMORE)
socket.send_string(str('\x10'+ str(zmq.ROUTING_ID)))

total_temp = 0
for update_nbr in range(5):
    zipcode = socket.recv_string()
    temperature = socket.recv_string()
    print(zipcode, temperature)
    
