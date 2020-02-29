import zmq
import time
import sys

port = "5556"
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)

sleep = 1
if len(sys.argv) > 2:
    sleep = int(sys.argv[2])

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

while True:
    #  Wait for next request from client
    message = socket.recv()
    print("Received request: ", message)
    time.sleep(sleep)
    socket.send_string("World from %s" % port)
