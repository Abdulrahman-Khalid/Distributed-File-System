import pprint
import sys
import time
import zmq
from utils import *
import pickle
import time


def DK_Master_Alive(myIp):
    # Configure myself as publisher with with master
    ipPort = myIp + ":" + dataKeepersAlivePort
    pubSocket, pubContext = configure_port(ipPort, zmq.PUB, 'bind')
    # I'm Alive Msg that will be sent periodically
    msg = {'id': MsgDetails.DK_MASTER_ALIVE, 'msg': "I'm Alive", 'ip': myIp}

    # Periodically 1 sec
    while (True):
        pubSocket.send(pickle.dumps(msg))
        time.sleep(1)
