import pprint
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *


def MasterDK_Alive(dataKeepers):
    # Configure myself as subscriber all data keepers
    subSocket, subContext = configure_subscriber_port(True)
    while(True):
        try:
            while True:
                receivedMessage = pickle.loads(subSocket.recv())
                ip = receivedMessage['ip']
                dataKeepers[ip] = DataKeeper(ip, dataKeepers[ip].arrPort, True)
        except:
            pass
        finally:
            for key in dataKeepers.keys():
                dataKeepers[key].isAlive = False
        # MasterDK_Alive(dataKeepers)
