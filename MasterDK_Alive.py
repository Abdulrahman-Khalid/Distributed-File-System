import pprint
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *


def MasterDK_Alive(dataKeepers):
    # Configure myself as subscriber all data keepers
    subSocket, subContext = configure_multiple_ports(dataKeepersIps, 
                                dataKeepersAlivePort, zmq.SUB, True)
    while(True):
        try:
            while True:
                receivedMessage = pickle.loads(subSocket.recv())
                ip = receivedMessage['ip']
                dataKeepers[ip] = DataKeeper(ip, dataKeepers[ip].arrPort, True)
        except:
            pass
        finally: 
            for DK_IP in dataKeepers.keys():
                dataKeepers[DK_IP] = DataKeeper(DK_IP, dataKeepers[DK_IP].arrPort, False)

