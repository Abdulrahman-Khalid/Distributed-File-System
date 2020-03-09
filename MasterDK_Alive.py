import pprint
import sys
import time
import zmq
import signal
from contextlib import contextmanager
from utils import *
from datetime import datetime
from dateutil.relativedelta import relativedelta


def MasterDK_Alive(dataKeepers, dataKeepersLock):
    # Configure myself as subscriber all data keepers
    subSocket, subContext = configure_multiple_ports(dataKeepersIps,
                                                     dataKeepersAlivePort, zmq.SUB)

    # Dictionary used to keep track the last time
    # that a data Keeper send Alive Msg
    isAliveDict = {}
    # Initialize it with Zeros
    dataKeepersLock.acquire()
    for ip in dataKeepers.keys():
        isAliveDict[ip] = 0
    dataKeepersLock.release()
    while True:
        # Wait for Alive Msg
        receivedMessage = pickle.loads(subSocket.recv())
        # Set time for this data keeper as NOW
        tNow = datetime.now()
        ip = receivedMessage['ip']
        isAliveDict[ip] = tNow
        # Make this data keeper Alive
        dataKeepersLock.acquire()
        dataKeepers[ip] = DataKeeper(ip, dataKeepers[ip].arrPort, True)
        dataKeepersLock.release()

        # Check for all data keepers that doesn't send
        # I'm Alive Msg for more than 2 seconds
        # And Set Them as not alive
        for DK_IP, lastTime in isAliveDict.items():
            if(relativedelta(tNow, lastTime).seconds >= 2):
                dataKeepersLock.acquire()
                dataKeepers[DK_IP] = DataKeeper(
                    DK_IP, dataKeepers[DK_IP].arrPort, False)
                dataKeepersLock.release()
