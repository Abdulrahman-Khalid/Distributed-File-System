import multiprocessing
import sys
import os
from utils import *
from DataKeeper import DataKeeper
from MasterClient import MasterClient
from MasterDK_Rep import MasterDK_Rep
from MasterDK_Alive import MasterDK_Alive
import threading

manager = multiprocessing.Manager()
# Save all the Data Keepers Informaton
# [their IP, their free and busy Ports, isAlive of not ]
dataKeepers = manager.dict()
# Save all files Informaton
# [their name, their Client ID, the Data Keepers that they are in ]
files_metadata = manager.dict()

ports = {}
# Generate Ports for all data keepers processes
for j in range(30002, 30002 + dataKeeperNumOfProcesses):
    ports[str(j)] = Port(str(j))

for ip in dataKeepersIps:
    dataKeepers[ip] = DataKeeper(ip, ports)


processes = []
# Keep DK alives Process
# launch a process which will increment every value of s_arr

dataKeepersLock = threading.Lock()
p = multiprocessing.Process(target=MasterDK_Alive,
                            args=(dataKeepers, dataKeepersLock))
processes.append(p)  # remember it
p.start()  # ...and run!

# N-Replicates Process
# launch a process which will increment every value of s_arr
fileMetaDataLock = threading.Lock()
p = multiprocessing.Process(
    target=MasterDK_Rep, args=(dataKeepers, files_metadata, fileMetaDataLock, dataKeepersLock))
processes.append(p)  # remember it
p.start()  # ...and run!

# Client & DK Processes
for x in range(masterNumOfProcesses):
    # launch a process which will increment every value of s_arr
    p = multiprocessing.Process(target=MasterClient, args=(
        dataKeepers, files_metadata, masterPortsArr[x], fileMetaDataLock, dataKeepersLock))
    processes.append(p)  # remember it
    p.start()  # ...and run!

# Wait for every process to end
for p in processes:
    p.join()
