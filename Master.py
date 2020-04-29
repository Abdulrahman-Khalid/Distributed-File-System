import multiprocessing
import sys
import os
from utils import *
from DataKeeper import DataKeeper
from MasterClient import MasterClient
from MasterDK_Rep import MasterDK_Rep
from MasterDK_Alive import MasterDK_Alive

manager = multiprocessing.Manager()

# Save all the Data Keepers Informaton
# [their IP, their free and busy Ports, isAlive of not ]
dataKeepers = manager.dict()
dataKeepersLock = multiprocessing.Lock()

# Save all files Informaton
# [their name, their Client ID, the Data Keepers that they are in ]
files_metadata = manager.dict()
fileMetaDataLock = multiprocessing.Lock()

ports = {}
# Generate Ports for all data keepers processes
for j in range(30002, 30002 + dataKeeperNumOfProcesses):
    ports[str(j)] = Port(str(j))

for ip in dataKeepersIps:
    dataKeepers[ip] = DataKeeper(ip, ports)


processes = []

# Keep DK alives Process
p = multiprocessing.Process(target=MasterDK_Alive,
                            args=(dataKeepers, dataKeepersLock))
processes.append(p)  
p.start()  

# N-Replicates Process
p = multiprocessing.Process(
    target=MasterDK_Rep, args=(dataKeepers, files_metadata, fileMetaDataLock, dataKeepersLock))
processes.append(p) 
p.start() 

# Client & DK Processes
for x in range(masterNumOfProcesses):
    p = multiprocessing.Process(target=MasterClient, args=(
        dataKeepers, files_metadata, masterPortsArr[x], fileMetaDataLock, dataKeepersLock))
    processes.append(p)  
    p.start() 

# Wait for every process to end
for p in processes:
    p.join()
