import multiprocessing
import sys
import os
from utils import *
from DKMaster_Alive import DK_Master_Alive
from DK_Rep import DK_Rep

manager = multiprocessing.Manager()
arrFullPaths = manager.list()
arrFullPathsLock = multiprocessing.Lock()

path = './Videos/'
IP = get_ip()

processes = []
# process that will send alive message to master
p = multiprocessing.Process(target=DK_Master_Alive, args=(IP,))
processes.append(p)  # remember it
p.start()  # ...and run!

# DK Processes (dataKeeperNumOfProcesses)
for port in dataKeeperPorts:
    p = multiprocessing.Process(
        target=DK_Rep, args=(port, path, arrFullPaths, IP, arrFullPathsLock))
    processes.append(p)  # remember it
    p.start()  # ...and run!

# Wait for every process to end
for p in processes:
    p.join()
