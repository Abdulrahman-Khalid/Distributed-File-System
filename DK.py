import multiprocessing
import sys
import os
from utils import *
from DKMaster_Alive import DK_MASTER_ALIVE
from DK_Rep import DK_Rep

manager = multiprocessing.Manager()
arrFullPaths = manager.list()
#arrFullPaths = multiprocessing.RawArray('d', arrFullPaths)

processes = []
# send alive message to master
p = multiprocessing.Process(target = DK_MASTER_ALIVE, args=()) # launch a process which will increment every value of s_arr
processes.append(p) # remember it
p.start() #...and run!

# DK Processes (dataKeeperNumOfProcesses)
for port in dataKeeperPorts:
    p = multiprocessing.Process(target = DK_Rep, args=(port, './Videos/', arrFullPaths)) # launch a process which will increment every value of s_arr
    processes.append(p) # remember it
    p.start() #...and run!

# Wait for every process to end
for p in processes:
    p.join()

