import multiprocessing
import sys
import os
from utils import *
from DKMaster_Alive import DK_Master_Alive
from DK_Rep import DK_Rep

manager = multiprocessing.Manager()
arrFullPaths = manager.list()

processes = []

paths = ['./Videos1/', './Videos2/']
#IP = get_ip()

for idx, DK_IP in enumerate(dataKeepersIps):
    # process that will send alive message to master
    p = multiprocessing.Process(target = DK_Master_Alive, args = (DK_IP,))
    processes.append(p)  # remember it
    p.start()  # ...and run!


    # DK Processes (dataKeeperNumOfProcesses)
    for port in dataKeeperPorts:
        # launch a process which will increment every value of s_arr
        p = multiprocessing.Process(
            target= DK_Rep, args=(port, paths[idx], arrFullPaths, DK_IP,))
        processes.append(p)  # remember it
        p.start()  # ...and run!

# Wait for every process to end
for p in processes:
    p.join()
