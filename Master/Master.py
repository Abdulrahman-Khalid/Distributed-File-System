import multiprocessing
import sys
import os

dataKeeprs = {}
dataKeeprs = multiprocessing.RawArray('d', dataKeeprs)
files_metadata = []
files_metadata = multiprocessing.RawArray('d', files_metadata)

if os.path.exists("DKs_metadata.txt"):
    with open("DKs_metadata.txt", 'r') as inputFile:
        try:
            Inputs = inputFile.readlines()
            for DK in Inputs:
                lineData = Dk.split()
                dkalivePort = lineData[3]
                dkPortsArr = range(dkalivePort + 1, dkalivePort + utils.dataKeeperNumOfProcesses)
                dataKeeprs.append({lineData[1] : DataKeeper(lineData[1], dkalivePort, dkPortsArr, False)})
        except (OSError, IOError) as e:
            print("Error in reading DK metadata file")
            exit()

processes = []
# Keep DK alives Process
p = multiprocessing.Process(target = increment, args=(dataKeeprs, files_metadata,)) # launch a process which will increment every value of s_arr
processes.append(p) # remember it
p.start() #...and run!

# N-Replicates Process
p = multiprocessing.Process(target = increment, args=(dataKeeprs, files_metadata,)) # launch a process which will increment every value of s_arr
processes.append(p) # remember it
p.start() #...and run!

# Client & DK Processes
for x in range(utils.masterNumOfProcesses):
    p = multiprocessing.Process(target = increment, args=(dataKeeprs,files_metadata,)) # launch a process which will increment every value of s_arr
    processes.append(p) # remember it
    p.start() #...and run!

# Wait for every process to end
for p in processes:
    p.join()

