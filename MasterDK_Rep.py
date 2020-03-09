import pprint
import sys
import time
import zmq
from utils import *
from FileDetails import FileDetails


def get_instance_count(DKs, dataKeepers, dataKeepersLock):
    Count = 0
    # Check For All Dks that contain this file and available
    dataKeepersLock.acquire()
    for DK_IP in DKs:
        if (dataKeepers[DK_IP].isAlive):
            Count += 1
    dataKeepersLock.release()
    return Count


def get_source_Machine(DKs, dataKeepers, dataKeepersLock):
    # Check For All Dks that contain this file and available and has free port
    dataKeepersLock.acquire()
    for DK_IP in DKs:
        if (dataKeepers[DK_IP].isAlive):
            for portNum, port in dataKeepers[DK_IP].arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    dataKeepers[DK_IP].arrPort[portNum] = Port(portNum, True)
                    dataKeepersLock.release()
                    return DK_IP, portNum
    dataKeepersLock.release()
    # return None if there isn't any free port
    # at any available machine that contain that file
    # [it must not happen as we already check for the instance count
    # and it was greater than one ]
    return None, None


def select_machines_to_copy_to(replicationNum, DKs, dataKeepers, dataKeepersLock):
    freeMachinePorts = []
    # Check For All Dks that doesn't contain this file and available and has free port
    dataKeepersLock.acquire()
    for DK_IP, DK in dataKeepers.items():
        if(DK.isAlive and (DK_IP not in DKs)):
            for portNum, port in DK.arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    modifiedArrPorts = dataKeepers[DK_IP].arrPort.copy()
                    modifiedArrPorts[portNum].isBusy = True
                    dataKeepers[DK_IP] = DataKeeper(DK_IP, modifiedArrPorts,
                                                    dataKeepers[DK_IP].isAlive)
                    # Append it to the List of Free Ports
                    freeMachinePorts.append((DK_IP, portNum))
                    # I need Number of DST Machines equal to replicationNum
                    if(replicationNum == len(freeMachinePorts)):
                        dataKeepersLock.release()
                        return freeMachinePorts
    dataKeepersLock.release()
    return freeMachinePorts


def notify_DKs(srcIP, srcPort, freePorts, fileName, dataKeepers, files_metadata, fileMetaDataLock, dataKeepersLock):
    # DST MSG
    dstMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                  "type": DataKeeperType.DST, 'srcIp': srcIP, 'srcPort': srcPort}
    # SRC MSG
    srcMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                  "type": DataKeeperType.SRC, "fileName": fileName}

    # Connect to SRC
    src_socket, src_context = configure_port(
        srcIP + ":" + srcPort, zmq.REQ, 'connect')

    for dstIp, dstPort in freePorts:
        # Connect to DST
        dst_socket, dst_context = configure_port(
            dstIp + ":" + dstPort, zmq.REQ, 'connect')
        # Notify SRC
        src_socket.send(pickle.dumps(srcMessage))
        # Notify DST
        dst_socket.send(pickle.dumps(dstMessage))
        # Get SRC Response
        msgFromDK = pickle.loads(src_socket.recv())
        # Get DST Response
        msgFromDK = pickle.loads(dst_socket.recv())
        # Declare That This Dst Port is Free Now
        dataKeepersLock.acquire()
        modifiedArrPorts = dataKeepers[dstIp].arrPort.copy()
        modifiedArrPorts[dstPort].isBusy = False
        dataKeepers[dstIp] = DataKeeper(dstIp, modifiedArrPorts,
                                        dataKeepers[dstIp].isAlive)
        dataKeepersLock.release()
        # Add This Data keeper to the File Data Keepers
        fileMetaDataLock.acquire()
        NewDKs = files_metadata[fileName].DKs.copy()
        NewDKs.append(dstIp)
        files_metadata[fileName] = FileDetails(fileName,
                                               files_metadata[fileName].clientId, NewDKs)
        fileMetaDataLock.release()
        # Terminate The connection with That Dst
        dst_socket.close()
        dst_context.destroy()

    # Declare That This SRC Port is Free Now
    dataKeepersLock.acquire()
    modifiedArrPorts = dataKeepers[srcIP].arrPort.copy()
    modifiedArrPorts[srcPort].isBusy = False
    dataKeepersLock.acquire()
    dataKeepers[srcIP] = DataKeeper(srcIP, modifiedArrPorts,
                                    dataKeepers[srcIP].isAlive)
    dataKeepersLock.release()
    # Terminate The connection with That Src
    src_socket.close()
    src_context.destroy()

############## Main Funciton ##############


def MasterDK_Rep(dataKeepers, files_metadata, fileMetaDataLock, dataKeepersLock):
    while True:
        # Loop On all Files
        # TODO NOTICE not using fileMetaDataLock in this area as it doesn't make sense
        for file in files_metadata.values():
            # Check Number of Available Machines contains that File
            instanceCount = get_instance_count(
                file.DKs, dataKeepers, dataKeepersLock)
            # Calculate number of Replications Needed
            Replications = replicationFactor - instanceCount
            if(Replications > 0):
                # Find Free Port on available machine that contain that file
                srcIp, srcPort = get_source_Machine(
                    file.DKs, dataKeepers, dataKeepersLock)
                # Find Number of Dst equal to number of Replications Needed
                freePorts = select_machines_to_copy_to(
                    Replications, file.DKs, dataKeepers, dataKeepersLock)
                # transfer data from source to all destinations
                notify_DKs(srcIp, srcPort, freePorts, file.fileName,
                           dataKeepers, files_metadata, fileMetaDataLock, dataKeepersLock)

        time.sleep(replicationPeriod)
