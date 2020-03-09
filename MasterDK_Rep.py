import pprint
import sys
import time
import zmq
from utils import *
from FileDetails import FileDetails


def get_instance_count(DKs, dataKeepers):
    Count = 0
    # Check For All Dks that contain this file and available
    for DK_IP in DKs:
        if (dataKeepers[DK_IP].isAlive):
            Count += 1
    return Count


def get_source_Machine(DKs, dataKeepers, dataKeepersLock):
    # Check For All Dks that contain this file and available and has free port
    for DK_IP in DKs:
        if (dataKeepers[DK_IP].isAlive):
            for portNum, port in dataKeepers[DK_IP].arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    dataKeepersLock.acquire()
                    dataKeepers[DK_IP].arrPort[portNum] = Port(portNum, True)
                    dataKeepersLock.release()
                    return DK_IP, portNum
    # return None if there isn't any free port
    # at any available machine that contain that file
    return None, None


def select_machines_to_copy_to(replicationNum, DKs, dataKeepers, dataKeepersLock):
    freeMachinePorts = []
    # Check For All Dks that doesn't contain this file and available and has free port
    for DK_IP, DK in dataKeepers.items():
        if(DK.isAlive and (DK_IP not in DKs)):
            for portNum, port in DK.arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    modifiedArrPorts = dataKeepers[DK_IP].arrPort.copy()
                    modifiedArrPorts[portNum].isBusy = True
                    dataKeepersLock.acquire()
                    dataKeepers[DK_IP] = DataKeeper(DK_IP, modifiedArrPorts,
                                                    dataKeepers[DK_IP].isAlive)
                    dataKeepersLock.release()
                    # Append it to the List of Free Ports
                    freeMachinePorts.append((DK_IP, portNum))
                    # I need Number of DST Machines equal to replicationNum
                    if(replicationNum == len(freeMachinePorts)):
                        return freeMachinePorts
    return freeMachinePorts


def notify_src(srcIP, srcPort, fileName):
    src_socket, src_context = configure_port(
        srcIP + ":" + srcPort, zmq.REQ, 'connect')
    srcMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                  "type": DataKeeperType.SRC, "fileName": fileName}
    src_socket.send(pickle.dumps(srcMessage))
    return src_socket, src_context


def notify_Dsts(srcIP, srcPort, freePorts):
    dstSockets = []
    dstContexts = []

    # DST MSG
    dstMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                  "type": DataKeeperType.DST, 'srcIp': srcIP, 'srcPort': srcPort}

    for dstIp, dstPort in freePorts:
        dst_socket, dst_context = configure_port(
            dstIp + ":" + dstPort, zmq.REQ, 'connect')
        dstSockets.append(dst_socket)
        dstContexts.append(dst_context)
        dst_socket.send(pickle.dumps(dstMessage))

    return dstSockets, dstContexts


def get_Dsts_response(freePorts, dstSockets, dstContexts, dataKeepers, files_metadata, fileName, dataKeepersLock):
    idx = 0
    for dstIp, dstPort in freePorts:
        # Recieve OK MSG From DST To Notify That Port is Free Now
        msgFromDK = pickle.loads(dstSockets[idx].recv())
        # Declare That This Dst Port is Free Now
        modifiedArrPorts = dataKeepers[dstIp].arrPort.copy()
        modifiedArrPorts[dstPort].isBusy = False
        dataKeepersLock.acquire()
        dataKeepers[dstIp] = DataKeeper(dstIp, modifiedArrPorts,
                                        dataKeepers[dstIp].isAlive)
        dataKeepersLock.release()
        # Add This Data keeper to the File Data Keepers
        NewDKs = files_metadata[fileName].DKs.copy()
        NewDKs.append(dstIp)
        files_metadata[fileName] = FileDetails(fileName,
                                               files_metadata[fileName].clientId, NewDKs)
        # Terminate The connection with That Dst
        dstSockets[idx].close()
        dstContexts[idx].destroy()
        idx += 1


def get_Src_response(src_socket, src_context, srcIP, srcPort, dataKeepers, dataKeepersLock):
    modifiedArrPorts = dataKeepers[srcIP].arrPort.copy()
    modifiedArrPorts[srcPort].isBusy = False
    dataKeepersLock.acquire()
    dataKeepers[srcIP] = DataKeeper(srcIP, modifiedArrPorts,
                                    dataKeepers[srcIP].isAlive)
    dataKeepersLock.release()
    src_socket.close()
    src_context.destroy()

############## Main Funciton ##############


def MasterDK_Rep(dataKeepers, files_metadata, dataKeepersLock):
    while True:
        # Loop On all Files
        for file in files_metadata.values():
            # Check Number of Available Machines contains that File
            instanceCount = get_instance_count(file.DKs, dataKeepers)
            # Calculate number of Replications Needed
            Replications = replicationFactor - instanceCount
            # Find Free Port on available machine that contain that file
            srcIp, srcPort = get_source_Machine(
                file.DKs, dataKeepers, dataKeepersLock)
            # Find Number of Dst equal to number of Replications Needed
            freePorts = select_machines_to_copy_to(
                Replications, file.DKs, dataKeepers, dataKeepersLock)
            if(len(freePorts) > 0):
                ########## transfer data from source to destination #########
                # Notify Source
                src_socket, src_context = notify_src(
                    srcIp, srcPort, file.fileName)
                # Notify All Destinations
                dstSockets, dstContexts = notify_Dsts(
                    srcIp, srcPort, freePorts)
                # Get Destanitions Response, Declare Them as Free Ports & Terminate Their Connection
                # Add Them To The File Data Keepers
                get_Dsts_response(freePorts, dstSockets, dstContexts,
                                  dataKeepers, files_metadata, file.fileName, dataKeepersLock)
                # Get Src Response, Declare it as Free Ports & Terminate its Connection
                get_Src_response(src_socket, src_context,
                                 srcIp, srcPort, dataKeepers, dataKeepersLock)

        time.sleep(replicationPeriod)
