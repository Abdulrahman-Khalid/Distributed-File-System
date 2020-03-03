import pprint
import sys
import time
import zmq
from utils import *


def get_instance_count(DKs, dataKeepers):
    Count = 0
    # Check For All Dks that contain this file and available
    for DK_IP in DKs:
        if (dataKeepers[DK_IP].isAlive):
            Count += 1
    return Count


def get_source_Machine(DKs, dataKeepers):
    # Check For All Dks that contain this file and available and has free port
    for DK_IP in DKs:
        if (dataKeepers[DK_IP].isAlive):
            for portNum, port in DK.arrPort.items():
                if(not port.isBusy):
                     # Declare That this port isn't free any more
                    dataKeepers[DK_IP].arrPort[portNum] = Port(portNum ,True)
                    return DK_IP, portNum
    # return None if there isn't any free port 
    # at any available machine that contain that file
    return None, None


def select_machines_to_copy_to(replicationNum, DKs, dataKeepers):
    freeMachinePorts = []
    # Check For All Dks that doesn't contain this file and available and has free port
    for DK_IP, DK in dataKeepers.items():
        if(DK.isAlive and DK_IP not in DKs):
            for portNum, port in value1.arrPort.item():
                if(not port.isBusy):
                     # Declare That this port isn't free any more
                    dataKeepers[DK_IP].arrPort[portNum] = Port(portNum ,True)
                    freeMachinePorts.append((DK_IP, portNum))
                    # I need Number of DST Machines equal to replicationNum
                    if(replicationNum == len(arr)):
                        return freeMachinePorts
    return freeMachinePorts


def MasterDK_Rep(dataKeepers, files_metadata):
    while True:
        # Loop On all Files
        for file in files_metadata.values():
            # Check Number of Available Machines contains that File
            instanceCount = get_instance_count(file.DKs, dataKeepers)
            # Calculate number of Replications Needed
            Replications = replicationFactor - instanceCount
            # Find Free Port on available machine that contain that file
            srcIp, srcPort = get_source_Machine(file.DKs, dataKeepers)
            # Find Number of Dst equal to number of Replications Needed
            freePorts = select_machines_to_copy_to(Replications, file.DKs, dataKeepers)

           
            ########## transfer data from source to destination #########
           
            # Notify SRC
            src_socket, src_context = configure_port(srcIp + ":" + srcPort, zmq.REQ, 'connect')
            srcMessage = {'id': MsgDetails.MASTER_DK_REPLICATE, "type": DataKeeperType.SRC}
            src_socket.send(pickle.dumps(srcMessage))

            # Notify All DST
            for dstIp, dstPort in freePorts:
                dst_socket, dst_context = configure_port(dstIp + ":" + dstPort, zmq.REQ, 'connect')
                dstMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                              "type": DataKeeperType.DST, 'srcIp': srcIp, 'srcPort': srcPort}
                dst_socket.send(pickle.dumps(dstMessage))
                # Recieve OK MSG From DST To Notify That Port is Free Now
                msgFromDK = pickle.loads(dst_socket.recv())
                # Declare That This Dst Port is Free Now
                dataKeepers[dstIp].arrPort[dstPort] = Port(dstPort ,True)
                #Terminate The connection with That Dst
                dst_socket.close()
                dst_context.destroy()

            # Declare That This SRC Port is Free Now
            dataKeepers[srcIp].arrPort[srcPort]= Port(dstPort ,True)
            src_socket.close()
            src_context.destroy()
            
        time.sleep(replicationPeriod)
