from utils import *
import zmq
import pickle
from FileDetails import FileDetails
from DataKeeper import DataKeeper
from Port import Port


def send_download_data(files_metadata, dataKeepers, recievedMsg, portNum, Socket, fileMetaDataLock, dataKeepersLock):
    fileName = recievedMsg["fileName"]
    # This file isn't being uploaded before

    if fileName not in files_metadata:
        sentMsg = {"id": MsgDetails.FAIL, "Msg": "File isn't exist."}
        Socket.send(pickle.dumps(sentMsg))
        return

    file_metadata = files_metadata[fileName]
    freePortFound = False  # Dummy Variable Used To Terminate The Outer Loop
    # For All Data Keepers that contain that file
    
    for DK_IP in file_metadata.DKs:
        # Check if This Data Keeper is available
        if(dataKeepers[DK_IP].isAlive):
            # Loop on all this Data Keeper Ports to Check if one of them is free
            for portNum, port in dataKeepers[DK_IP].arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    modifiedArrPorts = dataKeepers[DK_IP].arrPort.copy()
                    modifiedArrPorts[portNum].isBusy = True
                    dataKeepersLock.acquire()
                    dataKeepers[DK_IP] = DataKeeper(DK_IP, modifiedArrPorts,
                                                    dataKeepers[DK_IP].isAlive)
                    dataKeepersLock.release()
                    # Tell The Master The Ip and Port of This available Machine
                    sentMsg = {
                        "id": MsgDetails.MASTER_CLIENT_DOWNLOAD_DETAILS,
                        "ip": DK_IP, "port": portNum}
                    Socket.send(pickle.dumps(sentMsg))
                    freePortFound = True
                    break
        if(freePortFound):
            break

    # All Ports are Busy or all Machines that contain that file are down
    if(not freePortFound):
        sentMsg = {"id": MsgDetails.FAIL,
                   "Msg": "All ports are Busy, Try again Later."}
        Socket.send(pickle.dumps(sentMsg))


def send_upload_data(dataKeepers, Socket, dataKeepersLock):
    freePortFound = False   # Dummy Variable Used To Terminate The Outer Loop
    # For All Available Data Keepers
    # TODO check on Size
    
    for DK_IP, DK in dataKeepers.items():
        if(DK.isAlive):
            # Loop on all this Data Keeper Ports to Check if one of them is free
            for portNum, port in DK.arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    modifiedArrPorts = dataKeepers[DK_IP].arrPort.copy()
                    modifiedArrPorts[portNum].isBusy = True
                    dataKeepersLock.acquire()
                    dataKeepers[DK_IP] = DataKeeper(DK_IP, modifiedArrPorts,
                                                    dataKeepers[DK_IP].isAlive)
                    dataKeepersLock.release()
                    # Tell The Master The Ip and Port of This available Machine
                    sentMsg = {
                        "id": MsgDetails.MASTER_CLIENT_UPLOAD_DETAILS,
                        "ip": DK_IP, "port": portNum}
                    Socket.send(pickle.dumps(sentMsg))
                    freePortFound = True
                    break
        if(freePortFound):
            break
    
    # All Ports are Busy or all Machines that contain that file are down
    if(not freePortFound):
        sentMsg = {"id": MsgDetails.FAIL,
                   "Msg": "All ports are Busy, Try again Later."}
        Socket.send(pickle.dumps(sentMsg))


def upload_success(files_metadata, dataKeepers, recievedMsg, Socket, fileMetaDataLock, dataKeepersLock):
    # Extract Msg Data
    fileName = recievedMsg["fileName"]
    clientId = recievedMsg["clientId"]
    MachineIp = recievedMsg["ip"]
    MachinePort = recievedMsg["port"]
    # Update The Look-Up Table
    DKs = [MachineIp]
    fileMetaDataLock.acquire()
    files_metadata[fileName] = FileDetails(fileName, clientId, DKs)
    fileMetaDataLock.release()
    # The Port is Free Now
    modifiedArrPorts = dataKeepers[MachineIp].arrPort.copy()
    modifiedArrPorts[MachinePort].isBusy = False
    dataKeepersLock.acquire()
    dataKeepers[MachineIp] = DataKeeper(MachineIp, modifiedArrPorts,
                                        dataKeepers[MachineIp].isAlive)
    dataKeepersLock.release()
    # Replay To The Data keeper
    sentMsg = {"id": MsgDetails.OK}
    Socket.send(pickle.dumps(sentMsg))


def download_success(dataKeepers, recievedMsg, Socket, dataKeepersLock):
    # Extract Msg Data
    MachineIp = recievedMsg["ip"]
    MachinePort = recievedMsg["port"]
    # The Port is Free Now
    modifiedArrPorts = dataKeepers[MachineIp].arrPort.copy()
    modifiedArrPorts[MachinePort].isBusy = False
    dataKeepersLock.acquire()
    dataKeepers[MachineIp] = DataKeeper(MachineIp, modifiedArrPorts,
                                        dataKeepers[MachineIp].isAlive)
    dataKeepersLock.release()
    # Replay To The DK
    sentMsg = {"id": MsgDetails.OK}
    Socket.send(pickle.dumps(sentMsg))


############## Main Funciton ##############
def MasterClient(dataKeepers, files_metadata, portNum, fileMetaDataLock, dataKeepersLock):
    # Configure Master as a Replier to Client or DK
    ipPort = "*:{}".format(portNum)
    Socket, Context = configure_port(ipPort, zmq.REP, "bind")

    while (True):
        # Recieve message
        recievedMsg = pickle.loads(Socket.recv())
        msgType = recievedMsg["id"]

        # Take an action based on message Type
        if(msgType == MsgDetails.CLIENT_MASTER_UPLOAD):
            send_upload_data(dataKeepers, Socket, dataKeepersLock)

        elif(msgType == MsgDetails.CLIENT_MASTER_DOWNLOAD):
            send_download_data(files_metadata, dataKeepers,
                               recievedMsg, portNum, Socket, fileMetaDataLock, dataKeepersLock)

        elif(msgType == MsgDetails.DK_MASTER_UPLOAD_SUCCESS):
            upload_success(files_metadata, dataKeepers,
                           recievedMsg, Socket, fileMetaDataLock, dataKeepersLock)

        elif(msgType == MsgDetails.CLIENT_MASTER_DOWNLOAD_SUCCESS):
            download_success(dataKeepers, recievedMsg, Socket, dataKeepersLock)
