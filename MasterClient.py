from utils import *
import zmq
import pickle
from FileDetails import FileDetails
from Port import Port


def MasterClient(dataKeeprs, files_metadata, portNum):
    # Configure Master as a Replier to Client or DK
    ipPort = "*:{}".format(portNum)
    Socket, Context = configure_port(ipPort, zmq.REP, "bind")

    while (True):
        # Recieve message
        recievedMsg = pickle.loads(Socket.recv())
        msgType = recievedMsg["id"]

        # Take an action based on message Type
        if(msgType == MsgDetails.CLIENT_MASTER_UPLOAD):
            send_upload_data(dataKeeprs, Socket)

        elif(msgType == MsgDetails.CLIENT_MASTER_DOWNLOAD):
            send_download_data(files_metadata, dataKeeprs, recievedMsg, portNum, Socket)

        elif(msgType == MsgDetails.DK_MASTER_UPLOAD_SUCCESS):
            upload_success(files_metadata, dataKeeprs, recievedMsg, Socket)

        elif(msgType == MsgDetails.CLIENT_MASTER_DOWNLOAD_SUCCESS):
            download_success(dataKeeprs, recievedMsg, Socket)


def send_download_data(files_metadata, dataKeepers, recievedMsg, portNum, Socket):
    fileName = recievedMsg["fileName"]
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
                    dataKeeprs[DK_IP].arrPort[portNum] = Port(portNum ,True)
                    # Tell The Master The Ip and Port of This available Machine  
                    sentMsg = {
                        "id": MsgDetails.MASTER_CLIENT_DOWNLOAD_DETAILS, 
                        "ip": DK_IP, "port": portNum}
                    Socket.send(pickle.dumps(sentMsg))
                    freePortFound = True
                    break
        if(freePortFound):
            break


def send_upload_data(dataKeeprs, Socket):
    freePortFound = False   # Dummy Variable Used To Terminate The Outer Loop
    # For All Available Data Keepers 
    # TODO check on Size
    for DK_IP, DK in dataKeeprs.items():
        if(DK.isAlive):
            # Loop on all this Data Keeper Ports to Check if one of them is free 
            for portNum, port in value1.arrPort.items():
                if(not port.isBusy):
                    # Declare That this port isn't free any more
                    dataKeeprs[DK_IP].arrPort[portNum] = Port(portNum ,True)
                     # Tell The Master The Ip and Port of This available Machine  
                    sentMsg = {
                        "id": MsgDetails.MASTER_CLIENT_UPLOAD_DETAILS, 
                        "ip": DK_IP, "port": portNum}
                    Socket.send(pickle.dumps(sentMsg))
                    freePortFound = True
                    break
        if(freePortFound):
            break


def upload_success(files_metadata, dataKeepers, recievedMsg, Socket):
    # Extract Msg Data
    fileName = recievedMsg["fileName"]
    clientId = recievedMsg["clientId"]
    MachineIp = recievedMsg["ip"]
    MachinePort = recievedMsg["port"]
    DKs = [MachineIp]
    # Update The Look-Up Table
    files_metadata[fileName] = FileDetails(fileName, clientId ,DKs)
    # The Port is Free Now
    dataKeepers[MachineIp].arrPort[MachinePort] = Port(MachinePort , False)
    # Replay To The Data keeper
    sentMsg = {"id": MsgDetails.OK}
    Socket.send(pickle.dumps(sentMsg)) 


def download_success(dataKeepers, recievedMsg, Socket):
    # Extract Msg Data
    MachineIp = recievedMsg["ip"]
    MachinePort = recievedMsg["port"]

    dataKeepers[MachineIp].arrPort[MachinePort] = Port(MachinePort ,False)

    # Replay To The DK
    sentMsg = {"id": MsgDetails.OK}
    Socket.send(pickle.dumps(sentMsg))
