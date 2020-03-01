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
        recievedMsg  = pickle.loads(Socket.recv())

        # Take an action based on message Type
        if(recievedMsg["id"] == MsgDetails.CLIENT_MASTER_UPLOAD):
            send_upload_data(dataKeeprs, Socket)            

        elif(recievedMsg["id"] == MsgDetails.CLIENT_MASTER_DOWNLOAD):
            send_download_data(files_metadata, recievedMsg, portNum, Socket)
            
        elif(recievedMsg["id"] == MsgDetails.DK_MASTER_UPLOAD_SUCCESS):
            upload_success(files_metadata, dataKeeprs, recievedMsg, Socket)
            
        elif(recievedMsg["id"] == MsgDetails.CLIENT_MASTER_DOWNLOAD_SUCCESS):
            download_success(dataKeeprs, recievedMsg, Socket)

def send_download_data(files_metadata, recievedMsg, portNum, Socket):
    file_metadata = files_metadata[recievedMsg["fileName"]]
    freePortFound = False
    for idx, DK in enumerate(file_metadata.DKs):
        if(DK.isAlive):
            for key, value in DK.arrPort.items():
                if(not value.isBusy):
                    file_metadata.DKs[idx].arrPort[key].isBusy = True
                    sentMsg = {"id": MsgDetails.MASTER_CLIENT_DOWNLOAD_DETAILS, "ip": DK.ip, "port": key }
                    Socket.send(pickle.dumps(sentMsg))
                    freePortFound = True
                    break
        if(freePortFound):
            break

def send_upload_data(dataKeeprs, Socket):
    freePortFound = False
    for key1, value1 in dataKeeprs.items():
        if(value1.isAlive):
            for key2, value2 in value1.arrPort.items():
                if(not value2.isBusy):
                    dataKeeprs[key1].arrPort[key2].isBusy = True
                    sentMsg = {"id": MsgDetails.MASTER_CLIENT_UPLOAD_DETAILS, "ip": key1, "port": key2 }
                    Socket.send(pickle.dumps(sentMsg))
                    freePortFound = True
                    break
        if(freePortFound):
            break
        
def upload_success(files_metadata, dataKeepers, recievedMsg, Socket):
    DKs = []
    DKs.append(dataKeepers[recievedMsg["ip"]])
    files_metadata[recievedMsg["fileName"]] = FileDetails(recievedMsg["fileName"], 
                                                          recievedMsg["clientId"],
                                                          DKs)
    dataKeepers[recievedMsg["ip"]].arrPort[recievedMsg["port"]].isBusy = False
    sentMsg = {"id": MsgDetails.OK}
    Socket.send(pickle.dumps(sentMsg))

def download_success(dataKeepers, recievedMsg, Socket):
    dataKeepers[recievedMsg["ip"]].arrPort[recievedMsg["port"]].isBusy = False
    sentMsg = {"id": MsgDetails.OK}
    Socket.send(pickle.dumps(sentMsg))
