import enum
import zmq
from utils import *
import pickle
import sys
import cv2


def replicate_as_DST(recievedMsg, filePath, arrFullPaths, mainSocket):
    # Extract Msg Data
    srcIP = recievedMsg["srcIp"]
    srcPort = recievedMsg["srcPort"]
    # We have To Define New Port for Src as 
    # The main port is used for receiving not sending
    newSrcPort = str(int(srcPort) + 10000)
    # Connect to Source
    socket, context = configure_port(srcIP + ":" + newSrcPort, zmq.PULL, "connect")
    # Recieve the file from the src
    dataMsg = pickle.loads(socket.recv())
    # Save file to the Hard Drive
    fullPath = filePath + dataMsg["fileName"]
    with open(fullPath, "wb") as wfile:
        wfile.write(dataMsg["data"])
        arrFullPaths.append(fullPath)
    # reply to master replication process
    mainSocket.send(pickle.dumps({"id": MsgDetails.OK}))
    # Terminate Connection
    socket.close()
    context.destroy()


def replicate_as_SRC(recievedMsg, myPort, myIp, filePath, mainSocket):
    # Extract Msg Data
    fileName = recievedMsg["fileName"]
    # We have To Define New Port for Src as 
    # The main port is used for receiving not sending
    newPort = str(int(myPort) + 10000)
    # Configure my Port
    socket, context = configure_port(myIp + ":" + newPort, zmq.PUSH, "bind")
    # Read file from the Hard Drive
    file = open(filePath + fileName, "rb")
    video = file.read()
    file.close()
    # Send the file to the destination
    dataToSend = {"id": MsgDetails.OK, "fileName": fileName, "data": video}
    socket.send(pickle.dumps(dataToSend))
    # reply to master replication process
    mainSocket.send(pickle.dumps({"id": MsgDetails.OK}))
    # Terminate Connection
    socket.close()
    context.destroy()


def send_to_client(recievedMsg, filePath, mainSocket):
    # Extract Msg Data
    fileName = recievedMsg["fileName"]
    # Prepare the file path
    fullPath = filePath + fileName 
    # Read file from the Hard Drive
    file = open(fullPath, "rb")
    data = file.read()
    file.close()
    # Send the file to Client [Download]
    mainSocket.send(pickle.dumps({"id": MsgDetails.OK, "fileName": fileName , "data": data}))


def recieve_from_client(recievedMsg, filePath, arrFullPaths, myIp, myPort, mainSocket):
    # Extract Msg Data
    fileName = recievedMsg["fileName"]
    # Prepare the file path
    fullPath = filePath + fileName
    # Save file to the Hard Drive [Upload]
    with open(fullPath, "wb") as wfile:
        wfile.write(recievedMsg["data"])
    arrFullPaths.append(fullPath)
    # FROM DK TO CLIENT
    mainSocket.send(pickle.dumps({"id": MsgDetails.OK}))
    # Tell the master that the upload is ended successfully
    send_upload_success_to_master(recievedMsg, filePath, myIp, myPort)


def send_upload_success_to_master(recievedMsg, filePath, myIp, myPort):
    # Configure the connection with master
    randMasterPort = "50002"  # TODO genrate random port from Utils.masterPortsArr
    socket, context = configure_port(
        masterIP + ":" + randMasterPort, zmq.REQ, "connect")
    # Tell The master my Ip and Port to make me available again [Free Port]
    dataToSend = {"id": MsgDetails.DK_MASTER_UPLOAD_SUCCESS,
                  "ip": myIp, "port": myPort, "clientId": recievedMsg["clientId"],
                  "fileName": recievedMsg["fileName"], "filePath": filePath}  # FROM DK TO MASTER
    socket.send(pickle.dumps(dataToSend))
    # Recieve OK MSG From Master
    msgFromMaster = pickle.loads(socket.recv())
    socket.close()
    context.destroy()


############## Main Funciton ##############
def DK_Rep(myPort, filePath, arrFullPaths, myIp):
    # Configure myself as Replier
    mainSocket, mainContext = configure_port(
        myIp + ":{}".format(myPort), zmq.REP, "bind")
        
    while (True):
        # Recieve Msg
        recievedMsg = pickle.loads(mainSocket.recv())
        msgType = recievedMsg["id"]

        # Take action based on the Msg
        if(msgType == MsgDetails.MASTER_DK_REPLICATE):
            replicationRole = recievedMsg["type"]
            if(replicationRole == DataKeeperType.DST):
                replicate_as_DST(recievedMsg, filePath, arrFullPaths, mainSocket)
            
            elif(replicationRole == DataKeeperType.SRC):
                replicate_as_SRC(recievedMsg, myPort, myIp, filePath, mainSocket)

        elif(msgType == MsgDetails.CLIENT_DK_DOWNLOAD):
            send_to_client(recievedMsg, filePath, mainSocket)

        elif(msgType == MsgDetails.CLIENT_DK_UPLOAD):
            recieve_from_client(recievedMsg, filePath,
                                arrFullPaths, myIp, myPort, mainSocket)
