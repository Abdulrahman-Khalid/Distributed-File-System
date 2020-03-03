import enum
import zmq
from utils import *
import pickle
import sys
import cv2


# Using enum class create enumerations
#filePath = "./Videos/"

def DK_Rep(myPort, filePath, arrFullPaths):
    myIp = get_ip()
    # Configure myself as Replier
    mainSocket, mainContext = configure_port(
        "*:{}".format(myPort), zmq.REP, "bind")

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


def replicate_as_DST(recievedMsg, filePath, arrFullPaths, mainSocket):
    # Connect to Source
    socket, context = configure_port(
        recievedMsg["srcIp"] + ":" + recievedMsg["srcPort"], zmq.SUB, "connect")
    # Recieve the file from the src
    allMsg = pickle.loads(socket.recv())
    # Save file to the Hard Drive
    fullPath = filePath + allMsg["fileName"]
    with open(fullPath, "wb") as wfile:
        wfile.write(allMsg["data"])
        arrFullPaths.append(fullPath)
    # reply to master replication process
    mainSocket.send(pickle.dumps({"id": MsgDetails.OK}))
    # Terminate Connection
    socket.close()
    context.destroy()


def replicate_as_SRC(recievedMsg, myPort, myIp, filePath, mainSocket):
    # Configure my port
    socket, context = configure_port(
        myIp + ":" + myPort, zmq.PUB, "bind")
    # Read file from the Hard Drive
    file = open(filePath + recievedMsg["fileName"], "rb")
    video = file.read()
    file.close()
    # Send the file to the destination
    dataToSend = {"id": MsgDetails.OK,
                  "fileName": recievedMsg["fileName"], "data": video}
    socket.send(pickle.dumps(dataToSend))
    # reply to master replication process
    mainSocket.send(pickle.dumps({"id": MsgDetails.OK}))
    # Terminate Connection
    socket.close()
    context.destroy()


def send_to_client(recievedMsg, filePath, mainSocket):
    # Prepare the file path
    fullPath = filePath + recievedMsg["fileName"]
    # Read file from the Hard Drive
    file = open(fullPath, "rb")
    data = file.read()
    file.close()
    # Send the file to Client [Download]
    mainSocket.send(pickle.dumps(
        {"id": MsgDetails.OK, "fileName": recievedMsg["fileName"], "data": data}))


def recieve_from_client(recievedMsg, filePath, arrFullPaths, myIp, myPort, mainSocket):
    # Prepare the file path
    fullPath = filePath + recievedMsg["fileName"]
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
