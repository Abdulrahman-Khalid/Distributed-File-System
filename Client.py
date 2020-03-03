from utils import *
import sys
from random import randint

action = sys.argv[1]
fileName = sys.argv[2]
clintID = sys.argv[3]

masterPort = "50002"
socketMaster, contextMaster = configure_port(
    masterIP + ":" + masterPort, zmq.REQ, "connect")


def Download_file():
    # Ask The master For The ip and Port
    ask_master_to_download()
    # Recieve ip and Port from Master
    msgFromMaster = pickle.loads(socketMaster.recv())
    # End Connection with Master
    socketMaster.close()
    contextMaster.destroy()
    # Connect to data keeper
    ipPort = msgFromMaster['ip'] + ":" + msgFromMaster['port']
    socketDK, contextDK = configure_port(ipPort, zmq.REQ, "connect")
    # Give the Dk the file name to recieve it
    ask_DK_to_download(socketDK)
    # Recieved the required file
    msgFromDK = pickle.loads(socketDK.recv())
    # End Connection with DK
    socketDK.close()
    contextDK.destroy()
    # Save video
    with open(fileName, 'wb') as wfile:
        wfile.write(msgFromDK['data'])

    masterPort = "50002"
    socketMaster2, contextMaster2 = configure_port(
        masterIP + ":" + masterPort, zmq.REQ, "connect")
    # tell the Master that The Download is succedded
    send_success_message(socketMaster2, msgFromMaster)
    # Recieve OK MSG From Master
    msgFromMaster = pickle.loads(socketMaster2.recv())
    # End Connection with Master
    socketMaster2.close()
    contextMaster2.destroy()


def Upload_File():
    # Read Video
    file = open(fileName, "rb")
    data = file.read()
    file.close()
    # Ask The master For The ip and Port
    ask_master_to_upload()
    # Recieve ip and Port from Master
    msgFromMaster = pickle.loads(socketMaster.recv())
    # Connect to data keeper
    ipPort = msgFromMaster['ip'] + ":" + msgFromMaster['port']
    socketDK, contextDK = configure_port(ipPort, zmq.REQ, "connect")
    # Upload File To DK
    upload_file_to_DK(socketDK, data)
    # Recieve OK MSG From DK
    msgFromDK = pickle.loads(socketDK.recv())


def ask_master_to_download():
    msgToMaster = {'id': MsgDetails.CLIENT_MASTER_DOWNLOAD,
                   'fileName': fileName}
    msgToMaster = pickle.dumps(msgToMaster)
    socketMaster.send(msgToMaster)


def ask_master_to_upload():
    msgToMaster = {'id': MsgDetails.CLIENT_MASTER_UPLOAD}
    msgToMaster = pickle.dumps(msgToMaster)
    socketMaster.send(msgToMaster)


def ask_DK_to_download(socketDK):
    msgToDK = {'id': MsgDetails.CLIENT_DK_DOWNLOAD, 'fileName': fileName}
    msgToDK = pickle.dumps(msgToDK)
    socketDK.send(msgToDK)


def upload_file_to_DK(socketDK, data):
    msgToDK = {'id': MsgDetails.CLIENT_DK_UPLOAD,
               'fileName': fileName, 'data': data, 'clientId': clintID}
    socketDK.send(pickle.dumps(msgToDK))


def send_success_message(socketMaster, msgFromMaster):
    msgToMaster = {'id': MsgDetails.CLIENT_MASTER_DOWNLOAD_SUCCESS,
                   'ip': msgFromMaster['ip'], 'port': msgFromMaster['port']}
    msgToMaster = pickle.dumps(msgToMaster)
    socketMaster.send(msgToMaster)


if (action == "download"):
    Download_file()
elif (action == "upload"):
    Upload_File()
