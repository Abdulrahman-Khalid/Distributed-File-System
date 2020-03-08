from utils import *
import sys
from random import randint

action = sys.argv[1]
fileName = sys.argv[2]
clientID = sys.argv[3]

socketMaster, contextMaster = configure_multiple_ports(
    masterIP, masterPortsArr, zmq.REQ)


def Download_file(socketMaster):
    # Ask The master For The ip and Port
    ask_master_to_download()
    # Recieve ip and Port from Master
    msgFromMaster = pickle.loads(socketMaster.recv())
    # End Connection with Master
    socketMaster.close()
    contextMaster.destroy()

    if(msgFromMaster['id'] == MsgDetails.FAIL):
        print(msgFromMaster["Msg"])
        return
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

    if(msgFromDK['id'] == MsgDetails.FAIL):
        print(msgFromDK["Msg"])
        # TODO: May be Sent To Master
        return

    # Save video
    with open(fileName, 'wb') as wfile:
        wfile.write(msgFromDK['data'])

    # tell the Master that The Download is succedded
    send_success_message(msgFromMaster)


def Upload_File(socketMaster):
    # Read Video
    try:
        file = open(fileName, 'rb')
        data = file.read()
        file.close()
    except:
        print("File isn't exist on your machine.")
        return

    # Ask The master For The ip and Port
    ask_master_to_upload()
    # Recieve ip and Port from Master
    msgFromMaster = pickle.loads(socketMaster.recv())
    # End Connection with Master
    socketMaster.close()
    contextMaster.destroy()

    if(msgFromMaster['id'] == MsgDetails.FAIL):
        print(msgFromMaster["Msg"])
        return
    # Connect to data keeper
    ipPort = msgFromMaster['ip'] + ":" + msgFromMaster['port']
    socketDK, contextDK = configure_port(ipPort, zmq.REQ, "connect")
    # Upload File To DK
    print("1")
    upload_file_to_DK(socketDK, data)
    print("2")
    # Recieve OK MSG From DK
    msgFromDK = pickle.loads(socketDK.recv())
    print("3")
    # End Connection With Data Keeper
    socketDK.close()
    contextDK.destroy()


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
               'fileName': fileName, 'data': data, 'clientId': clientID}
    socketDK.send(pickle.dumps(msgToDK))


def send_success_message(msgFromMaster):
    msgToMaster = {'id': MsgDetails.CLIENT_MASTER_DOWNLOAD_SUCCESS,
                   'ip': msgFromMaster['ip'], 'port': msgFromMaster['port']}
    # MAY BE REMOVED
    socketMaster, contextMaster = configure_multiple_ports(
        masterIP, masterPortsArr, zmq.REQ)
    socketMaster.send(pickle.dumps(msgToMaster))
    # Recieve OK MSG From Master
    msgFromMaster = pickle.loads(socketMaster.recv())
    socketMaster.close()
    contextMaster.destroy()


if (action == "download"):
    Download_file(socketMaster)
elif (action == "upload"):
    Upload_File(socketMaster)
