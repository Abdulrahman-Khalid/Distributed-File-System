import pprint
import sys
import time
import zmq
from utils import *


def get_instance_count(DKs):
    Count = 0
    for DK in DKs:
        if (DK.isAlive):
            Count += 1
    return Count


def get_source_Machine(DKs, dataKeepers):
    for DK in DKs:
        if (DK.isAlive):
            for key, value in DK.arrPort.items():
                if(not value.isBusy):
                    dataKeepers[DK.ip].arrPort[key].isBusy = True
                    return DK.ip, key
    return None, None


def file_on_this_DK(ip, DKs):
    for DK in DKs:
        if(ip == DK.ip):
            return True
    return False


def select_machines_to_copy_to(num, DKs, dataKeepers):
    arr = []
    for key1, value1 in dataKeepers.items():
        if(value1.isAlive):
            if(not file_on_this_DK(key1, DKs)):
                for key2, value2 in value1.arrPort.item():
                    if(not value2.isBusy):
                        dataKeepers[key1].arrPort[key2].isBusy = True
                        arr.append((key1, key2))
                        if(num == len(arr)):
                            return arr
    return arr


def MasterDK_Rep(dataKeepers, files_metadata):
    while True:
        for file in files_metadata.values():
            instanceCount = get_instance_count(file.DKs)
            Replications = replicationFactor - instanceCount
            ip, port = get_source_Machine(file.DKs, dataKeepers)
            freePorts = select_machines_to_copy_to(
                Replications, file.DKs, dataKeepers)

            '''
            transfer data from source to destination
            '''
            src_socket, src_context = configure_port(
                ip + ":" + port, zmq.REQ, 'connect')
            srcMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                          "type": DataKeeperType.SRC}
            src_socket.send(pickle.dumps(srcMessage))

            for ipPort in freePorts:
                dst_socket, dst_context = configure_port(
                    ipPort[0] + ":" + ipPort[1], zmq.REQ, 'connect')
                dstMessage = {'id': MsgDetails.MASTER_DK_REPLICATE,
                              "type": DataKeeperType.DST, 'srcIp': ip, 'srcPort': port}
                dst_socket.send(pickle.dumps(dstMessage))
                # Recieve OK MSG From Master
                msgFromDK = pickle.loads(dst_socket.recv())
                dataKeepers[ipPort[0]].arrPort[ipPort[1]].isBusy = True

            dataKeepers[ip].arrPort[port].isBusy = True
            src_socket.close()
            dst_socket.close()
            src_context.destroy()
            dst_context.destroy()
        time.sleep(replicationPeriod)
