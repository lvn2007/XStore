import sys
import XStore_pb2 as XStore_Proto
import zmq


class XStore_Client:
    def __init__(self, SERVER_ADDR, SERVER_PORT):
        print(f'Connecting to tcp://{SERVER_ADDR}:{SERVER_PORT}')
        self.zmqContext = zmq.Context()
        self.socket = self.zmqContext.socket(zmq.REQ)
        self.socket.connect(f'tcp://{SERVER_ADDR}:{SERVER_PORT}')

    def infoRequest(self):
        reqCMD = XStore_Proto.reqCMD()
        reqCMD.rType = XStore_Proto.INFOREQUEST
        self.socket.send(reqCMD.SerializeToString())
        # print(reqCMD.SerializeToString())
        recvData = self.socket.recv()

        rcvObj = XStore_Proto.sysInfo_REP()
        rcvObj.ParseFromString(recvData)
        print(rcvObj)


if __name__ == '__main__':
    # Command-line args
    SERVER_ADDR = sys.argv[1]
    SERVER_PORT = sys.argv[2]

    # XStore client object
    XStore = XStore_Client(SERVER_ADDR, SERVER_PORT)
    XStore.infoRequest()