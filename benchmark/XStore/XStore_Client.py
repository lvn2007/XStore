import grpc
from protoFile import xStore_pb2
from protoFile import xStore_pb2_grpc
import time
from statistics import stdev
import random

address = '172.20.3.113'
port = '5000'

searchTime = []
startDate = 1483228801
endDate = 1514764799

class xStoreClient:
    def __init__(self):
        channel = grpc.insecure_channel(f'{address}:{port}')
        self.conn = xStore_pb2_grpc.xStoreStub(channel)

    def unaryRequest(self, epochTime, dbName):
        reqMsg = xStore_pb2.unaryRequestMsg(timestamp=epochTime, dbName=dbName)
        response = self.conn.unaryRequest(reqMsg)
        return response

    def batchRequest(self, startEpoch, endEpoch, dbName):
        toReturn = []
        reqMsg = xStore_pb2.rangeRequestMsg(startTime=startEpoch, endTime=endEpoch, dbName=dbName)
        batchResponse = self.conn.batchRequest(reqMsg)

        for i in batchResponse:
            toReturn.append(i)

        return toReturn


if __name__ == '__main__':
    # Init xStore client obj
    client = xStoreClient()

    for i in range(1):
        target = random.randint(startDate, endDate)
        # searchTime.append(batchRequest(1451606400, 1451616400))
        # result = client.unaryRequest(1451606400, 'tsla')
        result = client.batchRequest(1451606400, 1451616400, 'tsla')
        # searchTime.append(unaryRequest(target))
        # run()

    # myMin = min(searchTime)
    # myMax = max(searchTime)
    # myAvg = sum(searchTime) / len(searchTime)
    # myStdev = stdev(searchTime)
    #
    # print('--- EXECUTION TIME in [ms] ---')
    # print(f' -Min: {myMin * 1000:,.2f}')
    # print(f' -Max: {myMax * 1000:,.2f}')
    # print(f' -Mean: {myAvg * 1000:,.2f}')
    # print(f' -Stdev: {myStdev * 1000:,.2f}')
    #
    # print('\n--- EXECUTION TIME in [µs] ---')
    # print(f' -Min: {myMin * 1000000:,.2f}')
    # print(f' -Max: {myMax * 1000000:,.2f}')
    # print(f' -Mean: {myAvg * 1000000:,.2f}')
    # print(f' -Stdev: {myStdev * 1000000:,.2f}')
    #
    # print('\n--- EXECUTION TIME in [ns] ---')
    # print(f' -Min: {myMin * 1000000000:,.2f}')
    # print(f' -Max: {myMax * 1000000000:,.2f}')
    # print(f' -Mean: {myAvg * 1000000000:,.2f}')
    # print(f' -Stdev: {myStdev * 1000000000:,.2f}')