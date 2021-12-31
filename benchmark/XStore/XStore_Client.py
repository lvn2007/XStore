import grpc
from protoFile import xStore_pb2
from protoFile import xStore_pb2_grpc
import time
from statistics import stdev
import random

searchTime = []
startDate = 1483228801
endDate = 1514764799

def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('172.20.3.113:5000') as channel:
        target = random.randint(startDate, endDate)
        # print(target)
        stub = xStore_pb2_grpc.xStoreStub(channel)
        startTime = time.time()
        response = stub.sendRequest(xStore_pb2.unaryRequest(timestamp=target))
        endTime = time.time()
        # print(response.dataset)
    searchTime.append(endTime-startTime)
    # print("Response: " + str(response.dataset))

def unaryRequest(epochTime):
    with grpc.insecure_channel('172.20.3.113:5000') as channel:
        stub = xStore_pb2_grpc.xStoreStub(channel)
        startTime = time.time()
        response = stub.sendRequest(xStore_pb2.unaryRequest(timestamp=epochTime))
        endTime = time.time()
    return endTime-startTime

def batchRequest(startEpoch, endEpoch):
    with grpc.insecure_channel('172.20.3.113:5000') as channel:
        stub = xStore_pb2_grpc.xStoreStub(channel)
        startExec = time.time()
        reqMsg = xStore_pb2.rangeRequest(startTime=startEpoch, endTime=endEpoch)
        batchResponses = stub.batchRequest(reqMsg)

        for i in batchResponses:
            pass

    # Total execution time of a batch request
    endExec = time.time()

    totalExec = endExec - startExec
    print(totalExec)
    return totalExec


if __name__ == '__main__':
    print(unaryRequest(1451606400))
    batchRequest(1451606400, 1451607400)
    # for i in range(1000):
    #     run()
    #
    # myMin = min(searchTime)
    # myMax = max(searchTime)
    # myAvg = sum(searchTime) / len(searchTime)
    # myStdev = stdev(searchTime)
    #
    # print('--- EXECUTION TIME in [s] ---')
    # print(f'  -Min: {myMin:,.2f}')
    # print(f'  -Max: {myMax:,.2f}')
    # print(f'  -Mean: {myAvg:,.2f}')
    # print(f'  -Stdev: {myStdev:,.2f}')
    #
    # print('\n--- EXECUTION TIME in [ms] ---')
    # print(f'  -Min: {myMin * 1000:,.2f}')
    # print(f'  -Max: {myMax * 1000:,.2f}')
    # print(f'  -Mean: {myAvg * 1000:,.2f}')
    # print(f'  -Stdev: {myStdev * 1000:,.2f}')
    #
    # print('\n--- EXECUTION TIME in [µs] ---')
    # print(f'  -Min: {myMin * 1000000:,.2f}')
    # print(f'  -Max: {myMax * 1000000:,.2f}')
    # print(f'  -Mean: {myAvg * 1000000:,.2f}')
    # print(f'  -Stdev: {myStdev * 1000000:,.2f}')
    #
    # print('\n--- EXECUTION TIME in [ns] ---')
    # print(f'  -Min: {myMin * 1000000000:,.2f}')
    # print(f'  -Max: {myMax * 1000000000:,.2f}')
    # print(f'  -Mean: {myAvg * 1000000000:,.2f}')
    # print(f'  -Stdev: {myStdev * 1000000000:,.2f}')