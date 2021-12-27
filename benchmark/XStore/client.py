import grpc
import mathtest_pb2
import mathtest_pb2_grpc
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
        stub = mathtest_pb2_grpc.MathTestStub(channel)
        startTime = time.time()
        response = stub.sendRequest(mathtest_pb2.MathRequest(timeStamp=target))
        endTime = time.time()
        # print(response.dataset)
    searchTime.append(endTime-startTime)
    # print("Response: " + str(response.dataset))


if __name__ == '__main__':
    for i in range(1000):
        run()

    myMin = min(searchTime)
    myMax = max(searchTime)
    myAvg = sum(searchTime) / len(searchTime)
    myStdev = stdev(searchTime)

    print('--- EXECUTION TIME in [s] ---')
    print(f'  -Min: {myMin:,.2f}')
    print(f'  -Max: {myMax:,.2f}')
    print(f'  -Mean: {myAvg:,.2f}')
    print(f'  -Stdev: {myStdev:,.2f}')

    print('\n--- EXECUTION TIME in [ms] ---')
    print(f'  -Min: {myMin * 1000:,.2f}')
    print(f'  -Max: {myMax * 1000:,.2f}')
    print(f'  -Mean: {myAvg * 1000:,.2f}')
    print(f'  -Stdev: {myStdev * 1000:,.2f}')

    print('\n--- EXECUTION TIME in [µs] ---')
    print(f'  -Min: {myMin * 1000000:,.2f}')
    print(f'  -Max: {myMax * 1000000:,.2f}')
    print(f'  -Mean: {myAvg * 1000000:,.2f}')
    print(f'  -Stdev: {myStdev * 1000000:,.2f}')

    print('\n--- EXECUTION TIME in [ns] ---')
    print(f'  -Min: {myMin * 1000000000:,.2f}')
    print(f'  -Max: {myMax * 1000000000:,.2f}')
    print(f'  -Mean: {myAvg * 1000000000:,.2f}')
    print(f'  -Stdev: {myStdev * 1000000000:,.2f}')