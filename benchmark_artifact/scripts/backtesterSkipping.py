# pip3 install pandas pyarrow pyzmq protobuf pymongo influxdb
import multiprocessing
from itertools import product
import pandas as pd
import numpy as np
import os, sys, time, argparse, requests
from dataclasses import dataclass
from pyarrow.parquet import ParquetFile
from XStore import XStoreClient
from pymongo import MongoClient
from influxdb import InfluxDBClient, DataFrameClient
from datetime import datetime

pd.set_option('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None, 'display.width', None)

def getGridParams(n):
    gridParams = list(product(range(1, n + 1), range(1, n + 1)))
    return gridParams[:n]

@dataclass
class Position:
    entryPrice: float
    qty: float
    isLong: bool

class MA_Indicator():
    def __init__(self, rollingPeriod):
        self._rollingPeriod = rollingPeriod
        self._maQueue = []

    def getValue(self, data):
        if len(self._maQueue) == self._rollingPeriod - 1:
            self._maQueue.append(data)
            maValue = sum(self._maQueue) / self._rollingPeriod
            self._maQueue.pop(0)
            return maValue
        else:
            self._maQueue.append(data)

class Backtester:
    def __init__(self, slowMA, fastMA, filename, ip_address, port_number, chunksize, skipsize=60, cash=100000):
        self._filename = filename
        self._ip_address = ip_address
        self._port_number = port_number
        self._chunksize = chunksize
        self._skipSize = skipsize
        self.cash = cash
        self.results = pd.DataFrame()
        self._strategy = self.Strategy(slowMA, fastMA)
        self._currentPosition = None

    class Strategy:
        def __init__(self, slowMA, fastMA):
            # Indicators
            self._slowMA = MA_Indicator(rollingPeriod=slowMA)
            self._fastMA = MA_Indicator(rollingPeriod=fastMA)

        def analyze(self, data):
            # Build MA
            slowMAValue = self._slowMA.getValue(data)
            fastMAValue = self._fastMA.getValue(data)

            if slowMAValue is None or fastMAValue is None:
                return

            if fastMAValue > slowMAValue:
                return 1
            elif fastMAValue < slowMAValue:
                return -1

    def _makeTrade(self, tradeSignal, data):
        # LONG
        if tradeSignal == 1 and self._currentPosition is None:
            self._currentPosition = Position(data, self.cash / data, True)
            return "LONG"
        elif tradeSignal == -1 and self._currentPosition and self._currentPosition.isLong:
            self.cash += (self._currentPosition.entryPrice * self._currentPosition.qty) - (data * self._currentPosition.qty)
            self._currentPosition = None
            return "EXIT LONG"

        # SHORT
        if tradeSignal == -1 and self._currentPosition is None:
            self._currentPosition = Position(data, self.cash / data, False)
            return "SHORT"
        elif tradeSignal == 1 and self._currentPosition and not self._currentPosition.isLong:
            self.cash += (data * self._currentPosition.qty) - (self._currentPosition.entryPrice * self._currentPosition.qty)
            self._currentPosition = None
            return "EXIT SHORT"

    def _getQueryRangeSkip(self, startTime, endTime, nSkip):
        queryRange = []

        for i in range(startTime, endTime, nSkip):
            queryRange.append(i)
        return queryRange

    def backtestCSV(self):
        # Assuming host's memory is smaller than dataset -> Read chunks
        #   However, multi-threaded model would be unsafe since interleaving threads would alter the file pointer of chunks
        #   Therefore, multi-process model is appropriate since data is now isolated
        totalIOTime = totalComputeTime = 0

        queryRanges = self._getQueryRangeSkip(946_684_800, 1_262_303_999, self._skipSize)

        startRead = time.time()
        df = pd.read_csv(self._filename, names=["Timestamp", "Open", "High", "Low", "Close", "Volume"], chunksize=self._chunksize, dtype={"Timestamp": "Int64", "Open": np.float64, "High": np.float64, "Low": np.float64, "Close": np.float64, "Volume": "Int64"}, header=0)
        endRead = time.time()
        totalIOTime += (endRead - startRead)

        ioStartTime = time.time()
        for chunkDF in df:
            # Filter & process rows within queryRanges
            chunkDF = chunkDF[chunkDF["Timestamp"].isin(queryRanges)]

            # Exit loop once df is empty
            if chunkDF.empty:
                break

            for rowIdx, rowTimestamp, rowOpen, rowHigh, rowLow, rowClose, rowVolume in zip(chunkDF.index, chunkDF.Timestamp, chunkDF.Open, chunkDF.High, chunkDF.Low, chunkDF.Close, chunkDF.Volume):
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(rowClose)
                result = self._makeTrade(tradeSignal, rowClose)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)
        ioEndTime = time.time()
        totalIOTime += (ioEndTime - ioStartTime)
        totalIOTime = totalIOTime - totalComputeTime

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestParquet(self):
        totalIOTime = totalComputeTime = 0

        queryRanges = self._getQueryRangeSkip(946_684_800, 1_262_303_999, self._skipSize)

        startRead = time.time()
        df = ParquetFile(self._filename.replace('.csv', '.parquet'))
        endRead = time.time()
        totalIOTime += (endRead - startRead)

        ioStartTime = time.time()
        for chunkDF in df.iter_batches(batch_size=self._chunksize, use_threads=False):
            # Filter & process rows within queryRanges
            chunkDF = chunkDF.to_pandas()
            chunkDF = chunkDF[chunkDF["Timestamp"].isin(queryRanges)]

            for rowIdx, rowTimestamp, rowOpen, rowHigh, rowLow, rowClose, rowVolume in zip(chunkDF.index, chunkDF.Timestamp, chunkDF.Open, chunkDF.High, chunkDF.Low, chunkDF.Close, chunkDF.Volume):
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(rowClose)
                result = self._makeTrade(tradeSignal, rowClose)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

            # deallocStartTime = time.time()
            # del chunkDF
            # gc.collect()
            # deallocEndTime = time.time()

        ioEndTime = time.time()
        totalIOTime += (ioEndTime - ioStartTime)
        totalIOTime = totalIOTime - totalComputeTime

        return [totalIOTime, totalComputeTime, self.cash]

    def _getQueryRange(self, startTime, endTime):
        queryRange = []

        for i in range(startTime, endTime, self._chunksize):
            currEnd = i + self._chunksize

            if currEnd > endTime:
                currEnd = endTime
            queryRange.append((i, currEnd))
        return queryRange


    def _chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def backtestMongo(self):
        totalIOTime = totalComputeTime = 0

        mongoClient = MongoClient(host=self._ip_address, port=self._port_number, timeoutMS=1_800_000, maxPoolSize=0)
        mongoCollection = mongoClient['BACKTESTER']['BACKTESTER']

        queryRange = self._getQueryRange(946684800, 1_262_303_999)
        queryRangeSkip = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRangeSkip = [datetime.fromtimestamp(i) for i in queryRangeSkip]

        # Querying random points using $in is extremely slow
        #   Issue #1: Large chunk size cause error - pymongo.errors.OperationFailure: Failed to optimize pipeline :: caused by :: BufBuilder attempted to grow%0D%0A() to 131072001 bytes, past the 125MB limit
        #   Issue #2: BSONObjectTooLarge
        for i in queryRange:
            ioStartTime = time.time()
            curQuery = {"Timestamp": {"$gte": datetime.fromtimestamp(i[0]), "$lt": datetime.fromtimestamp(i[1])}}
            mongoQuery = mongoCollection.find(curQuery).to_list()
            mongoQuery = pd.DataFrame(mongoQuery, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
            mongoQuery = mongoQuery[mongoQuery["Timestamp"].isin(queryRangeSkip)]
            ioEndTime = time.time()
            totalIOTime += (ioEndTime - ioStartTime)

            for rowIdx, rowTimestamp, rowOpen, rowHigh, rowLow, rowClose, rowVolume in zip(mongoQuery.index, mongoQuery.Timestamp, mongoQuery.Open, mongoQuery.High, mongoQuery.Low, mongoQuery.Close, mongoQuery.Volume):
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(rowClose)
                result = self._makeTrade(tradeSignal, rowClose)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestMongoUnary(self):
        totalIOTime = totalComputeTime = 0

        mongoClient = MongoClient(host=self._ip_address, port=self._port_number, timeoutMS=1_800_000, maxPoolSize=0)
        mongoCollection = mongoClient['BACKTESTER']['BACKTESTER']

        queryRangeSkip = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRangeSkip = list(self._chunks(queryRangeSkip, self._chunksize))

        for chunkTime in queryRangeSkip:
            dataList = []
            for i in chunkTime:
                ioStartTime = time.time()
                mongoQuery = mongoCollection.find_one({"Timestamp": datetime.fromtimestamp(i)})["Close"]
                ioEndTime = time.time()
                totalIOTime += (ioEndTime - ioStartTime)
                dataList.append(mongoQuery)

            for data in dataList:
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(data)
                result = self._makeTrade(tradeSignal, data)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestInflux(self):
        totalIOTime = totalComputeTime = 0

        # influxClient = InfluxDBClient(self._ip_address, self._port_number, database="BACKTESTER")
        influxClient = DataFrameClient(self._ip_address, self._port_number, database="BACKTESTER")

        queryRange = self._getQueryRange(946684800, 1_262_303_999)
        queryRangeSkip = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRangeSkip = [datetime.fromtimestamp(i) for i in queryRangeSkip]

        # InfluxQL doesn't support querying a batch of non-continuous timestamps");
        # REF 1: https://docs.influxdata.com/influxdb/v1/query_language/explore-data/#syntax-1")
        # REF 2: https://community.influxdata.com/t/querying-multiple-time-points/35388");
        for i in queryRange:
            curQuery = f"SELECT * FROM BACKTESTER WHERE time >= {i[0]}s AND time < {i[1]}s"
            ioStartTime = time.time()
            influxQuery = influxClient.query(curQuery)
            influxQuery = influxQuery["BACKTESTER"]
            influxQuery.index = influxQuery.index.values.astype("datetime64[s]")
            influxQuery = influxQuery[influxQuery.index.isin(queryRangeSkip)]
            ioEndTime = time.time()
            totalIOTime += (ioEndTime - ioStartTime)

            for rowIdx, rowOpen, rowHigh, rowLow, rowClose, rowVolume in zip(influxQuery.index, influxQuery.Open, influxQuery.High, influxQuery.Low, influxQuery.Close, influxQuery.Volume):
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(rowClose)
                result = self._makeTrade(tradeSignal, rowClose)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestInfluxUnary(self):
        totalIOTime = totalComputeTime = 0

        influxClient = InfluxDBClient(self._ip_address, self._port_number, database="BACKTESTER")

        queryRangeSkip = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRangeSkip = list(self._chunks(queryRangeSkip, self._chunksize))

        # InfluxQL doesn't support querying a batch of non-continuous timestamps");
        # REF 1: https://docs.influxdata.com/influxdb/v1/query_language/explore-data/#syntax-1")
        # REF 2: https://community.influxdata.com/t/querying-multiple-time-points/35388");
        for chunkTime in queryRangeSkip:
            dataList = []
            for i in chunkTime:
                curQuery = f"SELECT * FROM BACKTESTER WHERE time = {i}s"
                ioStartTime = time.time()
                influxQuery = next(influxClient.query(curQuery).get_points())["Close"]
                ioEndTime = time.time()
                totalIOTime += (ioEndTime - ioStartTime)
                dataList.append(influxQuery)

            for data in dataList:
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(data)
                result = self._makeTrade(tradeSignal, data)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestGraphite(self):
        totalIOTime = totalComputeTime = 0
        
        url = f"http://{self._ip_address}:{self._port_number}/render"
        HTTPSession = requests.Session()

        queryRangeSkip = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRangeSkip = list(self._chunks(queryRangeSkip, self._chunksize))

        # Graphite doesn't support querying a batch of non-continuous timestamps"
        for chunkTime in queryRangeSkip:
            dataList = []
            for i in chunkTime:
                curQuery = {
                    "target": "BACKTESTER.*",
                    "from": i - 1,
                    "until": i,
                    "format": "json"
                }
                ioStartTime = time.time()
                graphiteQuery = HTTPSession.get(url, params=curQuery).json()[0]["datapoints"][0][0]
                ioEndTime = time.time()
                totalIOTime += (ioEndTime - ioStartTime)
                dataList.append(graphiteQuery)

            for data in dataList:
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(data)
                result = self._makeTrade(tradeSignal, data)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestXStore(self):
        totalIOTime = totalComputeTime = 0

        xstoreClient = XStoreClient(self._ip_address, self._port_number)

        queryRange = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRange = list(self._chunks(queryRange, self._chunksize))

        for i in queryRange:
            ioStartTime = time.time()
            xstoreQuery = xstoreClient.batchQuery(i, "BACKTESTER", False)
            ioEndTime = time.time()
            totalIOTime += (ioEndTime - ioStartTime)

            for data in xstoreQuery:
                ioStartTime = time.time()
                data = data.data.fieldData[3].doubleData
                ioEndTime = time.time()
                totalIOTime += (ioEndTime - ioStartTime)

                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(data)
                result = self._makeTrade(tradeSignal, data)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

    def backtestXStoreUnary(self):
        totalIOTime = totalComputeTime = 0

        xstoreClient = XStoreClient(self._ip_address, self._port_number)

        queryRangeSkip = self._getQueryRangeSkip(946684800, 1_262_303_999, self._skipSize)
        queryRangeSkip = list(self._chunks(queryRangeSkip, self._chunksize))

        for chunkTime in queryRangeSkip:
            dataList = []
            for i in chunkTime:
                ioStartTime = time.time()
                xstoreQuery = xstoreClient.unaryQuery(i, "BACKTESTER").data.fieldData[3].doubleData
                ioEndTime = time.time()
                totalIOTime += (ioEndTime - ioStartTime)
                dataList.append(xstoreQuery)

            for data in dataList:
                computeStartTime = time.time()
                tradeSignal = self._strategy.analyze(data)
                result = self._makeTrade(tradeSignal, data)
                computeEndTime = time.time()
                totalComputeTime += (computeEndTime - computeStartTime)

        return [totalIOTime, totalComputeTime, self.cash]

def processBacktestCSV(instance):
    return instance.backtestCSV()

def processBacktestParquet(instance):
    return instance.backtestParquet()

def processBacktestMongo(instance):
    return instance.backtestMongoUnary()

def processBacktestInflux(instance):
    return instance.backtestInfluxUnary()

def processBacktestXStore(instance):
    return instance.backtestXStore()

def processBacktestGraphite(instance):
    return instance.backtestGraphite()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='XBench - Backtester Application')
    parser.add_argument('--grid-size', type=int, required=True,
                        help='Size of grid search (An int larger than 0)')
    parser.add_argument('-f', '--filename', type=str, required=False, default='data.csv',
                        help='CSV Input filename')
    parser.add_argument('-ip', '--ip_address', type=str, required=False, default='127.0.0.1',
                        help='IP address of DB Server')
    parser.add_argument('-port', '--port_number', type=int, required=False, default=9497,
                        help='Port number of DB Server')
    parser.add_argument('-c', '--chunk-size', type=int, required=True,
                        help='Chunk size')
    parser.add_argument('-s', '--skip-size', type=int, required=True,
                        help='Skip size')
    parser.add_argument('-d', '--database-name', type=str, required=True,
                        help='Name of database to query from', choices=['CSV', 'PARQUET', 'XSTORE', 'MONGODB', 'INFLUXDB', 'GRAPHITE'])
    parser.add_argument('-o', '--output-filename', type=str, required=False,
                        help='Name of output file', default='BacktesterOutput.txt')
    parser.add_argument('-ia', '--invoke-at', type=int, required=False,
                        help='Invoke benchmark at specific time (epoch time)', default=0)

    # Proc map
    ProcessMap = {
        "CSV": processBacktestCSV,
        "PARQUET": processBacktestParquet,
        "XSTORE": processBacktestXStore,
        "MONGODB": processBacktestMongo,
        "INFLUXDB": processBacktestInflux,
        "GRAPHITE": processBacktestGraphite,
    }
    
    # If no argument given -> Print help msg
    if len(sys.argv) == 1:
        parser.print_help()
        exit(1)

    # Parsing args
    args = parser.parse_args()

    if args.grid_size < 1:
        parser.print_help()
        exit(1)
    if args.chunk_size < 1:
        parser.print_help()
        exit(1)
    if args.database_name in ["CSV", "PARQUET"] and not os.path.isfile(args.filename):
        parser.print_help()
        exit(1)

    gridParams = getGridParams(args.grid_size)
    toProcess = []

    for i in gridParams:
        toProcess.append(Backtester(i[0], i[1], args.filename, args.ip_address, args.port_number, args.chunk_size, args.skip_size))

    TaskFunc = ProcessMap.get(args.database_name)
    
    print(f"Total parameter combination: {len(toProcess)}", flush=True)
    print(f" Available Threads: {multiprocessing.cpu_count()}", flush=True)
    print(f" Filename: {args.filename}", flush=True)
    print(f" IP Address: {args.ip_address}", flush=True)
    print(f" Port Number: {args.port_number}", flush=True)
    print(f" Chunk Size: {args.chunk_size:,}", flush=True)
    print(f" Skip Size: {args.skip_size:,}", flush=True)
    print(f" Database: {args.database_name}", flush=True)

    # Poll till invoke time is reached
    while int(time.time()) < args.invoke_at:
        time.sleep(0.001)

    with multiprocessing.Pool() as pool:
        startTime = time.perf_counter()
        updated_instances = list(pool.imap(TaskFunc, toProcess))
        endTime = time.perf_counter()

    avgIOTime = sum(x[0] for x in updated_instances) / len(toProcess)
    avgComputeTime = sum(x[1] for x in updated_instances) / len(toProcess)

    with open(args.output_filename, "w") as f:
        print(f"Total parameter combination: {len(toProcess)}", file=f)
        print(f" No. of Core: {multiprocessing.cpu_count()}", file=f)
        print(f" Filename: {args.filename}", file=f)
        print(f" IP Address: {args.ip_address}", file=f)
        print(f" Port Number: {args.port_number}", file=f)
        print(f" Chunk Size: {args.chunk_size:,}", file=f)
        print(f" Skip Size: {args.skip_size:,}", file=f)
        print(f" Database: {args.database_name}", file=f)
        print(f" Total time: {endTime - startTime:,.2f}s", file=f)
        print(f" Throughput: {len(toProcess) * 3600 / (endTime - startTime):,.2f} Backtests per Hour", file=f)
        print(f" Avg. I/O Time: {avgIOTime:,.2f}s", file=f)
        print(f" Avg. Compute Time: {avgComputeTime:,.2f}s\n", file=f)
        f.flush()
    print(f" Total time: {endTime - startTime:,.2f}s", flush=True)
    print(f" Throughput: {len(toProcess) * 3600 / (endTime - startTime):,.2f} Backtests per Hour", flush=True)
    print(f" Avg. I/O Time: {avgIOTime:,.2f}s", flush=True)
    print(f" Avg. Compute Time: {avgComputeTime:,.2f}s\n", flush=True)
    