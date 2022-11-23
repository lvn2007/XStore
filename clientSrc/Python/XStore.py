import sys
import time
from typing import List
import XStore_pb2 as XStore_Proto
import zmq
import dataclasses


@dataclasses.dataclass
class DBInfo:
    dbName: str
    dbResolution: str
    totalSizeInByte: int


@dataclasses.dataclass
class SysInfo:
    serverAddr: str
    threadCount: int
    dbList: List[DBInfo]


@dataclasses.dataclass
class StatusMessage:
    statusMessage: str


@dataclasses.dataclass
class QueryResult:
    timestamp: int
    data: str
    queryStatus: StatusMessage


@dataclasses.dataclass
class StatusMessages:
    statusMessages: list


@dataclasses.dataclass
class QueryResults:
    queryResult: List[QueryResult]
    queryStatus: StatusMessage


class XStoreClient:
    def __init__(self, SERVER_ADDR, SERVER_PORT):
        print(f'Connecting to tcp://{SERVER_ADDR}:{SERVER_PORT}')
        self.zmqContext = zmq.Context()
        self.socket = self.zmqContext.socket(zmq.REQ)
        self.socket.connect(f'tcp://{SERVER_ADDR}:{SERVER_PORT}')

    def infoRequest(self):
        """
        :return: Nothing
        """
        # Build msg prior to dispatch
        reqCMD = XStore_Proto.reqCMD()
        reqCMD.rType = XStore_Proto.INFO

        # Dispatch request
        self.socket.send(reqCMD.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.sysInfo_REP()
        recvObj.ParseFromString(recvData)

        # List of dbInfo
        dbInfo = []

        # Extract & Append each DB Info into list
        for i in recvObj.dbInfo:
            currDB = DBInfo(i.dbName, i.dbResolution, i.totalSizeInByte)
            dbInfo.append(currDB)

        # SysInfo as returning object
        return SysInfo(recvObj.serverAddr, recvObj.threadCount, dbInfo)

    def createDB(self, dbName, timeResolution, rowLength):
        """
        :param dbName:
        :param timeResolution:
        :param rowLength:
        :return:
        """
        # Build msg prior to dispatch
        createDB_REQ = XStore_Proto.createDB_REQ()
        createDB_REQ.rType = XStore_Proto.CREATEDB
        createDB_REQ.dbName = dbName
        createDB_REQ.timeResolution = timeResolution
        createDB_REQ.rowLength = rowLength

        # Dispatch request
        self.socket.send(createDB_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.createDB_REP()
        recvObj.ParseFromString(recvData)
        return StatusMessage(recvObj.createDBStatus)

    def unaryInsert(self, insertUnixTime, data, targetDBName):
        """
        :param insertUnixTime:
        :param data:
        :param targetDBName:
        :return:
        """
        # Build msg prior to dispatch
        unaryInsert_REQ = XStore_Proto.unaryInsert_REQ()
        unaryInsert_REQ.rType = XStore_Proto.UNARYINSERT
        unaryInsert_REQ.timestamp = insertUnixTime
        unaryInsert_REQ.data = data
        unaryInsert_REQ.dbName = targetDBName

        # Dispatch request
        self.socket.send(unaryInsert_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.unaryInsert_REP()
        recvObj.ParseFromString(recvData)
        return StatusMessage(recvObj.insertStatus)

    def unaryQuery(self, targetUnixTime, targetDBName):
        """
        :param targetUnixTime:
        :param targetDBName:
        :return:
        """
        # Build msg prior to dispatch
        unaryQuery_REQ = XStore_Proto.unaryQuery_REQ()
        unaryQuery_REQ.rType = XStore_Proto.UNARYQUERY
        unaryQuery_REQ.timestamp = targetUnixTime
        unaryQuery_REQ.dbName = targetDBName

        # Dispatch request
        self.socket.send(unaryQuery_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.unaryQuery_REP()
        recvObj.ParseFromString(recvData)
        return QueryResult(recvObj.timestamp, recvObj.dataset, recvObj.queryStatus)

    def batchInsert(self, dataList, targetDBName):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        # Build msg prior to dispatch
        batchInsert_REQ = XStore_Proto.batchInsert_REQ()
        batchInsert_REQ.rType = XStore_Proto.BATCHINSERT
        batchInsert_REQ.dbName = targetDBName

        # Loop through each data and add to batchInsert_REQ
        for data in dataList:
            # Organize batchInsert_DATA
            batchInsert_DATA = batchInsert_REQ.batchInsert_DATA.add()
            batchInsert_DATA.timestamp = data[0]
            batchInsert_DATA.data = data[1]

        # Dispatch request
        self.socket.send(batchInsert_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.batchInsert_REP()
        recvObj.ParseFromString(recvData)
        return StatusMessages(recvObj.insertStatus)

    def rangeQuery(self, startUnixTime, endUnixTime, targetDBName):
        """
        :param startUnixTime:
        :param endUnixTime:
        :param targetDBName:
        :return:
        """
        # Build msg prior to dispatch
        rangeQuery_REQ = XStore_Proto.rangeQuery_REQ()
        rangeQuery_REQ.rType = XStore_Proto.RANGEQUERY
        rangeQuery_REQ.startTime = startUnixTime
        rangeQuery_REQ.endTime = endUnixTime
        rangeQuery_REQ.dbName = targetDBName

        # Dispatch request
        self.socket.send(rangeQuery_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.batchQuery_REP()
        recvObj.ParseFromString(recvData)

        # List of dbInfo
        queryResults = []

        # Extract & Append each DB Info into list
        for i in recvObj.unaryResponse:
            currResult = QueryResult(i.timestamp, i.dataset, i.queryStatus)
            queryResults.append(currResult)

        # QueryResults as returning object
        return QueryResults(queryResults, recvObj.queryStatus)

    def remove_db(self, dbName):
        """
        :param dbName:
        :return:
        """
        # Build msg prior to dispatch
        removeDB_REQ = XStore_Proto.removeDB_REQ()
        removeDB_REQ.rType = XStore_Proto.REMOVEDB
        removeDB_REQ.dbName = dbName

        # Dispatch request
        self.socket.send(removeDB_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.removeDB_REP()
        recvObj.ParseFromString(recvData)
        return StatusMessage(recvObj.removeStatus)

    def remove_db_files(self, filenameList, dbName):
        """
        :param filenameList:
        :param dbName:
        :return:
        """
        # Build msg prior to dispatch
        removeDBFiles_REQ = XStore_Proto.removeDBFiles_REQ()
        removeDBFiles_REQ.rType = XStore_Proto.REMOVEDBFILES
        removeDBFiles_REQ.dbName = dbName

        # Loop through each data and add to removeDBFiles_REQ
        for i in filenameList:
            # Add filename to removeDBFiles_REQ
            removeDBFiles_REQ.fileName.append(i)

        # Dispatch request
        self.socket.send(removeDBFiles_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.removeDBFiles_REP()
        recvObj.ParseFromString(recvData)
        return StatusMessages(recvObj.removeStatus)


if __name__ == '__main__':
    # # Command-line args
    SERVER_ADDR = sys.argv[1]
    SERVER_PORT = sys.argv[2]

    # XStore client object
    XStore = XStoreClient(SERVER_ADDR, SERVER_PORT)

    # SYS INFO
    sysInfo = XStore.infoRequest()
    print(sysInfo)

    # Create DB
    # createDBStatus = XStore.createDB("TSLA", "SECOND", 128)
    # print(createDBStatus)

    # Unary Insert
    # unaryInsertStatus = XStore.unaryInsert(1482271200, "From Python", "TSLA")
    # print(unaryInsertStatus)

    # Unary Query
    # unaryQueryResult = XStore.unaryQuery(1482271200, "TSLA")
    # print(unaryQueryResult)

    # Batch Insert
    #   Hypothetical list of data
    # hypData = [[1482271200, "One"], [1482271201, "Two"], [1482271202, "Three"]]
    # batchInsertStatus = XStore.batchInsert(hypData, "TSLA")
    # print(batchInsertStatus)

    # Range Query
    sTime = time.time()
    rangeQueryResult = XStore.rangeQuery(1483228798, 1483228798+1000, "TSLA")
    print(f'Took: {time.time() - sTime}')
    [print(i) for i in rangeQueryResult.queryResult]
    print(rangeQueryResult.queryStatus)

    # Remove DB
    # removeDBResult = XStore.remove_db("TSLA")
    # print(removeDBResult)

    # Remove DB Files
    # removeDBFilesResults = XStore.remove_db_files(['2016.XSTORE', '2017.XSTORE'], "TSLA")
    # print(removeDBFilesResults)
