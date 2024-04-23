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


class XStoreClient:
    def __init__(self, SERVER_ADDR, SERVER_PORT):
        print(f'Connecting to tcp://{SERVER_ADDR}:{SERVER_PORT}')
        self.zmqContext = zmq.Context()
        self.socket = self.zmqContext.socket(zmq.REQ)
        self.socket.connect(f'tcp://{SERVER_ADDR}:{SERVER_PORT}')

    def infoQuery(self):
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
        return recvObj.createDBStatus

    def unaryInsert(self, insertUnixTime: int, data: [object, list], targetDBName: str) -> str:
        # Build msg prior to dispatch
        unaryInsert_REQ = XStore_Proto.unaryInsert_REQ()
        unaryInsert_REQ.rType = XStore_Proto.UNARYINSERT
        unaryInsert_REQ.timestamp = insertUnixTime
        unaryInsert_REQ.dbName = targetDBName

        # IF data is list -> Assume worse case scenario for space-efficiency
        if isinstance(data, list):
            # Build 1 row worth of data
            rowData = XStore_Proto.rowData()
            for idx, item in enumerate(data):
                fieldData = rowData.fData.add()
                if isinstance(item, str):
                    fieldData.stringData = item
                elif isinstance(item, bool):
                    fieldData.boolData = item
                elif isinstance(item, (int, float)):
                    fieldData.doubleData = item
            unaryInsert_REQ.data.MergeFrom(rowData)
        else:
            unaryInsert_REQ.data.MergeFrom(data)

        # Dispatch request
        self.socket.send(unaryInsert_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.unaryInsert_REP()
        recvObj.ParseFromString(recvData)
        return recvObj.insertStatus

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
        return recvObj

    def unaryDelete(self, targetUnixTime, targetDBName):
        # Build msg prior to dispatch
        unaryDelete_REQ = XStore_Proto.unaryDelete_REQ()
        unaryDelete_REQ.rType = XStore_Proto.UNARYDELETE
        unaryDelete_REQ.timestamp = targetUnixTime
        unaryDelete_REQ.dbName = targetDBName

        # Dispatch request
        self.socket.send(unaryDelete_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.unaryDelete_REP()
        recvObj.ParseFromString(recvData)
        return recvObj.deleteStatus

    def rangeInsert(self, dataList, targetDBName):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        # Build msg prior to dispatch
        rangeInsert_REQ = XStore_Proto.rangeInsert_REQ()
        rangeInsert_REQ.rType = XStore_Proto.RANGEINSERT
        rangeInsert_REQ.dbName = targetDBName

        # IF first element in dataList is not type rangeInsert_DATA
        #   -> Inferred data type -- Assume worse case scenario for space-efficiency
        if type(dataList[0]) != XStore_Proto.rangeInsert_DATA:
            # Build 1 row worth of data
            for data in dataList:
                rowData = XStore_Proto.rowData()
                for idx, item in enumerate(data["data"]):
                    if isinstance(item, str):
                        rowData.fieldData.add().stringData = item
                    elif isinstance(item, bool):
                        rowData.fieldData.add().boolData = item
                    elif isinstance(item, (int, float)):
                        rowData.fieldData.add().doubleData = item
                insertData = XStore_Proto.rangeInsert_DATA()
                insertData.timestamp = data["timestamp"]
                insertData.data.MergeFrom(rowData)
                rangeInsert_REQ.rangeInsert_DATA.append(insertData)
        else:
            rangeInsert_REQ.rangeInsert_DATA.MergeFrom(dataList)

        # Dispatch request
        self.socket.send(rangeInsert_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.rangeInsert_REP()
        recvObj.ParseFromString(recvData)
        return recvObj.insertStatus

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

        # IF first element in dataList is not type batchInsert_DATA
        #   -> Inferred data type -- Assume worse case scenario for space-efficiency
        if type(dataList[0]) != XStore_Proto.batchInsert_DATA:
            # Build 1 row worth of data
            for data in dataList:
                rowData = XStore_Proto.rowData()
                for idx, item in enumerate(data["data"]):
                    if isinstance(item, str):
                        rowData.fieldData.add().stringData = item
                    elif isinstance(item, bool):
                        rowData.fieldData.add().boolData = item
                    elif isinstance(item, (int, float)):
                        rowData.fieldData.add().doubleData = item
                insertData = XStore_Proto.batchInsert_DATA()
                insertData.timestamp = data["timestamp"]
                insertData.data.MergeFrom(rowData)
                batchInsert_REQ.batchInsert_DATA.append(insertData)
        else:
            batchInsert_REQ.batchInsert_DATA.MergeFrom(dataList)

        # Dispatch request
        self.socket.send(batchInsert_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.batchInsert_REP()
        recvObj.ParseFromString(recvData)
        return recvObj.insertStatus

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
        recvObj = XStore_Proto.rangeQuery_REP()
        recvObj.ParseFromString(recvData)

        # List of dbInfo
        # queryResults = []

        # Extract & Append each DB Info into list
        # for i in recvObj.unaryResponse:
        #     currResult = QueryResult(i.timestamp, i.dataset, i.queryStatus)
        #     queryResults.append(currResult)

        # QueryResults as returning object
        # return QueryResults(queryResults, recvObj.queryStatus)
        return recvObj

    def batchQuery(self, dataList, targetDBName):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        # Build msg prior to dispatch
        batchQuery_REQ = XStore_Proto.batchQuery_REQ()
        batchQuery_REQ.rType = XStore_Proto.BATCHQUERY
        batchQuery_REQ.dbName = targetDBName

        # Loop through each data and add to batchQuery_REQ
        for targetTime in dataList:
            # Organize batchInsert_DATA
            batchQuery_REQ.timestamp.append(targetTime)

        # Dispatch request
        self.socket.send(batchQuery_REQ.SerializeToString())

        # Receive response
        recvData = self.socket.recv()

        # Prepare object to be parsed into
        recvObj = XStore_Proto.batchQuery_REP()
        recvObj.ParseFromString(recvData)
        return recvObj.queryResponse

    def removeDB(self, dbName):
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
        return recvObj.removeStatus

    def removeDB_files(self, filenameList, dbName):
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
        return recvObj.removeStatus