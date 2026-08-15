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

    def _sendRecv(self, envelope, repClass):
        # Every RPC is: serialize the Request envelope, send, recv, parse
        # reply. The server dispatches on the envelope's oneof case
        # (payload_case()), so a bare/unwrapped request message never gets
        # routed anywhere -- it must always be the Request wrapper on the wire.
        self.socket.send(envelope.SerializeToString())
        recvData = self.socket.recv()
        recvObj = repClass()
        recvObj.ParseFromString(recvData)
        return recvObj

    def ping(self):
        """
        Bare round trip, no server-side XStore work -- cheaper than
        infoQuery() for warmup/liveness checks.
        """
        envelope = XStore_Proto.Request()
        envelope.ping.RType = XStore_Proto.PING
        self._sendRecv(envelope, XStore_Proto.Ping_REP)

    def infoQuery(self):
        """
        :return: Nothing
        """
        envelope = XStore_Proto.Request()
        envelope.info.RType = XStore_Proto.INFO
        recvObj = self._sendRecv(envelope, XStore_Proto.SysInfo_REP)

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
        envelope = XStore_Proto.Request()
        payload = envelope.createdb
        payload.RType = XStore_Proto.CREATEDB
        payload.dbName = dbName
        payload.timeResolution = timeResolution
        payload.rowLength = rowLength

        recvObj = self._sendRecv(envelope, XStore_Proto.CreateDB_REP)
        return recvObj.createDBStatus

    def unaryInsert(self, insertUnixTime: int, data: [object, list], targetDBName: str) -> str:
        envelope = XStore_Proto.Request()
        payload = envelope.unaryinsert
        payload.RType = XStore_Proto.UNARYINSERT
        payload.timestamp = insertUnixTime
        payload.dbName = targetDBName

        # IF data is list -> Assume worse case scenario for space-efficiency
        if isinstance(data, list):
            # Build 1 row worth of data
            rowData = XStore_Proto.RowData()
            for idx, item in enumerate(data):
                fieldData = rowData.fieldData.add()
                if isinstance(item, str):
                    fieldData.stringData = item
                elif isinstance(item, bool):
                    fieldData.boolData = item
                elif isinstance(item, (int, float)):
                    fieldData.doubleData = item
            payload.data.MergeFrom(rowData)
        else:
            payload.data.MergeFrom(data)

        recvObj = self._sendRecv(envelope, XStore_Proto.UnaryInsert_REP)
        return recvObj.insertStatus

    def unaryQuery(self, targetUnixTime, targetDBName):
        """
        :param targetUnixTime:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.unaryquery
        payload.RType = XStore_Proto.UNARYQUERY
        payload.timestamp = targetUnixTime
        payload.dbName = targetDBName

        return self._sendRecv(envelope, XStore_Proto.UnaryQuery_REP)

    def unaryDelete(self, targetUnixTime, targetDBName):
        envelope = XStore_Proto.Request()
        payload = envelope.unarydelete
        payload.RType = XStore_Proto.UNARYDELETE
        payload.timestamp = targetUnixTime
        payload.dbName = targetDBName

        recvObj = self._sendRecv(envelope, XStore_Proto.UnaryDelete_REP)
        return recvObj.deleteStatus

    def rangeInsert(self, dataList, targetDBName):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.rangeinsert
        payload.RType = XStore_Proto.RANGEINSERT
        payload.dbName = targetDBName

        # IF first element in dataList is not type rangeInsert_DATA
        #   -> Inferred data type -- Assume worse case scenario for space-efficiency
        if type(dataList[0]) != XStore_Proto.RangeInsert_DATA:
            # Build 1 row worth of data
            for data in dataList:
                rowData = XStore_Proto.RowData()
                for idx, item in enumerate(data["data"]):
                    if isinstance(item, str):
                        rowData.fieldData.add().stringData = item
                    elif isinstance(item, bool):
                        rowData.fieldData.add().boolData = item
                    elif isinstance(item, (int, float)):
                        rowData.fieldData.add().doubleData = item
                insertData = XStore_Proto.RangeInsert_DATA()
                insertData.timestamp = data["timestamp"]
                insertData.data.MergeFrom(rowData)
                payload.rangeInsert_DATA.append(insertData)
        else:
            payload.rangeInsert_DATA.MergeFrom(dataList)

        recvObj = self._sendRecv(envelope, XStore_Proto.RangeInsert_REP)
        return recvObj.insertStatus

    def batchInsert(self, dataList, targetDBName):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.batchinsert
        payload.RType = XStore_Proto.BATCHINSERT
        payload.dbName = targetDBName

        # IF first element in dataList is not type batchInsert_DATA
        #   -> Inferred data type -- Assume worse case scenario for space-efficiency
        if type(dataList[0]) != XStore_Proto.BatchInsert_DATA:
            # Build 1 row worth of data
            for data in dataList:
                rowData = XStore_Proto.RowData()
                for idx, item in enumerate(data["data"]):
                    if isinstance(item, str):
                        rowData.fieldData.add().stringData = item
                    elif isinstance(item, bool):
                        rowData.fieldData.add().boolData = item
                    elif isinstance(item, (int, float)):
                        rowData.fieldData.add().doubleData = item
                insertData = XStore_Proto.BatchInsert_DATA()
                insertData.timestamp = data["timestamp"]
                insertData.data.MergeFrom(rowData)
                payload.batchInsert_DATA.append(insertData)
        else:
            payload.batchInsert_DATA.MergeFrom(dataList)

        recvObj = self._sendRecv(envelope, XStore_Proto.BatchInsert_REP)
        return recvObj.insertStatus

    def rangeQuery(self, startUnixTime, endUnixTime, targetDBName):
        """
        :param startUnixTime:
        :param endUnixTime:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.rangequery
        payload.RType = XStore_Proto.RANGEQUERY
        payload.startTime = startUnixTime
        payload.endTime = endUnixTime
        payload.dbName = targetDBName

        return self._sendRecv(envelope, XStore_Proto.RangeQuery_REP)

    def batchQuery(self, dataList, targetDBName, SortTime=False):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.batchquery
        payload.RType = XStore_Proto.BATCHQUERY
        payload.dbName = targetDBName
        payload.SortTime = SortTime

        # Loop through each data and add to batchquery payload
        for targetTime in dataList:
            payload.timestamp.append(targetTime)

        recvObj = self._sendRecv(envelope, XStore_Proto.BatchQuery_REP)
        return recvObj.queryResponse

    def removeDB(self, dbName):
        """
        :param dbName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.removedb
        payload.RType = XStore_Proto.REMOVEDB
        payload.dbName = dbName

        recvObj = self._sendRecv(envelope, XStore_Proto.RemoveDB_REP)
        return recvObj.removeStatus

    def removeDB_files(self, filenameList, dbName):
        """
        :param filenameList:
        :param dbName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.removedbfiles
        payload.RType = XStore_Proto.REMOVEDBFILES
        payload.dbName = dbName

        # Loop through each data and add to removedbfiles payload
        for i in filenameList:
            payload.fileName.append(i)

        recvObj = self._sendRecv(envelope, XStore_Proto.RemoveDBFiles_REP)
        return recvObj.removeStatus

    def batchDelete(self, dataList, targetDBName):
        """
        :param dataList:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.batchdelete
        payload.RType = XStore_Proto.BATCHDELETE
        payload.dbName = targetDBName

        # Loop through each data and add to batchdelete payload
        for targetTime in dataList:
            payload.timestamp.append(targetTime)

        recvObj = self._sendRecv(envelope, XStore_Proto.BatchDelete_REP)
        return recvObj.deleteStatus

    def rangeDelete(self, startUnixTime, endUnixTime, targetDBName):
        """
        :param startUnixTime:
        :param endUnixTime:
        :param targetDBName:
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.rangedelete
        payload.RType = XStore_Proto.RANGEDELETE
        payload.startTime = startUnixTime
        payload.endTime = endUnixTime
        payload.dbName = targetDBName

        recvObj = self._sendRecv(envelope, XStore_Proto.RangeDelete_REP)
        return recvObj.deleteStatus

    def _aggregate(self, reqType, oneofName, repClass, startUnixTime, endUnixTime, targetDBName, columnIdx):
        # Shared by min/max/sum/avg -- same (startTime, endTime, dbName,
        # columnIdx) request shape, differing only in RequestType and the
        # oneof field / generated REP class to use.
        envelope = XStore_Proto.Request()
        payload = getattr(envelope, oneofName)
        payload.RType = reqType
        payload.startTime = startUnixTime
        payload.endTime = endUnixTime
        payload.dbName = targetDBName
        payload.columnIdx = columnIdx

        return self._sendRecv(envelope, repClass)

    def min(self, startUnixTime, endUnixTime, targetDBName, columnIdx):
        """
        :return: Min_REP (opResult FData oneof, opStatus)
        """
        return self._aggregate(XStore_Proto.MIN, "min", XStore_Proto.Min_REP,
                                startUnixTime, endUnixTime, targetDBName, columnIdx)

    def max(self, startUnixTime, endUnixTime, targetDBName, columnIdx):
        """
        :return: Max_REP (opResult FData oneof, opStatus)
        """
        return self._aggregate(XStore_Proto.MAX, "max", XStore_Proto.Max_REP,
                                startUnixTime, endUnixTime, targetDBName, columnIdx)

    def sum(self, startUnixTime, endUnixTime, targetDBName, columnIdx):
        """
        :return: Sum_REP (opResult double, opStatus)
        """
        return self._aggregate(XStore_Proto.SUM, "sum", XStore_Proto.Sum_REP,
                                startUnixTime, endUnixTime, targetDBName, columnIdx)

    def avg(self, startUnixTime, endUnixTime, targetDBName, columnIdx):
        """
        :return: Avg_REP (opResult double, opStatus)
        """
        return self._aggregate(XStore_Proto.AVG, "avg", XStore_Proto.Avg_REP,
                                startUnixTime, endUnixTime, targetDBName, columnIdx)
