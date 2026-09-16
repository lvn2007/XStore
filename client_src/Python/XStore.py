import os
from typing import List
import XStore_pb2 as XStore_Proto
import zmq
from zmq.utils import z85
import dataclasses


@dataclasses.dataclass
class FileInfo:
    fileName: str
    actualSizeBytes: int    # real bytes on disk (st_blocks*512, sparse-hole-aware)
    apparentSizeBytes: int  # logical/reported size (st_size); >= actualSizeBytes for a sparse file


@dataclasses.dataclass
class DBInfo:
    dbName: str
    dbResolution: str
    totalSizeInByte: int    # sum of every file's actualSizeBytes -- real disk usage of this DB
    files: List[FileInfo]
    rowLength: int          # configured per-row slot size (bytes); every row is zero-padded to this


@dataclasses.dataclass
class SysInfo:
    serverAddr: str
    threadCount: int
    dbList: List[DBInfo]


class XStoreClient:
    def __init__(self, SERVER_ADDR, SERVER_PORT, verbose=False,
                 curve_client_secret_key_file=None, curve_server_public_key_file=None):
        if verbose:
            print(f'Connecting to tcp://{SERVER_ADDR}:{SERVER_PORT}')
        self.zmqContext = zmq.Context()
        self.socket = self.zmqContext.socket(zmq.REQ)

        have_client_key = curve_client_secret_key_file is not None
        have_server_key = curve_server_public_key_file is not None
        if have_client_key != have_server_key:
            raise ValueError(
                "XStoreClient: curve_client_secret_key_file and curve_server_public_key_file "
                "must be given together, or not at all")
        if have_client_key:
            client_secret_z85 = self._read_z85_file(curve_client_secret_key_file)
            client_public_path = os.path.splitext(curve_client_secret_key_file)[0] + ".pub"
            client_public_z85 = self._read_z85_file(client_public_path)
            server_public_z85 = self._read_z85_file(curve_server_public_key_file)

            # pyzmq's curve_* socket attributes take raw 32-byte keys, unlike
            # cppzmq's sockopt wrappers which accept Z85 text directly -- decode
            # here since this on-disk format is always 40-char Z85 (see the
            # design doc's Global Constraints).
            self.socket.curve_secretkey = z85.decode(client_secret_z85.encode("ascii"))
            self.socket.curve_publickey = z85.decode(client_public_z85.encode("ascii"))
            self.socket.curve_serverkey = z85.decode(server_public_z85.encode("ascii"))

        self.socket.connect(f'tcp://{SERVER_ADDR}:{SERVER_PORT}')

    @staticmethod
    def _read_z85_file(path):
        with open(path, "r") as f:
            key = f.read().strip()
        if len(key) != 40:
            raise ValueError(f"XStoreClient: {path} does not contain a 40-character Z85 key")
        return key

    def _sendRecv(self, envelope, repClass):
        # Every RPC is: serialize the Request envelope, send, recv, parse
        # reply. The server dispatches on the envelope's oneof case
        # (payload_case()), so a bare/unwrapped request message never gets
        # routed anywhere -- it must always be the Request wrapper on the wire.
        self.socket.send(envelope.SerializeToString())
        # A streamed reply (BatchQuery/RangeQuery) arrives as several frames
        # that are pieces of ONE logical message's bytes (protobuf doesn't
        # care where a message's serialized bytes get split, only that
        # they're concatenated back in order before parsing) -- reassembled
        # here so this stays invisible to every caller above _sendRecv. Same
        # approach as XStore_Client.h's SendRecv.
        frames = self.socket.recv_multipart()
        recvData = frames[0] if len(frames) == 1 else b"".join(frames)
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
            files = [FileInfo(f.fileName, f.actualSizeBytes, f.apparentSizeBytes) for f in i.files]
            currDB = DBInfo(i.dbName, i.dbResolution, i.totalSizeInByte, files, i.rowLength)
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

    @staticmethod
    def suggestRowLength(sampleRow, alignment: int = 8) -> int:
        """
        Suggests a CreateDB rowLength/slot size from one sample RowData, so
        callers don't have to hand-guess a value. Mirrors XStore_Client.h's
        SuggestRowLength (C++) field-for-field.

        PROTOBUF_FIELD_OVERHEAD=3 is exact for every fixed-width/varint oneof
        case (double/int32/int64/etc.): 1B outer repeated-field tag
        (RowData.fieldData=1) + 1B outer length-varint (assumes the encoded
        FData stays under 128B, true for any scalar or short string) + 1B
        inner oneof tag. It undercounts stringData/bytesData by 1B: those are
        themselves length-delimited, so they need a second inner
        length-varint the fixed/varint cases don't -- same known gap as the
        C++ helper, flagged there and here, not silently fixed.
        """
        if alignment <= 0 or (alignment & (alignment - 1)) != 0:
            raise ValueError("alignment must be a power of 2")
        PROTOBUF_FIELD_OVERHEAD = 3

        rowSize = 0
        for field in sampleRow.fieldData:
            case = field.WhichOneof("oneOfData")
            if case == "stringData":
                rowSize += len(field.stringData.encode("utf-8")) + PROTOBUF_FIELD_OVERHEAD
            elif case == "bytesData":
                rowSize += len(field.bytesData) + PROTOBUF_FIELD_OVERHEAD
            elif case in ("doubleData", "int64Data", "uint64Data", "sint64Data", "fixed64Data", "sfixed64Data"):
                rowSize += 8 + PROTOBUF_FIELD_OVERHEAD
            elif case in ("floatData", "int32Data", "uint32Data", "sint32Data", "fixed32Data", "sfixed32Data"):
                rowSize += 4 + PROTOBUF_FIELD_OVERHEAD
            elif case == "boolData":
                rowSize += 1 + PROTOBUF_FIELD_OVERHEAD
            # case is None -> oneof unset, contributes 0 bytes

        return (rowSize + alignment - 1) & ~(alignment - 1)

    def unaryInsert(self, insertUnixTime: int, data: [object, list], targetDBName: str,
                     durabilityMode=XStore_Proto.ASYNC) -> str:
        envelope = XStore_Proto.Request()
        payload = envelope.unaryinsert
        payload.RType = XStore_Proto.UNARYINSERT
        payload.timestamp = insertUnixTime
        payload.dbName = targetDBName
        payload.durabilityMode = durabilityMode

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

    def rangeInsert(self, dataList, targetDBName, durabilityMode=XStore_Proto.ASYNC):
        """
        :param dataList:
        :param targetDBName:
        :param durabilityMode: same WAL-routed semantics as UnaryInsert's --
            see RangeInsert_REQ's durabilityMode comment in XStore.proto.
            SYNC blocks until the write is durably fsync'd and applied;
            ASYNC (the default) enqueues and returns immediately, deferring
            both the fsync AND the data-file write itself to the server's
            WAL writer thread -- a read immediately after an ASYNC
            rangeInsert is not guaranteed to see it yet.
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.rangeinsert
        payload.RType = XStore_Proto.RANGEINSERT
        payload.dbName = targetDBName
        payload.durabilityMode = durabilityMode

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

    def batchInsert(self, dataList, targetDBName, durabilityMode=XStore_Proto.ASYNC):
        """
        :param dataList:
        :param targetDBName:
        :param durabilityMode: same WAL-routed semantics as UnaryInsert's --
            see BatchInsert_REQ's durabilityMode comment in XStore.proto.
            SYNC blocks until the write is durably fsync'd and applied;
            ASYNC (the default) enqueues and returns immediately, deferring
            both the fsync AND the data-file write itself to the server's
            WAL writer thread -- a read immediately after an ASYNC
            batchInsert is not guaranteed to see it yet.
        :return:
        """
        envelope = XStore_Proto.Request()
        payload = envelope.batchinsert
        payload.RType = XStore_Proto.BATCHINSERT
        payload.dbName = targetDBName
        payload.durabilityMode = durabilityMode

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

    def variance(self, startUnixTime, endUnixTime, targetDBName, columnIdx):
        """
        :return: Variance_REP (opResult double, opStatus)
        """
        return self._aggregate(XStore_Proto.VARIANCE, "variance", XStore_Proto.Variance_REP,
                                startUnixTime, endUnixTime, targetDBName, columnIdx)

    def stddev(self, startUnixTime, endUnixTime, targetDBName, columnIdx):
        """
        :return: Stddev_REP (opResult double, opStatus)
        """
        return self._aggregate(XStore_Proto.STDDEV, "stddev", XStore_Proto.Stddev_REP,
                                startUnixTime, endUnixTime, targetDBName, columnIdx)
