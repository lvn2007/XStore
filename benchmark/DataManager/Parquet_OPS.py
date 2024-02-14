#!/usr/bin/env python3

import random
import sys
import argparse
import hashlib
import pandas as pd
import numpy as np
import os
from fastparquet import ParquetFile
import json

# pd.set_option('display.max_columns', None)
# pd.set_option('expand_frame_repr', False)


class ParquetOPS:
    def __init__(self):
        self.df = self.numRows = self.numGroups = self.rowsPerGroup = None

    def __atRowGroup(self, targetIdx):
        currMin = 0

        for i in range(self.numGroups):
            if currMin <= targetIdx < (self.rowsPerGroup * (i + 1)):
                return i
            currMin += self.rowsPerGroup

    def __segmentIdx(self, idxList):
        toReturn = {}
        for i in idxList:
            atRowGroup = self.__atRowGroup(i)
            if atRowGroup not in toReturn:
                toReturn[atRowGroup] = [i]
            else:
                toReturn[atRowGroup].append(i)
        return toReturn

    def __extractDataFromIdx(self, targetList):
        # Segment index into separate group to visit row group once
        segmentedIdxList = self.__segmentIdx(targetList)

        toWrite = pd.DataFrame()

        for rowGroupIdx in segmentedIdxList.keys():
            currIdxList = segmentedIdxList[rowGroupIdx]
            tempDF = self.df[rowGroupIdx].to_pandas()
            tempDF = tempDF.set_index(np.int64(tempDF.index + (self.rowsPerGroup * rowGroupIdx)))

            for item in currIdxList:
                toWrite = pd.concat([toWrite, tempDF.loc[[item]]])
        return toWrite.reset_index(drop=True)

    def genData(self, startTime, endTime, columnCount, outfileName):
        # Check if there's an existing data file
        if os.path.isfile(outfileName):
            print(f'[ERROR - GEN DATA] -- {outfileName} is already existed')
            exit(-1)

        if 'parquet' not in outfileName.lower():
            outfileName += '.parquet'

        columnLabels = [f'col{i}' for i in range(columnCount)]
        columnLabels.insert(0, 'timestamp')
        data = []

        for timeIdx in range(startTime, endTime + 1):
            temp = []
            # First column will always be timestamp
            temp.append(timeIdx)

            # Create hash data for each column
            for i in range(columnCount):
                temp.append(hashlib.md5(f'{timeIdx}-{i}'.encode()).hexdigest())

            # Append to master list
            data.append(temp)

            # Dumps to CSV every 1M rows
            if len(data) == 1_000_000:
                df = pd.DataFrame(data, columns=columnLabels)
                if not os.path.isfile(outfileName):
                    df.to_parquet(outfileName, engine='fastparquet', compression='zstd')
                else:
                    df.to_parquet(outfileName, engine='fastparquet', compression='zstd', append=True)

                # Reset master list
                data = []

        # Dumps remaining data to CSV
        df = pd.DataFrame(data, columns=columnLabels)

        if not os.path.isfile(outfileName):
            df.to_parquet(outfileName, engine='fastparquet', compression='zstd')
        else:
            df.to_parquet(outfileName, engine='fastparquet', compression='zstd', append=True)

    def createBasicQuery(self, offset, sampleSize, nClients, startTime, endTime, queryType='QUERY_A', batchIter=None):
        # Batch size default
        if batchIter is None:
            batchIter = 10

        if queryType == 'SEQ':
            for i in range(nClients):
                startTime = (startTime + offset) + (i * sampleSize)

                # Sequential
                targetList = [i for i in range(startTime, startTime + sampleSize)]

                # Prepare DF to write
                toWrite = pd.DataFrame(targetList, columns=['timestamp'])
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                toWrite.to_parquet(f'BASIC-QUERY-SEQ_Client-{i}_{sampleSize}_{startTime}.parquet', engine='fastparquet', compression='zstd')

        elif queryType == 'RAND':
            # Data for all N clients
            randData = random.sample(range(startTime, endTime), nClients * sampleSize)

            # Segment data for each client
            for i in range(nClients):
                currStart = i * sampleSize
                currEnd = (i * sampleSize) + sampleSize
                targetList = randData[currStart:currEnd]

                # Prepare DF to write
                toWrite = pd.DataFrame(targetList, columns=['timestamp'])
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                toWrite.to_parquet(f'BASIC-QUERY-RAND_Client-{i}_{sampleSize}_{startTime}-{endTime}.parquet', engine='fastparquet', compression='zstd')

        elif queryType == 'RANGE':
            for i in range(nClients):
                startTime = (startTime + offset) + (i * sampleSize * batchIter)

                sTime = [startTime + (j * sampleSize) for j in range(batchIter)]
                eTime = [j + sampleSize for j in sTime]

                # Prepare DF to write
                toWrite = pd.DataFrame(zip(sTime, eTime), columns=['startTime', 'endTime'])
                toWrite['startTime'] = toWrite['startTime'].astype(np.uint64)
                toWrite['endTime'] = toWrite['endTime'].astype(np.uint64)

                # Write to file
                toWrite.to_parquet(f'BASIC-QUERY-RANGE_Client-{i}_{batchIter}-{sampleSize}_{startTime}-{endTime}.parquet', engine='fastparquet', compression='zstd')

        elif queryType == 'BATCH':
            # Data for all N clients
            randData = random.sample(range(startTime, endTime), nClients * sampleSize * batchIter)

            # Segment data for each client
            for i in range(nClients):
                thisStart = [(i * sampleSize) + (t * sampleSize) for t in range(batchIter)]
                thisEnd = [t + sampleSize for t in thisStart]

                targetList = [randData[j[0]:j[1]] for j in zip(thisStart, thisEnd)]

                # Prepare DF to write
                toWrite = pd.DataFrame(targetList)
                toWrite = toWrite.astype(np.uint64)
                toWrite.columns = toWrite.columns.astype(str)

                # Write to file
                toWrite.to_parquet(f'BASIC-QUERY-BATCH_Client-{i}_{batchIter}-{sampleSize}_{startTime}-{endTime}.parquet', engine='fastparquet', compression='zstd')

    def createBasicInsert(self, offset, sampleSize, nClients, inputFile, insertType='SEQ', batchIter=None):
        if batchIter is None:
            batchIter = 10

        # Read parquet file
        self.df = ParquetFile(inputFile)

        # Parquet metadata
        self.numRows = self.df.count()
        self.numGroups = len(self.df.row_groups)
        self.rowsPerGroup = self.df[0].count()

        if insertType == 'SEQ':
            for i in range(nClients):
                startRange = offset + (i * sampleSize)
                targetList = [i for i in range(startRange, startRange + sampleSize)]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)

                # Write to file
                toWrite.to_parquet(f'BASIC-INSERT-SEQ_Client-{i}_{startRange}.parquet', engine='fastparquet', compression='zstd')

        elif insertType == 'RAND':
            # Random indexes for all N clients
            randData = random.sample(range(0, self.numRows), nClients * sampleSize)

            for i in range(nClients):
                currStart = i * sampleSize
                currEnd = (i * sampleSize) + sampleSize
                targetList = randData[currStart:currEnd]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)

                # Write to file
                toWrite.to_parquet(f'BASIC-INSERT-RAND_Client-{i}_{0}-{self.numRows}.parquet', engine='fastparquet', compression='zstd')

        elif insertType == 'RANGE':
            for i in range(nClients):
                startRange = offset + (i * sampleSize * batchIter)
                endRange = startRange + (sampleSize * batchIter)

                targetList = [i for i in range(startRange, endRange)]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)

                # Write to file
                toWrite.to_parquet(f'BASIC-INSERT-RANGE_Client-{i}_{0}-{startRange}-{endRange}.parquet', engine='fastparquet', compression='zstd')

        elif insertType == 'BATCH':
            # Random indexes timestamp for all N clients
            randData = random.sample(range(0, self.numRows), nClients * sampleSize * batchIter)

            for i in range(nClients):
                startRange = i * sampleSize * batchIter
                endRange = startRange + (sampleSize * batchIter)

                targetList = [j for j in randData[startRange:endRange]]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)

                # Write to file
                toWrite.to_parquet(f'BASIC-INSERT-BATCH_Client-{i}_{0}-{startRange}-{endRange}.parquet', engine='fastparquet', compression='zstd')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='XBench Data Utilities')

    # Data generator
    subParser = parser.add_subparsers(title='commands', dest='command')
    dataGenerator = subParser.add_parser('generate', add_help=False, description='Synthetic data generator for benchmarking suite')
    dataGenerator.add_argument('-s', '--start-time', type=int, required=True, help='Start time in Epoch time format')
    dataGenerator.add_argument('-e', '--end-time', type=int, required=True, help='End time in Epoch time format')
    dataGenerator.add_argument('-c', '--column-count', type=int, required=True, help='Number of columns')
    dataGenerator.add_argument('-o', '--outfile_name', type=str, required=True, help='Name of the generated Parquet data file (With .parquet extension)')

    # Data preparer
    #   READ workload
    readWorkload = subParser.add_parser('queryWorkload', add_help=True, description='Data preparer for READ workloads')
    readWorkload.add_argument('-t', '--type', type=str, required=True, help='Workload type', choices=['SEQ', 'RAND', 'RANGE', 'BATCH'])
    readWorkload.add_argument('-o', '--offset', type=int, required=True, help='Offset/Skip N timestamps')
    readWorkload.add_argument('-s', '--sample-size', type=int, required=True, help='Number of timestamps')
    readWorkload.add_argument('-n', '--num-client', type=int, required=True, help='Number of client (Each client has its own input data file)')
    readWorkload.add_argument('-st', '--start-time', type=int, required=True, help='Start range of epoch time')
    readWorkload.add_argument('-et', '--end-time', type=int, required=True, help='End range of epoch time')
    readWorkload.add_argument('-b', '--batch-iter', type=int, required=False, help='Batch iteration (Only applicable to RANGE/BATCH workloads')

    #   WRITE workload
    writeWorkload = subParser.add_parser('insertWorkload', add_help=True, description='Data preparer for WRITE workloads')
    writeWorkload.add_argument('-t', '--type', type=str, required=True, help='Workload type', choices=['SEQ', 'RAND', 'RANGE', 'BATCH'])
    writeWorkload.add_argument('-o', '--offset', type=int, required=True, help='Offset/Skip N number of rows')
    writeWorkload.add_argument('-s', '--sample-size', type=int, required=True, help='Number of rows')
    writeWorkload.add_argument('-n', '--num-client', type=int, required=True, help='Number of client (Each client has its own input data file)')
    writeWorkload.add_argument('-i', '--input-data', type=str, required=True, help='Input parquet file')
    writeWorkload.add_argument('-b', '--batch-iter', type=int, required=False, help='Batch iteration (Only applicable to RANGE/BATCH workloads')

    # If no argument given -> Print help msg
    if len(sys.argv) == 1:
        parser.print_help()
        exit(1)

    # Parsing args
    args = parser.parse_args()
    parquetOps = ParquetOPS()

    # HANDLE data generator
    if args.command == 'generate':
        parquetOps.genData(args.start_time, args.end_time, args.column_count, args.outfile_name)

    # BASIC QUERIES
    elif args.command == 'queryWorkload':
        parquetOps.createBasicQuery(offset=args.offset, sampleSize=args.sample_size, nClients=args.num_client, startTime=args.start_time, endTime=args.end_time, queryType=args.type, batchIter=args.batch_iter)

    # BASIC ISNERTS
    elif args.command == 'insertWorkload':
        parquetOps.createBasicInsert(offset=args.offset, sampleSize=args.sample_size, nClients=args.num_client, inputFile=args.input_data, insertType=args.type, batchIter=args.batch_iter)