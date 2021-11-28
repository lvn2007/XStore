from pymongo import MongoClient
import time

client = MongoClient('localhost', 27017)
allDB = client.list_database_names()
print(allDB)

# Creating new DB & Collection
benchDB = client['benchDB']
benchCol = benchDB['benchmark']

# Iterate from 01/01/2017 12:00:01 AM to 12/31/2017 11:59:59
startDate = 1483228801
endDate = 1514764799
firstDate = startDate
lastDate = 0

#for i in range(endDate - startDate):
#	currData = {'Timestamp': startDate + i, 'Col1': 'NaN', 'Col2': 'NaN', 'Col3': 'NaN', 'Col4': 'NaN', 'Col5': 'NaN', 'Col6': 'NaN', 'Col7': 'NaN'}
#	benchCol.insert_one(currData)

#lastDate = startDate + i

#print(firstDate)
#print(lastDate)

# Insert dummy
#currData = {'Timestamp': '1504224000', 'Open': 1, 'High': 1, 'Low': 1, 'Close': 1}
#row = benchCol.insert_one(currData)

startTime = time.time()
thisGet = benchCol.find_one({'Timestamp': 1505718000})
endTime = time.time()

print(thisGet)
print(f'Query took: {endTime - startTime} (s)')