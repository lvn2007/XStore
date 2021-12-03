from pymongo import MongoClient
from datetime import datetime
import time
from os import system
import random
from statistics import stdev

client = MongoClient('localhost', 27017)
allDB = client.list_database_names()
print(allDB)

# Creating new DB & Collection
benchDB = client['benchTS']
benchCol = benchDB['benchTS']

# Iterate from 01/01/2017 12:00:01 AM to 12/31/2017 11:59:59
startDate = 1483228801
endDate = 1514764799
firstDate = startDate
lastDate = 0

# for i in range(endDate - startDate):
# 	currData = {'Timestamp': datetime.fromtimestamp(startDate + i), 'Col1': f'{datetime.fromtimestamp(startDate + i)}', 'Col2': 'NaN', 'Col3': 'NaN', 'Col4': 'NaN', 'Col5': 'NaN', 'Col6': 'NaN', 'Col7': 'NaN'}
# 	benchCol.insert_one(currData)

searchTime = []

for i in range(1000):
	system("free > /dev/null")
	system("sync > /dev/null")
	system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'")
	system("free > /dev/null")

	target = random.randint(startDate, endDate)
	startTime = time.time()
	thisGet = benchCol.find_one({'Timestamp': datetime.fromtimestamp(target)})
	endTime = time.time()

	searchTime.append(endTime - startTime)

	# print(thisGet)
	# print(f'Query took: {endTime - startTime} (s)')

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