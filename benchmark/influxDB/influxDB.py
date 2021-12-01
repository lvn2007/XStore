#from influxdb import InfluxDBClient
from influxdb import InfluxDBClient
from datetime import datetime
import time
from os import system
import random
from statistics import stdev

client = InfluxDBClient('localhost', 8086)
allDB = client.get_list_database()
print(allDB)

# Creating new DB & Switch to use that DB
# benchDB = client.create_database('benchmark')
client.switch_database('benchmark')

# Iterate from 01/01/2017 12:00:01 AM to 12/31/2017 11:59:59
startDate = 1483228801
endDate = 1514764799
firstDate = startDate
lastDate = 0

# for i in range(endDate - startDate):
# 	json_payload = []
# 	data = {
# 		"measurement": "stocks",
# 		"tags": {
# 			"ticker": "TSLA"
# 		},
# 		"time": datetime.fromtimestamp(startDate + i),
# 		"fields": {
# 			"Col1": startDate + i,
# 			"Col2": "NaN",
# 			"Col3": "NaN",
# 			"Col4": "NaN",
# 			"Col5": "NaN",
# 			"Col6": "NaN",
# 			"Col7": "NaN"
# 		}
# 	}
# 	json_payload.append(data)
# 	client.write_points(json_payload)

searchTime = []

for i in range(1000):
	system("free > /dev/null")
	system("sync > /dev/null")
	system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'")
	system("free > /dev/null")

	targetTime = random.randint(startDate, endDate)
	ts = datetime.fromtimestamp(targetTime)

	startTime = time.time()
	thisGet = client.query(f"SELECT * FROM stocks WHERE time >= '{ts}' AND time < '{datetime.fromtimestamp(targetTime + 1)}'")
	# thisGet = client.query("SELECT * FROM benchmark")
	endTime = time.time()

	searchTime.append(endTime - startTime)

myMin = min(searchTime)
myMax = max(searchTime)
myAvg = sum(searchTime) / len(searchTime)
myStdev = stdev(searchTime)

print('--- EXECUTION TIME in [s] ---')
print(f' -Min: {myMin}')
print(f' -Max: {myMax}')
print(f' -Mean: {myAvg}')
print(f' -Stdev: {myStdev}')