isPurge="$1"
WORKING_DIR="$2"

# Purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    influx -port 9492 -execute "DROP DATABASE BENCH_DB"
fi

# Stop InfluxDB
sudo service influxdb stop

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Start InfluxDB
sudo service influxdb start && sudo service influxdb restart

# Delay to make sure server has enough time to be up 
sleep 10