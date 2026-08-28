source "$(dirname "${BASH_SOURCE[0]}")/../_common.sh"

isPurge="$1"
WORKING_DIR="$2"

# Purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    influx -port 9492 -execute "DROP DATABASE BENCH_DB"
    influx -port 9492 -execute "DROP DATABASE BACKTESTER"
fi

# Stop InfluxDB
sudo service influxdb stop

dropPageCache

# Start InfluxDB
sudo service influxdb start && sudo service influxdb restart

# Delay to make sure server has enough time to be up 
sleep 10