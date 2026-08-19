isPurge="$1"
WORKING_DIR="$2"

# Purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    mongosh --port 9491 quiet --eval "use BENCH_DB" --eval "db.dropDatabase()"
    mongosh --port 9491 quiet --eval "use BACKTESTER" --eval "db.dropDatabase()"
fi

# Stop MongoDB
sudo systemctl stop mongod

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Start MongoDB
sudo systemctl start mongod && sudo systemctl restart mongod

# Delay to make sure server has enough time to be up 
sleep 10

# Limit cache at runtime
# mongosh --port 9491 quiet --eval "db.adminCommand({setParameter: 1, 'wiredTigerEngineRuntimeConfig': 'cache_size=1M'})"