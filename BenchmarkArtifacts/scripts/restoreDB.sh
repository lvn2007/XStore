SITE_NAME="$1"
DB_NAME="$2"
DATASET="$3"
WORKING_DIR="$4"

DATA_DIR="XStore_ExperimentDataset"

if [ ${DATASET} == "BACKTESTER" ]; then
    DATA_DIR="XStore_BacktestDataset"
fi

urlTACC="https://chi.tacc.chameleoncloud.org:7480/swift/v1/AUTH_07e3c8c4081143c189d399c676ad139f/${DATA_DIR}/${DB_NAME}_${DATASET}.tar.zst"
urlMystic="http://172.16.6.231:8000/${DATA_DIR}/${DB_NAME}_${DATASET}.tar.zst"

# Default download URL to TACC site
defaultURL=${urlTACC}

if [ "$SITE_NAME" == "CHI@TACC" ]; then
    defaultURL=${urlTACC}
elif [ "$SITE_NAME" == "MYSTIC@IIT" ]; then
    defaultURL=${urlMystic}
fi

cd ${WORKING_DIR}

# Intialize tmux
tmux new-session -d -s XStore -n RestoreDB

if [ "$DB_NAME" == "XStore" ]; then
    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvS --zstd && sudo rm -rf XStore_Server/build/DataStore/${DATASET} && mv ${DATASET} XStore_Server/build/DataStore && tmux kill-server" C-m

elif [ "$DB_NAME" == "MongoDB" ]; then
    # Purge existing dataStore
    mongosh --port 9491 quiet --eval "use ${DATASET}" --eval "db.dropDatabase()"

    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvSO --zstd | mongorestore --host 0.0.0.0 --port 9491 --nsInclude=${DATASET}.${DATASET} --archive --drop && tmux kill-server" C-m
    
elif [ "$DB_NAME" == "InfluxDB" ]; then
    # Purge existing dataStore
    influx -port 9492 -execute "DROP DATABASE ${DATASET}"
    
    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvS --zstd && influxd restore -portable InfluxDB_${DATASET} && sudo rm -r InfluxDB_${DATASET} && tmux kill-server" C-m
    
elif [ "$DB_NAME" == "Graphite" ]; then
    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvS --zstd && sudo rm -rf graphite/whisper/${DATASET} && mv ${DATASET} graphite/whisper && tmux kill-server" C-m

elif [ "$DB_NAME" == "CSV" ] || [ "$DB_NAME" == "Parquet" ]; then
    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvS --zstd && tmux kill-server" C-m
fi
















