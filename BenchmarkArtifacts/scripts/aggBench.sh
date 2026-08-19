EXPERIMENT_TYPE="$1"
NODE_NUM="$2"
SERVER_ADDR="$3"
SERVER_PORT="$4"
DB_NAME="$5"
THREAD_COUNT="$6"
N_ITER="$7"
BATCH_ITER="$8"
INVOKE_AT="$9"

# Parsed EXPERIMENT_TYPE
WORKLOAD=$(echo $EXPERIMENT_TYPE | cut -d '_' -f 1)
PATTERN=$(echo $EXPERIMENT_TYPE | cut -d '_' -f 2)

tmux has-session 2>/dev/null

# IF there's a tmux session exists -> Kill
if [ $? == 0 ]; then
    tmux kill-server
fi

# Pre-emptively clean any .csv/.parquet files
rm -f *.csv *.parquet && sleep 5

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Copy data to home directory
cp -r 10Y_128-Cols_Experiments/AGG_$WORKLOAD\_QUERY/AGG_$WORKLOAD\_QUERY\_Node-$NODE_NUM/AGG-$WORKLOAD\-QUERY_Client-* .

# Init tmux session
tmux new-session -d -s XStore -n Client

# Dispatch workload  
tmux send-keys -t "Client" "./benchmarkClient aggBench -i $SERVER_ADDR -p $SERVER_PORT -d $DB_NAME -t $THREAD_COUNT -e $WORKLOAD -n $N_ITER -b $BATCH_ITER -ia $INVOKE_AT && tmux kill-server" C-m