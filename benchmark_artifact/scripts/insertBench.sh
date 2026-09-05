source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

EXPERIMENT_TYPE="$1"
NODE_NUM="$2"
SERVER_ADDR="$3"
SERVER_PORT="$4"
DB_NAME="$5"
THREAD_COUNT="$6"
N_ITER="$7"
BATCH_ITER="$8"
INVOKE_AT="$9"
DURABILITY="${10:-ASYNC}"

# Parsed EXPERIMENT_TYPE
WORKLOAD=$(echo $EXPERIMENT_TYPE | cut -d '_' -f 1)
PATTERN=$(echo $EXPERIMENT_TYPE | cut -d '_' -f 2)

killTmuxIfRunning

# Pre-emptively clean any .csv/.parquet files
rm -f *.csv *.parquet && sleep 5

dropPageCache

# Copy data to home directory
cp -r 10Y_128-Cols_Experiments/$WORKLOAD\_INSERT_$PATTERN/$WORKLOAD\_INSERT_$PATTERN\_Node-$NODE_NUM/BASIC-INSERT-$WORKLOAD-$PATTERN\_Client-* .

# Init tmux session
tmux new-session -d -s XStore -n Client

# Dispatch workload  
tmux send-keys -t "Client" "./benchmarkClient insertBench -i $SERVER_ADDR -p $SERVER_PORT -d $DB_NAME -t $THREAD_COUNT -e $EXPERIMENT_TYPE -n $N_ITER -b $BATCH_ITER -ia $INVOKE_AT -w $DURABILITY && tmux kill-server" C-m