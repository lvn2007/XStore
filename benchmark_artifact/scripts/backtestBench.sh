GRID_SIZE="$1"
SERVER_ADDR="$2"
SERVER_PORT="$3"
CHUNK_SIZE="$4"
SKIP_SIZE="$5"
DATA_FILE="$6"
DB_NAME="$7"
INVOKE_AT="$8"

tmux has-session 2>/dev/null

# IF there's a tmux session exists -> Kill
if [ $? == 0 ]; then
    tmux kill-server
fi

# Pre-emptively clean any .csv/.parquet/.txt files
rm -f *.csv *.parquet *.txt && sleep 5

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

if [ $DB_NAME == "CSV" ] || [ $DB_NAME == "PARQUET" ]; then
    # Check if NFS is in DATA_FILE
    if [ ${DATA_FILE} == *"NFS"* ]; then
        sudo umount /NFS_STORAGE 2>/dev/null || true
        sleep 5
        sudo mount -t nfs -o hard,noatime,nodiratime,sync,noac,actimeo=0,lookupcache=none,rsize=4096,wsize=4096 ${SERVER_ADDR}:/NVME_RAID-0 /NFS_STORAGE
    fi
fi

# Init tmux session
tmux new-session -d -s XStore -n Client

# Dispatch workload  
tmux send-keys -t "Client" "python3 backtesterSkipping.py --grid-size ${GRID_SIZE} -ip ${SERVER_ADDR} -p ${SERVER_PORT} -c ${CHUNK_SIZE} -s ${SKIP_SIZE} -f ${DATA_FILE} -d ${DB_NAME} -o ${DB_NAME}_${GRID_SIZE}.txt -ia ${INVOKE_AT} && sleep 60 && tmux kill-server" C-m