source "$(dirname "${BASH_SOURCE[0]}")/../_common.sh"

isPurge="$1"
WORKING_DIR="$2"

sudo pkill go-carbon
killTmuxIfRunning 5

# Go to proper working dir
cd ${WORKING_DIR}

# Intialize tmux
tmux new-session -d -s Graphite -n Server
tmux new-window -t Graphite -n API

dropPageCache

# Purge existing dataStore (BENCH_DB & BACKTESTER only) & Start server
if [ "$isPurge" == "true" ]; then
    sudo rm -rf ${WORKING_DIR}/graphite/whisper/BENCH_DB ${WORKING_DIR}/graphite/whisper/BACKTESTER
    mkdir -p ${WORKING_DIR}/graphite/whisper/
fi

# Start Go-Carbon/API
tmux send-keys -t "Server" "sudo go-carbon -config ~/go-carbon.conf" C-m
tmux send-keys -t "API" "sudo carbonapi -config ~/carbon-api.yaml" C-m

# Delay to make sure server has enough time to be up 
sleep 10