source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

isPurge="$1"
WORKING_DIR="$2"

killTmuxIfRunning 5

# Go to proper working dir
cd ${WORKING_DIR}

# Intialize tmux
tmux new-session -d -s XStore -n Server

dropPageCache

# Pre-emptively purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    tmux send-keys -t "Server" "cd XStore_Server/build && sudo rm -rf DataStore/BENCH_DB DataStore/BACKTESTER; mkdir -p DataStore && ./XStore --host 0.0.0.0 --port 9497 --threads $(($(nproc --all)/2))" C-m
else
    tmux send-keys -t "Server" "cd XStore_Server/build && ./XStore --host 0.0.0.0 --port 9497 --threads $(($(nproc --all)/2))" C-m
fi

# Delay to make sure server has enough time to be up
sleep 10