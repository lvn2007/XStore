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
    tmux send-keys -t "Server" "cd XStore_Server/build && sudo rm -rf DataStore/BENCH_DB DataStore/BACKTESTER DataStore/WAL; mkdir -p DataStore && ./XStore --host 0.0.0.0 --port 9497 --threads $(($(nproc --all)/2))" C-m
else
    tmux send-keys -t "Server" "cd XStore_Server/build && ./XStore --host 0.0.0.0 --port 9497 --threads $(($(nproc --all)/2))" C-m
fi

# Wait until the server is actually accepting connections instead of a fixed
# sleep: the first start after restoreDB rebuilds .zmap/.catidx sidecars by a
# full scan of every year, which can take minutes (docs/XStore_Handoff.md R1).
# Timeout: optional third argument, else XSTORE_SERVER_WAIT_TIMEOUT, else 1800 s.
server_port=9497
wait_timeout="${3:-${XSTORE_SERVER_WAIT_TIMEOUT:-1800}}"
start_secs=$SECONDS
while ! (: > "/dev/tcp/127.0.0.1/${server_port}") 2>/dev/null; do
    if [ $((SECONDS - start_secs)) -ge "${wait_timeout}" ]; then
        echo "ERROR: server port ${server_port} not open after ${wait_timeout}s" >&2
        exit 1
    fi
    sleep 1
done
echo "Server port ${server_port} open after $((SECONDS - start_secs))s"