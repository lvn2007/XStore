isPurge="$1"
WORKING_DIR="$2"

# Pre-emptively kill any tmux session
tmux has-session 2>/dev/null

# IF there's a tmux session exists -> Kill
if [ $? == 0 ]; then
    tmux kill-server && sleep 5
fi

# Go to proper working dir
cd ${WORKING_DIR}

# Intialize tmux
tmux new-session -d -s XStore -n Server

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Pre-emptively purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    tmux send-keys -t "Server" "cd XStore_Server/build && sudo rm -r DataStore; mkdir -p DataStore && ./XStore 0.0.0.0 9497 $(($(nproc --all)/2))" C-m
else
    tmux send-keys -t "Server" "cd XStore_Server/build && ./XStore 0.0.0.0 9497 $(($(nproc --all)/2))" C-m
fi

# Delay to make sure server has enough time to be up
sleep 10