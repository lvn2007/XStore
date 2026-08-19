isPurge="$1"
WORKING_DIR="$2"

# Pre-emptively kill any tmux session
tmux has-session 2>/dev/null
sudo pkill go-carbon

# IF there's a tmux session exists -> Kill
if [ $? == 0 ]; then
    tmux kill-server && sleep 5
fi

# Go to proper working dir
cd ${WORKING_DIR}

# Intialize tmux
tmux new-session -d -s Graphite -n Server
tmux new-window -t Graphite -n API

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    sudo rm -r ${WORKING_DIR}/graphite/whisper/
    mkdir -p ${WORKING_DIR}/graphite/whisper/
fi

# Start Go-Carbon/API
tmux send-keys -t "Server" "sudo go-carbon -config ~/go-carbon.conf" C-m
tmux send-keys -t "API" "sudo carbonapi -config ~/carbon-api.yaml" C-m

# Delay to make sure server has enough time to be up 
sleep 10