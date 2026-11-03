DELAY_BY="$1"
DB_NAME="$2"

# Purge existing files
rm -rf cpu.txt mem.txt io.txt

# XStore/Graphite already have a "Server" tmux session running (started by run<DB>Server.sh);
# MongoDB/InfluxDB use systemctl/service instead, so no session exists yet -> create one here.
if [[ "$DB_NAME" != "XStore" && "$DB_NAME" != "Graphite" ]]; then
    tmux kill-server 2>/dev/null
    tmux new-session -d -s ${DB_NAME} -n Server
fi

# Create new tmux window
tmux new-window -t ${DB_NAME} -n Monitor

# Delay & Montoring
tmux send-keys -t "Monitor" "sleep ${DELAY_BY} && (sar -u 1 >> cpu.txt & sar -r 1 >> mem.txt & sar -b 1 >> io.txt &)" C-m