# Shared helpers sourced by both the client-side dispatch wrappers
# (insertBench.sh, queryBench.sh, aggBench.sh, backtestBench.sh) and the
# per-DB server scripts (<DB>/init<DB>Server.sh, <DB>/run<DB>Server.sh).
# Each caller stays its own separate, DB/workload-named file; only the
# literal boilerplate lines below move here.

killTmuxIfRunning() {
    # $1: optional seconds to sleep after a successful kill (default: 0)
    local delay="${1:-0}"

    # docs/XStore_Handoff.md S9: stop XStore gracefully BEFORE tmux kill-server.
    # The server runs in the foreground of a tmux pane; killing tmux first makes
    # it die of unhandled SIGHUP -- no final WAL checkpoint, every index left
    # dirty, full index rebuild on next start. Exact-name pgrep only: this helper
    # also runs for the Graphite server and on client nodes, where nothing (or a
    # different process) must match.
    local xstore_pids
    xstore_pids="$(pgrep -x XStore)"
    if [ -n "$xstore_pids" ]; then
        local pid
        for pid in $xstore_pids; do
            kill -TERM "$pid" 2>/dev/null
        done
        local waited=0
        while [ "$waited" -lt 60 ]; do
            sleep 1
            waited=$((waited + 1))
            if ! pgrep -x XStore >/dev/null; then
                echo "XStore exited cleanly after ${waited}s"
                break
            fi
        done
        if pgrep -x XStore >/dev/null; then
            echo "WARNING: XStore did not exit; indexes will be rebuilt on next start" >&2
        fi
    fi

    tmux has-session 2>/dev/null
    if [ $? == 0 ]; then
        tmux kill-server
        [ "$delay" -gt 0 ] && sleep "$delay"
    fi
}

dropPageCache() {
    free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null
}

openFirewallPorts() {
    # $@: one or more ports to open (tcp, permanent)
    if command -p firewall-cmd >/dev/null 2>&1; then
        for port in "$@"; do
            sudo firewall-cmd --add-port ${port}/tcp --permanent
        done
        sudo firewall-cmd --reload
    fi
}
