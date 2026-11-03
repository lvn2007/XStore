# Shared helpers sourced by both the client-side dispatch wrappers
# (insertBench.sh, queryBench.sh, aggBench.sh, backtestBench.sh) and the
# per-DB server scripts (<DB>/init<DB>Server.sh, <DB>/run<DB>Server.sh).
# Each caller stays its own separate, DB/workload-named file; only the
# literal boilerplate lines below move here.

killTmuxIfRunning() {
    # $1: optional seconds to sleep after a successful kill (default: 0)
    local delay="${1:-0}"
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
