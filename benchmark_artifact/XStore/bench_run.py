import sys, time, traceback
sys.path.append("/home/ubuntu/XStore/benchmark_artifact/scripts")
from fabric import ThreadingGroup
from utilities import makeConnection, runWorkload, unaryPlots, batchPlots, pollTmuxUntilDone

sshUser = "ubuntu"
server_ips = ["172.16.5.141"]
client_ips = ["172.16.5.134", "172.16.5.136", "172.16.5.138", "172.16.5.133",
              "172.16.5.135", "172.16.5.137", "172.16.5.139", "172.16.5.140"]
SERVER_WORKING_DIR = "/NVME_RAID-0"
SNAPSHOT = "/ceph/lnguyen/XStore_BENCH_DB_snapshot/DataStore"
BUILD = "/NVME_RAID-0/XStore/build"

serverConnections = [makeConnection(i, user=sshUser) for i in server_ips]
clientConnections = [makeConnection(i, user=sshUser) for i in client_ips]
serverGroup = ThreadingGroup.from_connections(serverConnections)
clientGroup = ThreadingGroup.from_connections(clientConnections)

local_ips = serverGroup.run("hostname -I | cut -d ' ' -f 1", hide=True)
server_local = next(iter(local_ips.values())).stdout.strip()
print(f"[Driver] server_local={server_local}", flush=True)
targetThreads = [1, 2, 4, 8, 16, 32, 64]

# Bootstrap DB: restore BENCH_DB (+WAL) from /ceph snapshot (replaces restoreDB.sh)
print(f"[Driver] Stopping server before restore", flush=True)
serverGroup.run("tmux kill-session -t XStore 2>/dev/null; sleep 3; pgrep -x XStore && echo STILL_RUNNING || echo STOPPED", hide=True)
print(f"[Driver] Restore start {time.strftime('%H:%M:%S')}", flush=True)
serverGroup.run(
    f"tmux kill-session -t restore 2>/dev/null; "
    f"tmux new-session -d -s restore 'rm -rf {BUILD}/DataStore/BENCH_DB {BUILD}/DataStore/WAL; "
    f"cp -a {SNAPSHOT}/BENCH_DB {BUILD}/DataStore/ && cp -a {SNAPSHOT}/WAL {BUILD}/DataStore/; "
    f"echo RESTORE_DONE rc=$? | tee /tmp/restore.status'", hide=True)
restored = pollTmuxUntilDone(serverConnections, 7200, 30, label="restore")
status = serverGroup.run("cat /tmp/restore.status 2>/dev/null; ls " + BUILD + "/DataStore/BENCH_DB | wc -l; du -sh " + BUILD + "/DataStore/BENCH_DB", hide=True)
print(f"[Driver] restore poll finished={restored}", flush=True)
for h, r in status.items():
    print(f"[Driver] restore status {h}: {r.stdout.strip()}", flush=True)

failures = []

def attempt(tag, fn):
    print(f"\n########## {tag} start {time.strftime('%H:%M:%S')} ##########", flush=True)
    try:
        fn()
        print(f"########## {tag} OK {time.strftime('%H:%M:%S')} ##########", flush=True)
    except Exception as e:
        failures.append(tag)
        print(f"########## {tag} FAILED: {e} ##########", flush=True)
        traceback.print_exc()

# QUERY workloads (runXStoreServer.sh false = non-purge; scripts dropPageCache on server+clients)
attempt("QUERY_UNARY_SEQ", lambda: (
    runWorkload("QUERY_UNARY_SEQ", targetThreads, server_local, 9497, "XStore", client_ips, 1000, 0, serverGroup, SERVER_WORKING_DIR, sshUser),
    unaryPlots("QUERY_UNARY_SEQ", "XStore", targetThreads, client_ips, "Query", logScale=True)))
attempt("QUERY_UNARY_RAND", lambda: (
    runWorkload("QUERY_UNARY_RAND", targetThreads, server_local, 9497, "XStore", client_ips, 1000, 0, serverGroup, SERVER_WORKING_DIR, sshUser),
    unaryPlots("QUERY_UNARY_RAND", "XStore", targetThreads, client_ips, "Query", logScale=True)))
attempt("QUERY_BATCH_SEQ", lambda: (
    runWorkload("QUERY_BATCH_SEQ", targetThreads, server_local, 9497, "XStore", client_ips, 100, 10, serverGroup, SERVER_WORKING_DIR, sshUser),
    batchPlots("QUERY_BATCH_SEQ", "XStore", targetThreads, client_ips, 100)))
attempt("QUERY_BATCH_RAND", lambda: (
    runWorkload("QUERY_BATCH_RAND", targetThreads, server_local, 9497, "XStore", client_ips, 100, 10, serverGroup, SERVER_WORKING_DIR, sshUser),
    batchPlots("QUERY_BATCH_RAND", "XStore", targetThreads, client_ips, 100)))

# Refresh before AGG (mirror notebook reboot with restart+cache-drop; data persists on disk)
serverGroup.run("tmux kill-session -t XStore 2>/dev/null; sleep 3", hide=True)

attempt("AGG_MIN_QUERY", lambda: (
    runWorkload("AGG_MIN_QUERY", targetThreads, server_local, 9497, "XStore", client_ips, 60, 1000, serverGroup, SERVER_WORKING_DIR, sshUser),
    unaryPlots("AGG_MIN_QUERY", "XStore", targetThreads, client_ips, "Aggregate", logScale=True)))
attempt("AGG_MAX_QUERY", lambda: (
    runWorkload("AGG_MAX_QUERY", targetThreads, server_local, 9497, "XStore", client_ips, 60, 1000, serverGroup, SERVER_WORKING_DIR, sshUser),
    unaryPlots("AGG_MAX_QUERY", "XStore", targetThreads, client_ips, "Aggregate", logScale=True)))
attempt("AGG_SUM_QUERY", lambda: (
    runWorkload("AGG_SUM_QUERY", targetThreads, server_local, 9497, "XStore", client_ips, 60, 1000, serverGroup, SERVER_WORKING_DIR, sshUser),
    unaryPlots("AGG_SUM_QUERY", "XStore", targetThreads, client_ips, "Aggregate", logScale=True)))
attempt("AGG_AVG_QUERY", lambda: (
    runWorkload("AGG_AVG_QUERY", targetThreads, server_local, 9497, "XStore", client_ips, 60, 1000, serverGroup, SERVER_WORKING_DIR, sshUser),
    unaryPlots("AGG_AVG_QUERY", "XStore", targetThreads, client_ips, "Aggregate", logScale=True)))

print(f"\n[Driver] DONE {time.strftime('%H:%M:%S')} failures={failures}", flush=True)
