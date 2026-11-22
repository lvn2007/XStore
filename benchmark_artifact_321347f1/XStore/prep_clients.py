import sys, time
sys.path.append("/home/ubuntu/XStore/benchmark_artifact/scripts")
from fabric import ThreadingGroup
from fabric.group import GroupException
from utilities import makeConnection

SCRIPTS = "/home/ubuntu/XStore/benchmark_artifact/scripts/"
client_ips = ["172.16.5.134", "172.16.5.136", "172.16.5.138", "172.16.5.133",
              "172.16.5.135", "172.16.5.137", "172.16.5.139", "172.16.5.140"]

conns = [makeConnection(ip, user="ubuntu") for ip in client_ips]
group = ThreadingGroup.from_connections(conns)

def run(tag, cmd, warn=True):
    print(f"\n=== [{tag}] {time.strftime('%H:%M:%S')} dispatching: {cmd}", flush=True)
    try:
        res = group.run(cmd, warn=warn, hide="out")
    except GroupException as e:
        res = e.result
    for host, r in res.items():
        status = getattr(r, "exited", repr(r)[:200])
        print(f"  [{tag}] {host}: exit={status}", flush=True)
    return res

group.put(SCRIPTS + "initXBenchClient.sh")
group.put(SCRIPTS + "initData.sh")
run("sed-url", "sed 's#http://172.16.6.231:8000/XStore_ExperimentDataset#http://172.16.6.15:8000/NEW#' initData.sh > initData_local.sh && grep -c 'NEW/10Y' initData_local.sh")
run("dataset", "bash initData_local.sh MYSTIC@IIT")
run("build", "PIP_BREAK_SYSTEM_PACKAGES=1 bash initXBenchClient.sh MYSTIC@IIT")
run("verify", "test -x ~/XBench && echo BIN_OK; test -d ~/10Y_128-Cols_Experiments && echo DATA_OK; ls ~/10Y_128-Cols_Experiments | head -20")
print("\nPREP_COMPLETE", flush=True)
