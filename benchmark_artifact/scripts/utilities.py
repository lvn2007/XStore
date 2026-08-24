import time, os, shutil, itertools, math
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from fabric import Connection

marker = itertools.cycle((">", "o", "s", "^", "v", "p", "P", "d", "*"))

DEFAULT_KEEPALIVE_SEC = 30
DEFAULT_RESTORE_TIMEOUT_SEC = 3600


class _KeepAliveConnection(Connection):
    """Connection subclass that re-applies SSH keepalive on every open() -- including
    Fabric's automatic lazy reopen after .close(). Keepalive lives on the Transport,
    and .close() tears the Transport down; the next .run()/.put() builds a brand new
    one with no keepalive unless something re-sets it. Plain "set keepalive once after
    connecting" silently stops protecting a connection the moment anything closes and
    reopens it -- e.g. a legitimate close on an error path (see pollTmuxUntilDone),
    or code elsewhere reusing the same long-lived serverGroup/allGroup/clientGroup
    across a multi-hour notebook run -- so a one-shot fix would only protect a
    connection up to its first close before quietly reverting to the original
    unprotected/idle-hang behavior."""
    def __init__(self, host, user="cc", connect_timeout=30, keepalive=DEFAULT_KEEPALIVE_SEC, **kwargs):
        super().__init__(host, user=user, connect_timeout=connect_timeout, **kwargs)
        object.__setattr__(self, "_keepalive_sec", keepalive)

    def open(self):
        super().open()
        self.transport.set_keepalive(self._keepalive_sec)


def makeConnection(host, user="cc", keepalive=DEFAULT_KEEPALIVE_SEC, connect_timeout=30):
    conn = _KeepAliveConnection(host, user=user, connect_timeout=connect_timeout, keepalive=keepalive)
    conn.open()
    return conn


def estimateWorkloadTimeout(nIter, batchIter, secPerOp=0.1, bufferMultiplier=5, minTimeout=600, maxTimeout=10800):
    """Upper bound on how long a benchmarkClient run should take, used only to catch
    a genuinely stuck job (tmux session never closes) -- not a tight SLA. batchIter==0
    (unary workloads) is treated as a multiplier of 1. secPerOp/bufferMultiplier are
    placeholders -- tune from real observed run durations on Mystic. Runtime varies by
    DB (InfluxDB is the slowest observed so far, ~2.5h); default ceiling is 3h to give
    that headroom. For a workload/DB combo known to need more, pass runWorkload's
    jobTimeoutOverride instead of raising this ceiling globally."""
    totalOps = nIter * max(batchIter, 1)
    return max(minTimeout, min(maxTimeout, totalOps * secPerOp * bufferMultiplier))


def _asConnections(target):
    if isinstance(target, Connection):
        return [target]
    return list(target)


def pollTmuxUntilDone(target, timeoutSec, pollInterval=10, label=""):
    """Poll `target` (a Connection, a list of Connections, or a Group) for an active
    tmux session. `tmux ls` exiting non-zero means no session left -- the workload
    closed it itself on completion, so that's success. Any other error (dead
    connection, auth failure) is retried with backoff up to timeoutSec instead of
    either hanging forever or silently treating a connectivity blip as "done". Holds
    the connection open across the whole poll (keepalive -- see makeConnection --
    already handles staleness) rather than closing/reopening every cycle; only closes
    on an actual error, to force a clean reconnect attempt next time around."""
    tag = f" {label}" if label else ""
    conns = _asConnections(target)
    deadline = time.time() + timeoutSec

    while time.time() < deadline:
        allDone = True
        for conn in conns:
            try:
                result = conn.run("tmux ls", hide=True, warn=True)
                if result.exited == 0:
                    allDone = False
            except Exception as e:
                print(f"[pollTmuxUntilDone{tag}] {conn.host}: transient error, retrying: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                allDone = False
        if allDone:
            return True
        time.sleep(pollInterval)

    print(f"[pollTmuxUntilDone{tag}] TIMEOUT after {int(timeoutSec)}s -- at least one node still has an active tmux session, likely stuck. Capturing pane(s):")
    for conn in conns:
        try:
            result = conn.run("tmux capture-pane -p", hide=True, warn=True)
            print(f"  --- {conn.host} ---\n{result.stdout}")
        except Exception as e:
            print(f"  --- {conn.host}: could not capture pane ({e}) ---")
    return False


def checkTmuxJobIsDone(connectionGroup, timeoutSec=DEFAULT_RESTORE_TIMEOUT_SEC, pollInterval=10):
    return pollTmuxUntilDone(connectionGroup, timeoutSec, pollInterval, label="restore")


def fetchResults(basePath, fileExt, selectedClients, serverGroup, user, fetchSysStats=True,
                  maxAttempts=12, retryDelay=5, keepalive=DEFAULT_KEEPALIVE_SEC):
    """Download each client's result files (fileExt) plus one copy of the server's
    sysstat .txt files (saved under Node-0). Bounded retries instead of `while True`
    so a real failure (bad path, dead connection) surfaces as an error instead of
    spinning forever."""
    lastErr = None
    for attempt in range(1, maxAttempts + 1):
        try:
            hasSysStats = not fetchSysStats
            for idx, ip in enumerate(selectedClients):
                currPath = f"{basePath}Node-{idx}/"
                os.makedirs(currPath, exist_ok=True)

                with makeConnection(ip, user=user, keepalive=keepalive) as conn:
                    files = conn.run(f"ls *.{fileExt}", hide=True).stdout.split("\n")
                    files = [f for f in files if f.endswith(f".{fileExt}")]
                    for file in files:
                        conn.get(file)
                        destFile = f"{currPath}{file}"
                        if os.path.isfile(destFile):
                            os.remove(destFile)
                        shutil.move(file, currPath)
                print(f"[fetchResults] [Client: {ip}] - Downloaded {len(files)} result files")

                if not hasSysStats:
                    with makeConnection(serverGroup[0].host, user=user, keepalive=keepalive) as sconn:
                        txtFiles = sconn.run("ls *.txt", hide=True).stdout.split("\n")
                        txtFiles = [f for f in txtFiles if f.endswith(".txt")]
                        for file in txtFiles:
                            sconn.get(file)
                            destFile = f"{currPath}{file}"
                            if os.path.isfile(destFile):
                                os.remove(destFile)
                            shutil.move(file, currPath)
                    print(f"[fetchResults] [Server] - Downloaded {len(txtFiles)} system stats files")
                    hasSysStats = True
            return
        except Exception as e:
            lastErr = e
            print(f"[fetchResults] attempt {attempt}/{maxAttempts} failed: {e}; retrying in {retryDelay}s")
            time.sleep(retryDelay)
    raise RuntimeError(f"fetchResults: giving up after {maxAttempts} attempts") from lastErr


def runWorkload(EXPERIMENT_TYPE, targetClients, serverNode, serverPort, dbName, clientNodes, nIter, batchIter,
                 serverGroup, serverWorkingDir, user="cc", chunkSize=1000000, skipSize=900, dataFile="data.csv",
                 sleepInterval=10, secPerOp=0.1, bufferMultiplier=5, minTimeout=600, maxTimeout=10800,
                 backtestTimeoutSec=7200, keepalive=DEFAULT_KEEPALIVE_SEC, jobTimeoutOverride=None):
    WORKLOAD_TYPE = EXPERIMENT_TYPE.split("_")[0]
    EXPERIMENT_TYPE = EXPERIMENT_TYPE[len(WORKLOAD_TYPE) + 1:]

    for numClient in targetClients:
        totalMachines = len(clientNodes)

        useSingleMachine = (numClient < 8) or (dbName in ["CSV", "Parquet"])

        if useSingleMachine:
            machinesToUse = 1
            selectedClients = clientNodes[:1]
            threadsPerMachine = numClient
        else:
            machinesToUse = min(numClient, totalMachines)
            selectedClients = clientNodes[:machinesToUse]
            threadsPerMachine = numClient // machinesToUse
        totalClients = numClient
        print(f"""[Benchmark Config]\n  Target Clients     : {numClient}\n  Total Machines     : {totalMachines}\n  Machines Used      : {machinesToUse}\n  Threads per Machine: {threadsPerMachine}\n  Total Clients Actual : {threadsPerMachine * machinesToUse}""")

        # Start server
        if dbName not in ["CSV", "Parquet"]:
            serverGroup.put(f"../scripts/{dbName}/run{dbName}Server.sh")
        else:
            if "NFS" in dataFile:
                serverGroup.put(f"../scripts/NFS/runNFSServer.sh")

        if WORKLOAD_TYPE == "INSERT":
            serverGroup.run(f"bash run{dbName}Server.sh true {serverWorkingDir}", hide=True)
        else:
            if dbName not in ["CSV", "Parquet"]:
                serverGroup.run(f"bash run{dbName}Server.sh false {serverWorkingDir}", hide=True)
            else:
                if "NFS" in dataFile:
                    serverGroup.run(f"bash runNFSServer.sh", hide=True)

        # Start monitoring system stats (IGNORE BACKTEST)
        if WORKLOAD_TYPE != "BACKTEST":
            serverGroup.put(f"../scripts/sysMonitor.sh")
            serverGroup.run(f"bash sysMonitor.sh 50 {dbName}", hide=True)

        # Schedule workload to be dispatch 2 min from now
        epochTime = int(time.time()) + (60 * 2)
        print(f"[Benchmark: {totalClients}] - Schedule workload on {epochTime}")

        for idx, ip in enumerate(selectedClients):
            with makeConnection(ip, user=user, keepalive=keepalive) as conn:
                if WORKLOAD_TYPE == "INSERT":
                    conn.put(f"../scripts/insertBench.sh")
                    conn.run(f"bash insertBench.sh {EXPERIMENT_TYPE} {idx} {serverNode} {serverPort} {dbName.upper()} {threadsPerMachine} {nIter} {batchIter} {epochTime}")
                elif WORKLOAD_TYPE == "QUERY":
                    conn.put(f"../scripts/queryBench.sh")
                    conn.run(f"bash queryBench.sh {EXPERIMENT_TYPE} {idx} {serverNode} {serverPort} {dbName.upper()} {threadsPerMachine} {nIter} {batchIter} {epochTime}")
                elif WORKLOAD_TYPE == "AGG":
                    conn.put(f"../scripts/aggBench.sh")
                    conn.run(f"bash aggBench.sh {EXPERIMENT_TYPE} {idx} {serverNode} {serverPort} {dbName.upper()} {threadsPerMachine} {nIter} {batchIter} {epochTime}")
                elif WORKLOAD_TYPE == "BACKTEST":
                    conn.put(f"../scripts/backtestBench.sh")
                    conn.run(f"bash backtestBench.sh {threadsPerMachine} {serverNode} {serverPort} {chunkSize} {skipSize} {dataFile} {dbName.upper()} {epochTime}")
            print(f"[Benchmark: {totalClients}] [Client: {ip}] - Time now: {int(time.time())} | Delta: {epochTime - int(time.time())}")

        # Poll until and add 1 min delay buffer for workload to finish
        poller(epochTime)
        time.sleep(60)

        # Wait for the client tmux sessions to close on their own (workload finished);
        # bounded so a real fault doesn't hang the notebook forever. Runtime varies by
        # DB (InfluxDB is the slowest observed, ~2.5h, hence the 3h default ceiling) --
        # pass jobTimeoutOverride for a workload/DB combo known to need more.
        if jobTimeoutOverride is not None:
            jobTimeout = jobTimeoutOverride
        elif WORKLOAD_TYPE == "BACKTEST":
            jobTimeout = backtestTimeoutSec
        else:
            jobTimeout = estimateWorkloadTimeout(nIter, batchIter, secPerOp, bufferMultiplier, minTimeout, maxTimeout)

        waitConns = [makeConnection(ip, user=user, keepalive=keepalive) for ip in selectedClients]
        try:
            finished = pollTmuxUntilDone(waitConns, jobTimeout, sleepInterval, label=f"Client-{totalClients}")
        finally:
            for c in waitConns:
                try:
                    c.close()
                except Exception:
                    pass

        if not finished:
            # Do NOT fall through to fetchResults: the job may still be legitimately
            # running (see docstring above), and fetching now would grab a partial/
            # still-being-written result set instead of a real completed run. Stop
            # loudly instead so the notebook cell fails visibly and can be re-run with
            # a larger jobTimeoutOverride, rather than silently recording bad data.
            raise RuntimeError(
                f"[Benchmark: {totalClients}] job did not finish within {int(jobTimeout)}s. "
                f"This does NOT necessarily mean it's stuck -- some DB/workload combos "
                f"(e.g. InfluxDB) can legitimately run this long. Check the captured tmux "
                f"pane above: if it's still making progress, rerun with a larger "
                f"jobTimeoutOverride; if it's genuinely hung, investigate the server/client "
                f"directly before retrying."
            )

        # Kill server monitoring
        if WORKLOAD_TYPE != "BACKTEST":
            serverGroup.run("pkill sar &", hide=True)

        # Fetch bench results to this notebook
        if WORKLOAD_TYPE != "BACKTEST":
            basePath = f"{dbName.upper()}_{WORKLOAD_TYPE}_{EXPERIMENT_TYPE}/Client-{totalClients}/"
            fileExt = "csv"
        else:
            basePath = f"{dbName.upper()}_{WORKLOAD_TYPE}/Client-{totalClients}/"
            fileExt = "txt"
        fetchResults(basePath, fileExt, selectedClients, serverGroup, user,
                     fetchSysStats=(WORKLOAD_TYPE != "BACKTEST"), keepalive=keepalive)


def unaryPlots(WORKLOAD_TYPE, DB_NAME, targetThreads, clientNodes, xLabel, logScale=False):
    df = _loadBenchData(WORKLOAD_TYPE, DB_NAME, dropFirst=True)
    fig, ax = plt.subplots()

    for idx, i in enumerate(df.columns):
        currDF = df[i]
        splittedLabel = i.split("-")
        if splittedLabel[-1] == "1":
            legendLabel = "1 Client"
        else:
            legendLabel = f"{splittedLabel[1]} Clients"
        plt.scatter(currDF.index, df[i], label=legendLabel, s=10, marker=next(marker), edgecolors="none")

    # Labels & Legends
    ax.set_title(f'Unary {WORKLOAD_TYPE.split("_")[0].title()} - {WORKLOAD_TYPE.split("_")[-1]}')
    ax.set_xlabel(f'{xLabel} #')
    ax.set_ylabel('Latency (ms)')

    if logScale:
        ax.set_yscale('log')

    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
    ax.yaxis.set_minor_formatter(ticker.FormatStrFormatter("%.2f"))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.legend(bbox_to_anchor=(0., -0.35, 1., 0.102), loc='lower center', mode='expand', ncols=3, borderaxespad=0, markerscale=3)
    plt.tight_layout(pad=0.15)
    plt.autoscale(axis='x')
    plt.savefig(f"{DB_NAME}_{WORKLOAD_TYPE}.svg", format='svg')


def batchPlots(WORKLOAD_TYPE, DB_NAME, targetThreads, clientNodes, batchSize=1000):
    df = _loadBenchData(WORKLOAD_TYPE, DB_NAME, zeroOnTimeout=True)
    aggThroughput = []

    for idx, i in enumerate(df.columns):
        currDF = df[i]
        # Compute rows per second
        #   EX: Took 70ms to query 1,000 rows
        #   -> Rows per Second = (1000 * 1000) / 70
        #   -> Aggregate across all clients = Rows per Second * numClients
        numClient = int(i.split("-")[-1])
        if currDF.mean() == 0:
            aggThroughput.append(0)
        else:
            aggThroughput.append(round(1000 * batchSize / currDF.mean() * numClient))

    # Axis
    fig, ax = plt.subplots()

    colors = ['#1B9E77', '#a9f971', '#fdaa48', '#6890F0', '#A890F0', '#56B4E9', '#CC79A7', '#a65628', '#e41a1c', '#999999']

    bar1 = ax.bar([f'{i.split("-")[-1]}' for i in df.columns], aggThroughput, color=colors)

    ax.set_yticklabels([f'{str(int(label))[0:-3] + "K"}' for label in ax.get_yticks().tolist()])

    ax.set_title(f'Aggregated Mean Throughput – ({WORKLOAD_TYPE})')
    ax.set_ylabel('Rows per Second')
    ax.set_xlabel('Number of Clients')

    ax.bar_label(ax.containers[0], label_type='edge', labels=[f'{i:,}' for i in bar1.datavalues])
    ax.margins(y=0.1)

    plt.tight_layout(pad=0.15)
    plt.autoscale(axis='x')
    plt.savefig(f"{DB_NAME}_{WORKLOAD_TYPE}_AGG-1000.svg", format='svg')


def _loadBenchData(WORKLOAD_TYPE, DB_NAME, dropFirst=False, zeroOnTimeout=False):
    """Shared CSV-load/aggregate step behind unaryPlots and batchPlots: read each
    node's per-client CSVs, average latency across clients per client-count tier."""
    clientDir = sorted(os.listdir(f"{DB_NAME.upper()}_{WORKLOAD_TYPE}"), key=lambda x: int(x.split("-")[-1]))
    benchData = {}

    for i in clientDir:
        if int(i.split('-')[1]) > 64:
            continue
        currCSVdata = []
        currClientDir = os.listdir(f"{DB_NAME.upper()}_{WORKLOAD_TYPE}/{i}")

        for item in currClientDir:
            myDir = f"{DB_NAME.upper()}_{WORKLOAD_TYPE}/{i}/{item}"
            csvFiles = [f for f in os.listdir(myDir) if f.endswith(".csv")]
            for csvFile in csvFiles:
                df = pd.read_csv(f"{myDir}/{csvFile}")
                currCSVdata.append(df["Time taken (ms)"].tolist())

        df = pd.DataFrame(zip(*currCSVdata))

        # If has value 0 -> Test must be partially/fully timed-out
        if zeroOnTimeout and (df.values == 0).any():
            benchData[i] = [0] * len(df)
            continue

        df["Mean"] = df.mean(axis=1)
        vals = df["Mean"].tolist()
        benchData[i] = vals[1:] if dropFirst else vals

    return pd.DataFrame(benchData)


def poller(epochTime):
    while time.time() <= epochTime:
        time.sleep(1)
