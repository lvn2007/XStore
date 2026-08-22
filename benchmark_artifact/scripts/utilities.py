import time, os, shutil, itertools, math
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from fabric import Connection

marker = itertools.cycle((">", "o", "s", "^", "v", "p", "P", "d", "*"))

def runWorkload(EXPERIMENT_TYPE, targetClients, serverNode, serverPort, dbName, clientNodes, nIter, batchIter, serverGroup, serverWorkingDir, user="cc", chunkSize=1000000, skipSize=900, dataFile="data.csv", sleepInterval=10):
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
        serverGroup.close()

        # Start monitoring system stats (IGNORE BACKTEST)
        if WORKLOAD_TYPE != "BACKTEST":
            serverGroup.put(f"../scripts/sysMonitor.sh")
            serverGroup.run(f"bash sysMonitor.sh 50 {dbName}", hide=True)
        
        # Schedule workload to be dispatch 2 min from now
        epochTime = int(time.time()) + (60 * 2)
        print(f"[Benchmark: {totalClients}] - Schedule workload on {epochTime}")

        for idx, ip in enumerate(selectedClients):
            # SSH to client
            conn = Connection(ip, user=user)
            
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
            conn.close()
            print(f"[Benchmark: {totalClients}] [Client: {ip}] - Time now: {int(time.time())} | Delta: {epochTime - int(time.time())}")
        
        # Poll until and add 1 min delay buffer for workload to finish
        poller(epochTime)
        time.sleep(60)
        
        # Checking every 10s for result file
        for idx, ip in enumerate(selectedClients):
            conn = Connection(ip, user=user)
            while True:
                try:
                    jobStatus = conn.run("tmux ls", hide=True)
                    time.sleep(sleepInterval)
                except:
                    time.sleep(sleepInterval)
                    break
            conn.close()

        # Kill server monitoring
        if WORKLOAD_TYPE != "BACKTEST":
            serverGroup.run("pkill sar &", hide=True)
        
        # Fetch bench results to this notebook
        #  Get till die since we know data is there from prev. cmd
        while True:
            try:
                hasSysStats = False
                for idx, ip in enumerate(selectedClients):
                    if WORKLOAD_TYPE != "BACKTEST":
                        currPath = f"{dbName.upper()}_{WORKLOAD_TYPE}_{EXPERIMENT_TYPE}/Client-{totalClients}/Node-{idx}/"
                    else:
                        currPath = f"{dbName.upper()}_{WORKLOAD_TYPE}/Client-{totalClients}/Node-{idx}/"
                    conn = Connection(ip, user=user)

                    # Create dir if not exist
                    os.makedirs(currPath, exist_ok=True)

                    # Fetch remote bench files to retrieve
                    if WORKLOAD_TYPE != "BACKTEST":
                        csvFiles = conn.run("ls *.csv", hide=True).stdout.split("\n")
                    else:
                        csvFiles = conn.run("ls *.txt", hide=True).stdout.split("\n")

                    # Download bench results
                    csvCount = 0
                    for file in csvFiles:
                        # Filter for .csv only 
                        if WORKLOAD_TYPE != "BACKTEST":
                            if not file.endswith(".csv"):
                                continue
                        else:
                            if not file.endswith(".txt"):
                                continue

                        # Obtain & Organize 
                        conn.get(file)
                        conn.close()
                        csvCount += 1

                        if os.path.isfile(f"{currPath}{file}"):
                            os.remove(f"{currPath}{file}")

                        shutil.move(file, currPath)
                    print(f"[Benchmark: {totalClients}] [Client: {ip}] - Downloaded {csvCount} result files")
                    time.sleep(5)

                    if not hasSysStats and WORKLOAD_TYPE != "BACKTEST":
                        # Fetch remote sysstat files to retrieve
                        txtFiles = serverGroup[0].run("ls *.txt", hide=True).stdout.split("\n")
    
                        # Download sysstat files
                        for file in txtFiles:
                            # Filter for .txt only 
                            if not file.endswith(".txt"):
                                continue
                
                            # Obtain & Organize 
                            serverGroup[0].get(file)
                            serverGroup[0].close()
                
                            if os.path.isfile(f"{currPath}{file}"):
                                os.remove(f"{currPath}{file}")
                
                            shutil.move(file, currPath)
                            time.sleep(5)
                        print(f"[Benchmark: {totalClients}] [Server] - Downloaded system stats files")
                        hasSysStats = True
                break
            except:
                time.sleep(5)
                pass

def runWorkload_singleClient(EXPERIMENT_TYPE, targetThreads, serverNode, serverPort, dbName, clientNodes, nIter, batchIter, serverGroup, serverWorkingDir, user="cc", chunkSize=1000000, skipSize=900, dataFile="data.csv", sleepInterval=10, dedicatedNodes=False):
    WORKLOAD_TYPE = EXPERIMENT_TYPE.split("_")[0]
    EXPERIMENT_TYPE = EXPERIMENT_TYPE[len(WORKLOAD_TYPE) + 1:]

    if not dedicatedNodes:
        clientNodes = [clientNodes[0]]
    
    for numThread in targetThreads:
        # Start server
        serverGroup.put(f"../scripts/{dbName}/run{dbName}Server.sh")
        
        if WORKLOAD_TYPE == "INSERT": 
            serverGroup.run(f"bash run{dbName}Server.sh true {serverWorkingDir}", hide=True)
        else:
            serverGroup.run(f"bash run{dbName}Server.sh false {serverWorkingDir}", hide=True)
        serverGroup.close()

        # Start monitoring system stats (IGNORE BACKTEST)
        if WORKLOAD_TYPE != "BACKTEST":
            serverGroup.put(f"../scripts/sysMonitor.sh")
            serverGroup.run(f"bash sysMonitor.sh 50 {dbName}", hide=True)
        
        # Schedule workload to be dispatch 1 min from now
        epochTime = int(time.time()) + 60
        print(f"[Benchmark: {numThread}] - Schedule workload on {epochTime}")

        # SSH to client
        conn = Connection(clientNodes, user=user)
            
        if WORKLOAD_TYPE == "INSERT":
            conn.put(f"../scripts/insertBench.sh")
            conn.run(f"bash insertBench.sh {EXPERIMENT_TYPE} 0 {serverNode} {serverPort} {dbName.upper()} {numThread} {nIter} {batchIter} {epochTime}")
        elif WORKLOAD_TYPE == "QUERY":
            conn.put(f"../scripts/queryBench.sh")
            conn.run(f"bash queryBench.sh {EXPERIMENT_TYPE} 0 {serverNode} {serverPort} {dbName.upper()} {numThread} {nIter} {batchIter} {epochTime}")
        elif WORKLOAD_TYPE == "AGG":
            conn.put(f"../scripts/aggBench.sh")
            conn.run(f"bash aggBench.sh {EXPERIMENT_TYPE} 0 {serverNode} {serverPort} {dbName.upper()} {numThread} {nIter} {batchIter} {epochTime}")
        elif WORKLOAD_TYPE == "BACKTEST":
            conn.put(f"../scripts/backtestBench.sh")
            conn.run(f"bash backtestBench.sh {numThread} {serverNode} {serverPort} {chunkSize} {skipSize} {dataFile} {dbName.upper()}")
        conn.close()
        print(f"[Benchmark: {numThread}] [Client: {clientNodes}] - Time now: {int(time.time())} | Delta: {epochTime - int(time.time())}")

        # Poll until and add 1 min delay buffer for workload to finish
        poller(epochTime)
        time.sleep(60)
        
        # Checking every 10s for result file
        for idx, ip in enumerate(clientNodes):
            conn = Connection(ip, user=user)
            while True:
                try:
                    jobStatus = conn.run("tmux ls", hide=True)
                    time.sleep(sleepInterval)
                except:
                    time.sleep(sleepInterval)
                    break
            conn.close()
        
        # Kill server monitoring
        serverGroup.run("pkill sar &", hide=True)
        
        # Fetch bench results to this notebook
        while True:
            try:
                hasSysStats = False
                for idx, ip in enumerate(clientNodes):
                    if WORKLOAD_TYPE != "BACKTEST":
                        currPath = f"{dbName.upper()}_{WORKLOAD_TYPE}_{EXPERIMENT_TYPE}/Client-{numThread}/Node-{idx}/"
                    else:
                        currPath = f"{dbName.upper()}_{WORKLOAD_TYPE}/Client-{numThread}/Node-{idx}/"
                    conn = Connection(ip, user=user)

                    # Create dir if not exist
                    os.makedirs(currPath, exist_ok=True)

                    # Fetch remote bench files to retrieve
                    if WORKLOAD_TYPE != "BACKTEST":
                        csvFiles = conn.run("ls *.csv", hide=True).stdout.split("\n")
                    else:
                        csvFiles = conn.run("ls *.txt", hide=True).stdout.split("\n")

                    # Download bench results
                    csvCount = 0
                    for file in csvFiles:
                        # Filter for .csv only 
                        if WORKLOAD_TYPE != "BACKTEST":
                            if not file.endswith(".csv"):
                                continue
                        else:
                            if not file.endswith(".txt"):
                                continue

                        # Obtain & Organize 
                        conn.get(file)
                        conn.close()
                        csvCount += 1

                        if os.path.isfile(f"{currPath}{file}"):
                            os.remove(f"{currPath}{file}")

                        shutil.move(file, currPath)
                    print(f"[Benchmark: {totalClients}] [Client: {ip}] - Downloaded {csvCount} result files")
                    time.sleep(5)

                    if not hasSysStats and WORKLOAD_TYPE != "BACKTEST":
                        # Fetch remote sysstat files to retrieve
                        txtFiles = serverGroup[0].run("ls *.txt", hide=True).stdout.split("\n")
    
                        # Download sysstat files
                        for file in txtFiles:
                            # Filter for .txt only 
                            if not file.endswith(".txt"):
                                continue
                
                            # Obtain & Organize 
                            serverGroup[0].get(file)
                            serverGroup[0].close()
                
                            if os.path.isfile(f"{currPath}{file}"):
                                os.remove(f"{currPath}{file}")
                
                            shutil.move(file, currPath)
                            time.sleep(5)
                        print(f"[Benchmark: {totalClients}] [Server] - Downloaded system stats files")
                        hasSysStats = True
                break
            except:
                time.sleep(5)
                pass
            
def unaryPlots(WORKLOAD_TYPE, DB_NAME, targetThreads, clientNodes, xLabel, logScale=False):
    numClients = [i * len(clientNodes) for i in targetThreads]
    clientDir = sorted([i for i in os.listdir(f"{DB_NAME.upper()}_{WORKLOAD_TYPE}")], key=lambda x:int(x.split("-")[-1]))
    benchData = {}
    
    for idx, i in enumerate(clientDir):
        if int(i.split('-')[1]) > 64:
            continue
        currCSVdata = []
        currClientDir = [cDir for cDir in os.listdir(f"{DB_NAME.upper()}_{WORKLOAD_TYPE}/{i}")]
        
        for nodeIdx, item in enumerate(currClientDir):
            myDir = f"{DB_NAME.upper()}_{WORKLOAD_TYPE}/{i}/{item}"
            csvFiles = os.listdir(myDir)
            csvFiles = [csvFile for csvFile in csvFiles if csvFile.endswith(".csv")]
            
            for csvFile in csvFiles:
                df = pd.read_csv(f"{myDir}/{csvFile}")
                currCSVdata.append(df["Time taken (ms)"].tolist())
        df = pd.DataFrame(zip(*currCSVdata))
        df["Mean"] = df.mean(axis=1)
        benchData[i] = df['Mean'].tolist()[1:]

    df = pd.DataFrame(benchData)
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
    # ax.yaxis.set_minor_locator(ticker.NullLocator())  # no minor ticks
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.legend(bbox_to_anchor=(0., -0.35, 1., 0.102), loc='lower center', mode='expand', ncols=3, borderaxespad=0, markerscale=3)
    plt.tight_layout(pad=0.15)
    plt.autoscale(axis='x')
    plt.savefig(f"{DB_NAME}_{WORKLOAD_TYPE}.svg", format='svg')

def batchPlots(WORKLOAD_TYPE, DB_NAME, targetThreads, clientNodes, batchSize=1000):
    numClients = [i * len(clientNodes) for i in targetThreads]
    clientDir = sorted([i for i in os.listdir(f"{DB_NAME.upper()}_{WORKLOAD_TYPE}")], key=lambda x:int(x.split("-")[-1]))
    benchData = {}

    for idx, i in enumerate(clientDir):
        if int(i.split('-')[1]) > 64:
            continue
        currCSVdata = []
        currClientDir = [cDir for cDir in os.listdir(f"{DB_NAME.upper()}_{WORKLOAD_TYPE}/{i}")]
        
        for nodeIdx, item in enumerate(currClientDir):
            myDir = f"{DB_NAME.upper()}_{WORKLOAD_TYPE}/{i}/{item}"
            csvFiles = os.listdir(myDir)
            csvFiles = [csvFile for csvFile in csvFiles if csvFile.endswith(".csv")]

            for csvFile in csvFiles:
                df = pd.read_csv(f'{myDir}/{csvFile}')
                currCSVdata.append(df['Time taken (ms)'].tolist())
        df = pd.DataFrame(zip(*currCSVdata))
        
        # If has value 0 -> Test must be partially/fully timed-out
        if (df.values == 0).any():
            df['Mean'] = 0
            benchData[i] = df['Mean'].tolist()
            continue
        
        # Otherwise, compute avg. time taken across all clients
        df['Mean'] = df.mean(axis=1)
        benchData[i] = df['Mean'].tolist()

    df = pd.DataFrame(benchData)
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

def poller(epochTime):
    while time.time() <= epochTime:
        time.sleep(1)
        
def checkTmuxJobIsDone(connectionGroup):
    # Keep checking if job is done running in tmux window
    while True:
        try:
            jobStatus = connectionGroup.run("tmux ls", hide=True)
            connectionGroup.close()
            time.sleep(10)
        except:
            connectionGroup.close()
            break