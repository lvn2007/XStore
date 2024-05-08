# All target machines for experiments are declared here
targetNodes=("ubuntu@172.16.6.6" "ubuntu@172.16.6.7" "ubuntu@172.16.6.8" "ubuntu@172.16.6.9")
arrayLength=${#targetNodes[@]}

# Pre-sets of libraries
updateUpgrade="sudo apt update && sudo apt upgrade -y"
xbenchLibs='sudo apt install -y ca-certificates lsb-release wget && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr "A-Z" "a-z")/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && sudo apt install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && sudo apt update && sudo apt install -y libparquet-dev && sudo apt install -y cmake g++ protobuf-compiler libprotobuf-dev libzmq3-dev && pip3 install pandas fastparquet'

function initTmux() {
  # Start new tmux session
  tmux new-session -d -s XBench -n Master

  # Loop & Create N windows
  for (( idx=0; idx < arrayLength; idx++ ))
  do
    tmux new-window -t XBench -n "Node $idx"
  done
}

function initSSH() {
  # Start new tmux session
  tmux new-session -d -s XBench -n Master

  # Loop & Create N windows
  for (( idx=0; idx < arrayLength; idx++ ))
  do
    tmux new-window -t XBench -n "Node $idx" -d "ssh ${targetNodes[idx]}"
  done
}

# Sending CMD to target nodes
function sendCMD() {
  for (( idx=0; idx < arrayLength; idx++ ))
  do
    tmux send-keys -t "Node $idx" "$@" C-m
  done
}

# Sending CMD to specific node
function sendCMD_Singular() {
  tmux send-keys -t "$@" "$@" C-m
}

function scpSendFiles() {
  for (( idx=0; idx < arrayLength; idx++ ))
    do
      tmux send-keys -t "Node $idx" "scp $@"" ${targetNodes[idx]}:" C-m
    done
}

function scpRecvBenchResults() {
  for (( idx=0; idx < arrayLength; idx++ ))
      do
        mkdir -p "Node-$idx" && tmux send-keys -t "Master" "scp ${targetNodes[idx]}:*.csv Node-$idx/" C-m
      done
}

function sshAllNodes() {
  for (( idx=0; idx < arrayLength; idx++ ))
    do
      tmux send-keys -t "Node $idx" "ssh ${targetNodes[idx]}" C-m
    done
}

function listTargetNodes() {
  echo "--- Listing all target nodes"

  for (( idx=0; idx < arrayLength; idx++ ))
      do
        echo "Node $idx: ${targetNodes[idx]}"
      done
  echo ""
}

# MAIN
while :
do
  echo "XBench runner. Please choose one of the following options:"
  echo " 1. List all target nodes"
  echo " 2. Send commands"
  echo " 3. Install XBench libraries"
  echo " 4. Fetch & Compile benchmarkClient binary"
  echo " 5. Generate hash data at all target nodes"
  echo " 6. Generate workload"
  echo " 7. LOAD all data"
  echo " 8. READ benchmarks"
  echo " 9. WRITE benchmarks"
  echo " 0. Exit"
  read -p "" inputChoice

  if [ "$inputChoice" -eq 0 ]
  then
    break
  fi

  # 1. List all target nodes
  if [ "$inputChoice" -eq 1 ]
  then
    listTargetNodes
  fi

  # 2. Sending custom commands
  if [ "$inputChoice" -eq 2 ]
    then
    # Initializing tmux window according to list of targetNodes
    initSSH

    # Parse input CMDs and send to all target nodes
    read -p "Enter command: " cmdToSend
    sendCMD "${cmdToSend}"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 3. Installing XBench libs
  if [ "$inputChoice" -eq 3 ]
    then
    # Initializing tmux window according to list of targetNodes
    initSSH

    # Copy over XBench
    sendCMD "${xbenchLibs}"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 4. Transfer over benchmarkClient binary
  if [ "$inputChoice" -eq 4 ]
  then
    # Initializing tmux window according to list of targetNodes
    initSSH

    # Copy over XBench
    sendCMD "git clone https://gitlab.com/lvn2007/XStore.git; cd XStore/benchmark && mkdir -p build && cd build && cmake .. && make && cp benchmarkClient ~ && cp ../DataManager/Parquet_OPS.py ~"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 5. Generate hash data at all target nodes
  if [ "$inputChoice" -eq 5 ]
  then
    # Initializing tmux window according to list of targetNodes
    initSSH

    # Specific parameters of Generate hash
    echo "--- Generate hash data at all target nodes"
    echo "Hash dataset parameter configurations:"
    read -p "Start time: " startTime
    read -p "End time: " endTime
    read -p "Column count: " columnCount
    read -p "Outfile name: " outfileName

    sendCMD "python3 Parquet_OPS.py generate -s $startTime -e $endTime -c $columnCount -o $outfileName"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 6. Generate workload
  if [ "$inputChoice" -eq 6 ]
  then
    # Initializing tmux window
    initSSH

    # Specific parameters of Generate hash
    echo "Operation type: "
    echo " 1. Query"
    echo " 2. Insert"
    read -p "" operationType

    # QUERY workloads
    if [ "$operationType" = "1" ]
    then
      echo "--- "
      read -r -p "Workload type: " workloadType
      read -r -p "Offset: " offset
      read -r -p "Sample size: " sampleSize
      read -r -p "Number of clients: " numClients
      read -r -p "Start time: " startTime
      read -r -p "End time: " endTime
      read -r -p "Batch iter: " batchIter

      echo "./parquetOPS.py queryWorkload -t $workloadType -o $offset -s $sampleSize -n $numClients -st $startTime -et $endTime -b $batchIter"
#      sendCMD "./parquetOPS.py queryWorkload -t $workloadType -o $offset -s $sampleSize -n $numClients -st $startTime -et $endTime -b $batchIter"
    fi

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 7. LOAD all data
  if [ "$inputChoice" -eq 7 ]
      then
        # Initializing tmux window according to list of targetNodes
        initSSH

        # Specific parameters of Generate hash
        echo "--- Load All data from file"
        echo "Data loader parameter:"
        read -p "Server IP Addr: " ipAddr
        read -p "Server Port No.: " portNo
        read -p "DB Name: " dbName
        read -p "Thread Count: " threadCount
        read -p "Filename: " fileName

        sendCMD_Singular "Node 0" "./benchmarkClient readBench -i $ipAddr -p $portNo -d $dbName -t $threadCount -f $fileName"

        # Once work is done -> Attach to master session
        tmux attach -t XBench
        exit
    fi

  # 8. READ benchmark
  if [ "$inputChoice" -eq 8 ]
    then
      # Initializing tmux window according to list of targetNodes
      initSSH

      # Remove any existing bench results
      sendCMD "rm *.csv"

      # Drop any cache at client side
      sendCMD "free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null"

      # Specific parameters of Generate hash
      echo "--- READ Benchmark"
      echo "READ Benchmark parameter configurations:"
      read -p "Server IP Addr: " ipAddr
      read -p "Server Port No.: " portNo
      read -p "DB Name: " dbName
      read -p "Thread Count: " threadCount
      read -p "Experiment type: " experimentType
      read -p "No. of Iterations: " nIteration
      read -p "No. of Batch Iterations: " bIteration
      read -p "Invoke at: " invokeAt

      sendCMD "./benchmarkClient queryBench -i $ipAddr -p $portNo -d $dbName -t $threadCount -e $experimentType -n $nIteration -b $bIteration -ia $invokeAt"

      # Poll until user tells to fetch bench results
      while :
      do
        read -p "Press (y) to fetch bench results: " fetchBench

        if [ "$fetchBench" = "y" ]
        then
          break
        fi
      done

      # Download benchmark results from all target nodes back into Master
      scpRecvBenchResults

      # Once work is done -> Attach to master session
      tmux attach -t XBench
      exit
  fi

  # 9. WRITE benchmark
  if [ "$inputChoice" -eq 9 ]
    then
      # Initializing tmux window according to list of targetNodes
      initSSH

      # Remove any existing bench results
      sendCMD "rm *.csv"

      # Drop any cache at client side
      sendCMD "free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null"

      # Specific parameters of Generate hash
      echo "--- WRITE Benchmark"
      echo "WRITE Benchmark parameter configurations:"
      read -p "Server IP Addr: " ipAddr
      read -p "Server Port No.: " portNo
      read -p "DB Name: " dbName
      read -p "Thread Count: " threadCount
      read -p "Experiment type: " experimentType
      read -p "No. of Iterations: " nIteration
      read -p "No. of Batch Iterations: " bIteration
      read -p "Invoke at: " invokeAt

      sendCMD "./benchmarkClient insertBench -i $ipAddr -p $portNo -d $dbName -t $threadCount -e $experimentType -n $nIteration -b $bIteration -ia $invokeAt"

      # Poll until user tells to fetch bench results
      while :
      do
        read -p "Press (y) to fetch bench results: " fetchBench

        if [ "$fetchBench" = "y" ]
        then
          break
        fi
      done

      # Download benchmark results from all target nodes back into Master
      scpRecvBenchResults

      # Once work is done -> Attach to master session
      tmux attach -t XBench
      exit
  fi

done