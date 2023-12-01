# All target machines for experiments are declared here
#targetNodes=("ubuntu@192.168.1.223")
targetNodes=("cc@129.114.108.188" "cc@129.114.109.135" "cc@129.114.108.134" "cc@129.114.108.255" "cc@129.114.109.86"
"cc@129.114.109.158" "cc@129.114.109.131" "cc@129.114.109.98")
arrayLength=${#targetNodes[@]}

# Pre-sets of libraries
updateUpgrade="sudo apt update && sudo apt upgrade -y"
commonLibs="sudo apt install libparquet-dev"

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
  echo " 4. Transfer over benchmarkClient binary"
  echo " 5. Generate hash data at all target nodes"
  echo " 6. READ benchmarks"
  echo " 7. WRITE benchmarks"
  echo " 0. Exit"
  read -p "" inputChoice

  if [ $inputChoice -eq 0 ]
  then
    break
  fi

  # 1. List all target nodes
  if [ $inputChoice -eq 1 ]
  then
    listTargetNodes
  fi

  # 2. Sending custom commands
  if [ $inputChoice -eq 2 ]
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

  # 3. Sending custom commands
  if [ $inputChoice -eq 3 ]
    then
    # Initializing tmux window according to list of targetNodes
    initSSH

    # Copy over XBench
    sendCMD "${commonLibs}"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 4. Transfer over benchmarkClient binary
  if [ $inputChoice -eq 4 ]
  then
    # Initializing tmux window according to list of targetNodes
    initTmux

    # Copy over XBench
    scpSendFiles "~/XStore/benchmark/build/benchmarkClient"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 5. Generate hash data at all target nodes
  if [ $inputChoice -eq 5 ]
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

    sendCMD "./benchmarkClient generate -s $startTime -e $endTime -c $columnCount -o $outfileName"

    # Once work is done -> Attach to master session
    tmux attach -t XBench
    exit
  fi

  # 6. READ benchmark
  if [ $inputChoice -eq 6 ]
    then
      # Initializing tmux window according to list of targetNodes
      initSSH

      # Specific parameters of Generate hash
      echo "--- READ Benchmark"
      echo "READ Benchmark parameter configurations:"
      read -p "Server IP Addr: " ipAddr
      read -p "Server Port No.: " portNo
      read -p "DB Name: " dbName
      read -p "Thread Count: " threadCount
      read -p "Experiment type: " experimentType
      read -p "No. of Iterations: " nIteration
      read -p "Start time range: " startTime
      read -p "End time range: " endTime
      read -p "Invoke at: " invokeAt

#        sendCMD "./benchmarkClient readBench -i $ipAddr -p $portNo -d $dbName -t $threadCount -e $experimentType -n $nIteration -s $startTime -en $endTime -ia $invokeAt"

      # Remove when done testing
      sendCMD "./benchmarkClient readBench -i 192.168.1.147 -p 12345 -d XSTORE -t 4 -e QUERY_A -n 1000 -s 946684800 -en 946694800 -ia 1700989990"

      # Download benchmark results from all target nodes back into Master
      scpRecvBenchResults

      # Once work is done -> Attach to master session
      tmux attach -t XBench
      exit
  fi
done