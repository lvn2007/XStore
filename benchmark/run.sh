# All target machines for experiments are declared here
targetNodes=("ubuntu@192.168.1.223")
arrayLength=${#targetNodes[@]}

# Pre-sets of libraries
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
      tmux send-keys -t "Node $idx" "$@"" ${targetNodes[idx]}:" C-m
    done
}

# Initializing tmux window according to list of targetNodes
initTmux

# Copy over XBench
scpSendFiles "scp ~/XStore/benchmark/build/benchmarkClient"

# Once all have been initialized -> Attach to master session
tmux attach -t XBench