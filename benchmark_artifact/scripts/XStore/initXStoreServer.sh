source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WORKING_DIR="$1"

# Install dependencies of XStore -- protobuf + libzmq are no longer apt deps;
# they're fetched & built by CMake (FetchContent) during the compile step below.
sudo apt install build-essential cmake g++ git sysstat -y

# Go to proper working dir
cd ${WORKING_DIR}

# Clone XStore repo (Which includes client code as well)
sudo rm -rf XStore; git clone https://gitlab.com/lvn2007/XStore.git

# Compile (FetchContent pulls & builds the pinned protobuf v35.1 + libzmq)
cd XStore && bash build.sh

# Configure firewall
openFirewallPorts 9497