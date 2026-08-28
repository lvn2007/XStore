source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WORKING_DIR="$1"

# Install dependencies of XStore
sudo apt install build-essential cmake g++ libzmq3-dev sysstat -y

# Go to proper working dir
cd ${WORKING_DIR}

# Clone XStore repo (Which includes client code as well)
sudo rm -rf XStore; git clone https://gitlab.com/lvn2007/XStore.git

# Install protobuf - v35.1
git clone https://github.com/protocolbuffers/protobuf.git; cd protobuf && git reset --hard 35cd01f9fe9afbeea38cc7b979a3b6bfcde82c03 && git submodule update --init --recursive && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release -Dprotobuf_BUILD_SHARED_LIBS=ON -Dprotobuf_BUILD_TESTS=OFF && sudo make -j install && sudo ldconfig && cd ${WORKING_DIR}

# Compile
cd XStore && bash build.sh

# Configure firewall
openFirewallPorts 9497