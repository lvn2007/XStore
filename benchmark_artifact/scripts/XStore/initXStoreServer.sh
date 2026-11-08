source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WORKING_DIR="$1"

# Install dependencies of XStore -- protobuf, libzmq and libsodium are no
# longer apt deps; they're fetched & built by CMake (FetchContent) during the
# compile step below. pkg-config is still required: libzmq's sodium detection
# calls find_package(PkgConfig REQUIRED) even when libsodium is in-tree.
# ccache is an optional compile cache CMake auto-detects (content-addressed,
# so cached builds always reflect the latest code); installing it here keeps
# the compile step below fast.
# Curtin leaves an apt proxy from imaging time (90curtin-aptproxy) pointing at a
# dead provisioning cache after the lease starts; apt then hangs on every fetch.
# Remove it and stop the periodic-apt jobs from racing the benchmark's own apt.
sudo rm -f /etc/apt/apt.conf.d/90curtin-aptproxy
sudo systemctl stop apt-daily.timer apt-daily-upgrade.timer 2>/dev/null
sudo systemctl disable apt-daily.timer apt-daily-upgrade.timer 2>/dev/null

sudo apt install build-essential cmake g++ git pkg-config sysstat ccache -y

# Go to proper working dir
cd ${WORKING_DIR}

# Clone XStore repo (Which includes client code as well)
sudo rm -rf XStore; git clone https://gitlab.com/lvn2007/XStore.git

# Compile (FetchContent pulls & builds the pinned protobuf v35.1 + libzmq)
cd XStore && bash build.sh

# Configure firewall
openFirewallPorts 9497