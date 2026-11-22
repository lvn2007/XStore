source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WORKING_DIR="$1"

# Go to proper working dir
cd ${WORKING_DIR}

# Carbon-api
curl -s https://packagecloud.io/install/repositories/go-graphite/stable/script.deb.sh | sudo bash
sudo apt install carbonapi sysstat -y 

# Download Go-Carbon v0.19.1
wget https://github.com/go-graphite/go-carbon/releases/download/v0.19.1/go-carbon_0.19.1_amd64.deb

# Install Go-Carbon
sudo apt install ./go-carbon_0.19.1_amd64.deb

# Prepare dirs for Carbon
mkdir -p ${WORKING_DIR}/graphite/whisper

# Overwrite storage schemas
sudo cp ~/storage-schemas.conf /etc/go-carbon/storage-schemas.conf

# Configure firewall
openFirewallPorts 2003 2004