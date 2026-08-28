source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WORKING_DIR="$1"
IS_MANUAL="$2"

# Installing sysstat monitoring
sudo apt install sysstat -y

# Go to proper working dir
cd ${WORKING_DIR}

# IIT network blocks Mystic from DNS resolving MongoDB ...
if [ "$IS_MANUAL" == "true" ]; then
    # Manually maps domain with IP
    echo "52.85.247.37 fastdl.mongodb.org" | sudo tee -a /etc/hosts
    echo "18.154.185.7 repo.mongodb.org" | sudo tee -a /etc/hosts
    echo "18.154.110.123 downloads.mongodb.com" | sudo tee -a /etc/hosts

    # Download
    wget https://repo.mongodb.org/apt/ubuntu/dists/jammy/mongodb-org/8.0/multiverse/binary-amd64/mongodb-org-mongos_8.0.3_amd64.deb
    wget https://repo.mongodb.org/apt/ubuntu/dists/jammy/mongodb-org/8.0/multiverse/binary-amd64/mongodb-org-server_8.0.3_amd64.deb
    wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.10.0.deb
    wget https://downloads.mongodb.com/compass/mongodb-mongosh_2.3.0_amd64.deb
    
    # Install
    sudo dpkg -i mongodb-org-mongos_8.0.3_amd64.deb
    sudo dpkg -i mongodb-org-server_8.0.3_amd64.deb
    sudo dpkg -i mongodb-database-tools-ubuntu2204-x86_64-100.10.0.deb
    sudo dpkg -i mongodb-mongosh_2.3.0_amd64.deb
else
    # Import public key 
    curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
    
    # Create a list file for MongoDB
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
    
    # Reload local package database & # Install MongoDB packages
    sudo apt-get update && sudo apt-get install -y mongodb-org
fi

# Start & Run MongoDB on start-up
sudo systemctl start mongod && sudo systemctl enable mongod

# Change MongoDB's default port to 9491 & bind to all interfaces
sudo sed -i 's/27017/9491/' /etc/mongod.conf
sudo sed -i 's/127.0.0.1/0.0.0.0/' /etc/mongod.conf

# If data will be stored on different drive
#  Otherwise, leave as default
if [ -d "$WORKING_DIR" ]; then
    sedTarget='s/\/var\/lib\/mongodb/\'"$WORKING_DIR"'/'
    sudo sed -i "${sedTarget}" /etc/mongod.conf
fi

# Configure firewall
openFirewallPorts 9491

# Restart MongoDB to reflect all changes
sudo systemctl restart mongod