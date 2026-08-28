source "$(dirname "${BASH_SOURCE[0]}")/../_common.sh"

WORKING_DIR="$1"

# Go to proper working dir
cd ${WORKING_DIR}

# Add Influx repo & Install
curl -fsSL https://repos.influxdata.com/influxdata-archive_compat-exp2029.key | gpg --dearmor | sudo tee /usr/share/keyrings/influxdata-archive-keyring.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/influxdata-archive-keyring.gpg] https://repos.influxdata.com/debian stable main" | sudo tee /etc/apt/sources.list.d/influxdata.list
sudo rm -f /etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys DA61C26A0585BD3B

sudo apt-get update && sudo apt-get install influxdb sysstat -y
sudo dpkg --configure -a

# Run InfluxDB
sudo service influxdb start

# Change InfluxDB's default port to 9492 & bind to all interfaces
sudo sed -i 's/# bind-address = ":8086"/bind-address= ":9492"/' /etc/influxdb/influxdb.conf

# Change InfluxDB's default body size -- Avoiding `Request Entity Too Large` error
sudo sed -i 's/# max-body-size = 25000000/max-body-size = 0/' /etc/influxdb/influxdb.conf

# Disable cached memory limit
# sudo sed -i 's/# cache-max-memory-size = "1g"/cache-max-memory-size = 0/' /etc/influxdb/influxdb.conf

# If data will be stored on different drive
#  Otherwise, leave as default
if [ -d "$WORKING_DIR" ]; then
    # Create dirs for InfluxDB
    mkdir -p influxdb/meta
    mkdir -p influxdb/data
    mkdir -p influxdb/wal
    sudo chmod -R 777 ${WORKING_DIR}

    sedMetaTarget='s/\/var\/lib\/influxdb\/meta/\'"$WORKING_DIR\/influxdb\/meta"'/'
    sedDataTarget='s/\/var\/lib\/influxdb\/data/\'"$WORKING_DIR\/influxdb\/data"'/'
    sedWalTarget='s/\/var\/lib\/influxdb\/wal/\'"$WORKING_DIR\/influxdb\/wal"'/'
    
    sudo sed -i "${sedMetaTarget}" /etc/influxdb/influxdb.conf
    sudo sed -i "${sedDataTarget}" /etc/influxdb/influxdb.conf
    sudo sed -i "${sedWalTarget}" /etc/influxdb/influxdb.conf
fi

# Configure firewall
openFirewallPorts 9492

# Restart InfluxDB to reflect all changes
sudo service influxdb restart
