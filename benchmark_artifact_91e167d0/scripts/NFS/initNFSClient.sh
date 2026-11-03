NFS_SERVER_ADDRESS="$1"

# Install dependencies of XStore
sudo apt install nfs-common sysstat -y

# Go to proper working dir
sudo mkdir -p /NFS_STORAGE
sudo chown -R nobody:nogroup /NFS_STORAGE
sudo chmod 777 /NFS_STORAGE