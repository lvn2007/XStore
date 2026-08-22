WORKING_DIR="$1"

# Install NFS
sudo apt install nfs-kernel-server sysstat -y

# Clone XStore repo (Which includes client code as well)
echo "${WORKING_DIR}    *(rw,sync,no_root_squash,no_subtree_check)" | sudo tee /etc/exports

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

sudo systemctl restart nfs-kernel-server
sleep 5