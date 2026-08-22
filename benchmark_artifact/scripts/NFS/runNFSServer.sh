# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

sudo systemctl restart nfs-kernel-server