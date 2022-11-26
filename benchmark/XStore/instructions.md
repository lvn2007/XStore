# XStore
## Configuring LXC container
1. lxd init
2. sudo lxc launch ubuntu:22.04 xstore-server 
3. lxc config device add xstore-server xstore-proxy proxy "listen=tcp:10.200.10.28:12345" connect=tcp:127.0.0.1:12345
4. sudo lxc config set xstore-server limits.memory 1GB
5. sudo lxc config set xstore-server limits.cpu 4

# MongoDB
## Configuring LXC container
1. lxd init
2. sudo lxc launch ubuntu:22.04 xstore-server
3. lxc config device add mongodb-server mongodb-proxy proxy "listen=tcp:10.200.10.181:27017" connect=tcp:127.0.0.1:27017
4. sudo lxc config set mongodb-server limits.memory 1GB
5. sudo lxc config set mongodb-server limits.cpu 4