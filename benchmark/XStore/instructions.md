# XStore
## Configuring LXC container
1. sudo lxd init
2. sudo lxc launch ubuntu:22.04 xstore --vm 
3. sudo lxc config device add xstore xstore-proxy proxy "listen=tcp:10.200.10.28:12345" connect=tcp:127.0.0.1:12345
4. sudo lxc config set xstore limits.memory 1GB
5. sudo lxc config set xstore limits.cpu 4
6. sudo lxc config device override xstore root size="50GB"


Note:
* Copy data (sparse): rsync --sparse -avz --rsh='ssh -p2222' ubuntu@10.200.10.28:/home/ubuntu/XStore_Server/dataStore .
* For VM, forward port by (FROM HOST ONLY):
  * sudo iptables -t nat -A PREROUTING -p tcp --dport 2222 -j DNAT --to-destination 10.83.233.196:22
  * sudo iptables -t nat -A PREROUTING -p tcp --dport 12345 -j DNAT --to-destination 10.83.233.196:12345
  * sudo iptables -t nat -A POSTROUTING -j MASQUERADE
* Clear caches: sudo free && sudo sync && sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" && sudo free
* Ubuntu 20.04 (Chameleon) - Server:
  * CMake:
    * wget -c https://github.com/Kitware/CMake/releases/download/v3.25.1/cmake-3.25.1.tar.gz
    * tar -zxvf cmake-3.25.1.tar.gz && rm -r cmake-3.25.1.tar.gz
    * cd cmake-3.25.1/ && ./configure && make && sudo make install && cd ~
  * Protobuf: 
    * wget -c https://github.com/protocolbuffers/protobuf/releases/download/v3.12.4/protobuf-all-3.12.4.tar.gz
    * tar -zxvf protobuf-all-3.12.4.tar.gz && rm -r protobuf-all-3.12.4.tar.gz
    * cd protobuf-3.12.4/ && ./autogen.sh && ./configure && make && sudo make install 
    * which protoc && sudo ldconfig
  * Other:
    * sudo apt install build-essential g++ libzmq3-dev libtbb-dev
* ubuntu 20.04 (Chameleon) - Client:
  * pip3 install pyzmq pandas protobuf==3.20.*
# MongoDB
## Configuring LXC container
1. sudo lxd init
2. sudo lxc launch ubuntu:22.04 mongodb --vm
3. sudo lxc config device add mongodb mongodb-proxy proxy "listen=tcp:10.200.10.181:27017" connect=tcp:127.0.0.1:27017
4. sudo lxc config set mongodb-server limits.memory 1GB
5. sudo lxc config set mongodb-server limits.cpu 4