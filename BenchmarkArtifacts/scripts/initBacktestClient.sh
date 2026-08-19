# Python packages for Backtesting client
sudo apt install python3-pip -y 
pip3 install pandas pyarrow pyzmq protobuf pymongo influxdb

# Python Proto files for Backtesting client
sudo rm -rf XStore && git clone https://gitlab.com/lvn2007/XStore/ && cp -r XStore/ClientSrc/Python/*.py .