# Install necessary libs for benchmark
# ccache is an optional compile cache CMake auto-detects (content-addressed,
# so cached builds always reflect the latest code); installing it here keeps
# the compile step below fast.
sudo apt install -y ca-certificates lsb-release wget python3-pip && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr "A-Z" "a-z")/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && sudo apt install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && sudo apt update && sudo apt install -y libparquet-dev && sudo apt install -y cmake g++ protobuf-compiler libprotobuf-dev libzmq3-dev ccache && sudo apt-get install libboost-all-dev -y && pip3 install pandas fastparquet && rm apache-arrow-apt-source-latest-*

# MongoDB C++ client library
sudo apt-get install libmongoc-dev -y 
curl -OL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.11.0/mongo-cxx-driver-r3.11.0.tar.gz && tar -xzf mongo-cxx-driver-r3.11.0.tar.gz
cd mongo-cxx-driver-r3.11.0/build && cmake .. -DCMAKE_BUILD_TYPE=Release -DMONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX=OFF && sudo cmake --build . --target install; cd ~

# CPR (InfluxCXX dependency)
git clone https://github.com/libcpr/cpr.git; cd cpr && git reset --hard 3fbe8028471663acb2ab5a68c7e75b6fc9b85557 && mkdir -p build && cd build && cmake .. -DCPR_USE_SYSTEM_CURL=ON -DBUILD_SHARED_LIBS=ON && cmake --build . --parallel && sudo cmake --install .  && sudo ldconfig; cd ~

# InfluxCXX
git clone https://github.com/offa/influxdb-cxx; cd influxdb-cxx && mkdir -p build && cd build

# Fixed timeout to 5 minutes
sed -i 's/{10}/{300}/' ../src/HTTP.cxx
cmake .. -DINFLUXCXX_TESTING:BOOL=OFF -DINFLUXCXX_WITH_BOOST:BOOL=ON && sudo make install; cd ~

# Clone XBench repo (Which includes client code as well) -- cloned to
# XBench_repo, not XBench, so the compiled XBench binary can be copied to
# ~/XBench below without colliding with the repo directory of the same name.
sudo rm -rf XBench_repo; git clone https://gitlab.com/lvn2007/XBench.git XBench_repo

# Compile XBench
cd XBench_repo && git submodule update --init && git submodule update --remote external/XStore && mkdir -p build && cd build && cmake .. && make -j && cp XBench ~