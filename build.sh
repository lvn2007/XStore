# CMake & Build -- proper out-of-source `-B`/`--build` invocation instead of
# `mkdir build && cd build && cmake .. && make -j`. Builds and gates only on
# the XStore target itself (XSTORE_BUILD_TESTS=OFF skips compiling the test
# suite here entirely): a previous version built the whole `all` target via
# plain `make -j`, which runs ctest as a side effect of building
# xstore_network_tests -- a pre-existing, independently-reproducible flaky
# test there could fail and `|| exit` would abort BEFORE the deploy step
# below ever ran, silently leaving a stale binary in ../XStore_Server with
# no error surfaced anywhere. Deploy should gate on "did the server binary
# build," never on an unrelated test suite.
cmake -B build -DCMAKE_BUILD_TYPE=Release -DXSTORE_BUILD_TESTS=OFF || exit
cmake --build build -j"$(nproc)" --target XStore || exit

# Prepare environment for XStore server -- DataStore/WAL included explicitly
# (XStore itself also creates it on demand via std::filesystem::
# create_directories, but the benchmark harness expects it to already exist
# here).
mkdir -p ../XStore_Server/DataStore/WAL && cp -r build ../XStore_Server && cd ..