# Make build directory
mkdir -p build && cd build

# CMake & Build
cmake .. || exit && make -j || exit && cd ..

# Prepare environment for XStore server
mkdir -p ../XStore_Server ../XStore_Server/dataStore && cp -r build ../XStore_Server