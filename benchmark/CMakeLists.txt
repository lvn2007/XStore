# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(benchmarkClient)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

include(FetchContent)

# Protobuf-compiler
find_package(Protobuf REQUIRED)
find_package(Arrow REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(XStore_Proto "XStore.proto" ABSOLUTE)
get_filename_component(XStore_Proto "${XStore_Proto}" PATH)

set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/XStore.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/XStore.pb.h")

add_custom_command(
        OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}"
        COMMAND ${_PROTOBUF_PROTOC} "--experimental_allow_proto3_optional"
        ARGS --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${XStore_proto_path}" "${XStore_proto}"
        DEPENDS "${XStore_proto}")

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

include_directories(.)

add_executable(benchmarkClient ${PROTO_SRCS} ${PROTO_HDRS} include/XStore_Client.h main.cpp include/zmq.hpp include/zmq_addon.hpp
        include/utilities.h include/Base_Adapter.h include/BS_thread_pool.hpp include/csv.hpp include/md5.h include/argparse.hpp
        DataManager/Data_Manager.h DataManager/Data_Manager.cpp
        XStore/XStore_Adapter.h XStore/XStore_Adapter.cpp MongoDB/MongoDB_Adapter.h MongoDB/MongoDB_Adapter.cpp
        )

add_compile_options(-O3)
target_link_libraries(benchmarkClient PUBLIC ${Protobuf_LIBRARIES} "zmq" "parquet" "arrow") # libparquet-dev