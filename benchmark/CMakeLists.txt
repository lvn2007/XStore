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
message(STATUS "Using protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(XStore_Proto "XStore.proto" ABSOLUTE)
get_filename_component(XStore_Proto "${XStore_Proto}" PATH)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ../protoFile/XStore.proto)

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

include_directories(.)


# WITHOUT gRPC
add_executable(benchmarkClient ${PROTO_SRCS} ${PROTO_HDRS} include/XStore_Client.h main.cpp include/zmq.hpp include/zmq_addon.hpp
        include/utilities.h include/Base_Adapter.h include/BS_thread_pool.hpp
        XStore/XStore_Adapter.h XStore/XStore_Adapter.cpp MongoDB/MongoDB_Adapter.h MongoDB/MongoDB_Adapter.cpp
        )
target_link_libraries(benchmarkClient PUBLIC ${Protobuf_LIBRARIES} "zmq")