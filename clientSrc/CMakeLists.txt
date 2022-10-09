# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(xstoreClient)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

include(FetchContent)

find_package(Protobuf REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(hw_proto "XStore.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

add_custom_command(
        OUTPUT "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
        DEPENDS "${hw_proto}")

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS XStore.proto)

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(.)

# WITHOUT gRPC
add_executable(XStore_Client ${PROTO_SRCS} ${PROTO_HDRS} XStore_Client.cpp XStore_Client.h zmq.hpp zmq_addon.hpp)
target_link_libraries(XStore_Client PUBLIC ${Protobuf_LIBRARIES} "zmq")