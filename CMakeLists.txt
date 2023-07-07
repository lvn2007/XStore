# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(XStore)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

include(FetchContent)

# ZStandard - sudo apt install libzstd-dev
set(Zstd_INCLUDE_DIR /usr/include)
mark_as_advanced(Zstd_INCLUDE_DIR)
#   Look for the necessary library
set(Zstd_LIBRARY /usr/lib/x86_64-linux-gnu/libzstd.so)
mark_as_advanced(Zstd_LIBRARY)
set(Zstd_FOUND 1)

set(Zstd_INCLUDE_DIRS ${Zstd_INCLUDE_DIR})
set(Zstd_LIBRARIES ${Zstd_LIBRARY})
if(NOT TARGET Zstd::Zstd)
    add_library(Zstd::Zstd UNKNOWN IMPORTED)
    set_target_properties(Zstd::Zstd PROPERTIES
            IMPORTED_LOCATION             "${Zstd_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Zstd_INCLUDE_DIR}")
endif()

find_package(Protobuf REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(hw_proto "protoFile/XStore.proto" ABSOLUTE)
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

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS protoFile/XStore.proto)

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

include_directories(.)
include_directories(include)
include_directories(src)
include_directories(protoFile)

# WITHOUT gRPC
add_executable(XStore src/main.cpp src/XStore/XStore.cpp include/XStore/XStore.h src/dataStructs/dataStruct.cpp
        include/dataStructs/dataStruct.h ${PROTO_SRCS} ${PROTO_HDRS} src/dataStructs/fileStruct.cpp
        include/dataStructs/fileStruct.h include/XStore_Networking/zmq.hpp include/XStore_Networking/zmq_addon.hpp
        include/XStore_Networking/XStore_zmqServer.h src/XStore_Networking/XStore_zmqServer.cpp
        include/XStore_Networking/zhelpers.hpp src/dataStructs/configStruct.cpp include/dataStructs/configStruct.h
        include/dataStructs/timeStruct.h src/dataStructs/timeStruct.cpp include/XStore_Networking/zstdpp.hpp)

add_compile_options(-O3)
target_link_libraries(XStore PUBLIC "zstd" "stdc++fs" ${Protobuf_LIBRARIES} "zmq" Threads::Threads)