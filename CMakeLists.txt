# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Suppress warnings
add_definitions(-w)

# Project
project(XStore)

# Optimization flag
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

# Definitions for large files support
add_definitions(-D_LARGEFILE_SOURCE -D_LARGE_FILES -D_FILE_OFFSET_BITS=64)

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

# JSON
include_directories(include/JSON/json.hpp)

# Protobuf
find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Logging library - spdlog
include_directories(include/Logger)

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(XStore_proto "protoFile/XStore.proto" ABSOLUTE)
get_filename_component(XStore_proto_path "${XStore_proto}" PATH)

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
include_directories(include)
include_directories(src)

add_executable(XStore src/main.cpp src/XStore/XStore.cpp include/XStore/XStore.h src/DataStructs/dataStruct.cpp
        include/DataStructs/dataStruct.h ${PROTO_SRCS} ${PROTO_HDRS} src/DataStructs/fileStruct.cpp
        include/DataStructs/fileStruct.h include/Networking/zmq.hpp include/Networking/zmq_addon.hpp
        include/Networking/XStore_MetadataServer.h src/XStore_Networking/XStore_MetadataServer.cpp
        include/Networking/XStore_DataServer.h src/XStore_Networking/XStore_DataServer.cpp
        include/Networking/zhelpers.hpp src/DataStructs/configStruct.cpp include/DataStructs/configStruct.h
        include/DataStructs/timeStruct.h src/DataStructs/timeStruct.cpp include/JSON/json.hpp
        include/Utilities/Utilities.h src/Utilities/Utilities.cpp
        include/Compression/zstdpp.hpp
        include/Logger/Logger.h include/Argparse/argparse.hpp)

#target_link_libraries(XStore PUBLIC "stdc++fs" protobuf::libprotobuf "zmq" Threads::Threads absl::log_internal_check_op)
target_link_libraries(XStore PUBLIC "stdc++fs" protobuf::libprotobuf "zmq" Threads::Threads)