# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Suppress warnings
add_definitions(-w)

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

add_executable(XStore src/main.cpp src/XStore/XStore.cpp include/XStore/XStore.h src/dataStructs/dataStruct.cpp
        include/dataStructs/dataStruct.h ${PROTO_SRCS} ${PROTO_HDRS} src/dataStructs/fileStruct.cpp
        include/dataStructs/fileStruct.h include/Networking/zmq.hpp include/Networking/zmq_addon.hpp
        include/Networking/XStore_zmqServer.h src/XStore_Networking/XStore_zmqServer.cpp
        include/Networking/zhelpers.hpp src/dataStructs/configStruct.cpp include/dataStructs/configStruct.h
        include/dataStructs/timeStruct.h src/dataStructs/timeStruct.cpp
        include/Compression/shoco.h src/Compression/shoco.cpp include/Compression/shoco_model.h include/Compression/zstdpp.hpp
        include/Logger/Logger.h)

add_compile_options(-O3)
target_link_libraries(XStore PUBLIC "stdc++fs" protobuf::libprotobuf "zmq" Threads::Threads)