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

# Sanitizers for debugging (uncomment when you need to find the bug)
# set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fsanitize=address -fsanitize=undefined")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")

# C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

# Definitions for large files support
add_definitions(-D_LARGEFILE_SOURCE -D_LARGE_FILES -D_FILE_OFFSET_BITS=64)

# WAL batching observability -- a compile-time-gated debug/benchmarking tool
# (per-shard batch-size counters + periodic log line), NOT permanent
# production instrumentation. Off by default: zero cost, nothing compiled
# in. Turn on only for a benchmarking session:
#   cmake -B build -DXSTORE_WAL_STATS=ON
option(XSTORE_WAL_STATS "Enable WAL batching observability counters (compile-time debug/benchmark tool, off by default)" OFF)
if(XSTORE_WAL_STATS)
    add_compile_definitions(XSTORE_WAL_STATS_ENABLED)
endif()

include(FetchContent)

FetchContent_Declare(
        moodycamel_concurrentqueue
        GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
        GIT_TAG v1.0.4
)
FetchContent_MakeAvailable(moodycamel_concurrentqueue)

add_library(moodycamel INTERFACE)
target_include_directories(moodycamel INTERFACE ${moodycamel_concurrentqueue_SOURCE_DIR})

# Protobuf
find_package(absl REQUIRED)
find_package(Protobuf REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Vendored third-party headers (spdlog, cppzmq)
include_directories(third_party)

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(XStore_proto "proto/XStore.proto" ABSOLUTE)
get_filename_component(XStore_proto_path "${XStore_proto}" PATH)

set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/XStore.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/XStore.pb.h")

add_custom_command(
        OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}"
        COMMAND ${_PROTOBUF_PROTOC} "--experimental_allow_proto3_optional"
        ARGS --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${XStore_proto_path}" "${XStore_proto}"
        DEPENDS "${XStore_proto}")

# Include generated *.pb.h files
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

include_directories(.)
include_directories(include)
include_directories(src)

add_executable(XStore src/main.cpp
        include/XStore/XStore_Internal.h src/XStore/XStore_Internal.cpp
        src/XStore/XStore_Management.cpp
        src/XStore/XStore_IO.cpp
        src/XStore/XStore_Wal.cpp
        src/XStore/XStore_SysStats.cpp
        include/XStore/XStore_SysStats.h
        src/XStore/XStore_UnaryOps.cpp
        src/XStore/XStore_BatchOps.cpp
        src/XStore/XStore_RangeOps.cpp
        src/XStore/XStore_Aggregates.cpp
        include/XStore/XStore.h
        include/DataStructs/ValueStruct.h ${PROTO_SRCS} ${PROTO_HDRS}
        third_party/zmq.hpp third_party/zmq_addon.hpp
        include/Networking/ZMQ_Server.h src/XStore_Networking/ZMQ_Server.cpp
        third_party/zhelpers.hpp
        third_party/argparse/argparse.hpp
        include/DataStructs/TimeStruct.h src/DataStructs/TimeStruct.cpp
        include/DataStructs/FileRangeLock.h src/DataStructs/FileRangeLock.cpp
        include/Logger/Logger.h)

target_link_libraries(XStore PUBLIC "stdc++fs" protobuf::libprotobuf "zmq" Threads::Threads moodycamel)
if(TARGET absl::log_internal_check_op)
    target_link_libraries(XStore PUBLIC absl::log_internal_check_op)
endif()

# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------
option(XSTORE_BUILD_CLIENT "Build the client_src C++ client library/example" ON)

if(XSTORE_BUILD_CLIENT)
    add_subdirectory(client_src/Cpp)
endif()

option(XSTORE_BUILD_TESTS "Build XStore test suite" ON)

if(XSTORE_BUILD_TESTS)
    FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.17.0
    )
    FetchContent_MakeAvailable(googletest)

    enable_testing()
    add_subdirectory(tests)
endif()