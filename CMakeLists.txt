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

include(FetchContent)

# Protobuf
find_package(absl REQUIRED)
find_package(Protobuf REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Logging library - spdlog
include_directories(include/Logger)

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(XStore_proto "ProtoFile/XStore.proto" ABSOLUTE)
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

add_executable(XStore src/main.cpp src/XStore/XStore.cpp include/XStore/XStore.h src/DataStructs/DataStruct.cpp
        include/DataStructs/DataStruct.h ${PROTO_SRCS} ${PROTO_HDRS} src/DataStructs/FileStruct.cpp
        include/DataStructs/FileStruct.h include/Networking/zmq.hpp include/Networking/zmq_addon.hpp
        include/Networking/ZMQ_Server.h src/XStore_Networking/ZMQ_Server.cpp
        include/Networking/zhelpers.hpp src/DataStructs/ConfigStruct.cpp include/DataStructs/ConfigStruct.h
        include/DataStructs/TimeStruct.h src/DataStructs/TimeStruct.cpp
        include/DataStructs/FileRangeLock.h src/DataStructs/FileRangeLock.cpp
        include/Logger/Logger.h)

target_link_libraries(XStore PUBLIC "stdc++fs" protobuf::libprotobuf "zmq" Threads::Threads)
if(TARGET absl::log_internal_check_op)
    target_link_libraries(XStore PUBLIC absl::log_internal_check_op)
endif()

# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------
option(XSTORE_BUILD_CLIENT "Build the ClientSrc C++ client library/example" ON)

if(XSTORE_BUILD_CLIENT)
    add_subdirectory(ClientSrc/Cpp)
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