# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(benchmarkClient)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

include(FetchContent)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_compile_options(-O3)

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(.)

# WITHOUT gRPC
add_executable(XStore_Client XStore_Client.cpp XStore_Client.h zmq.hpp zmq_addon.hpp)
target_link_libraries(XStore_Client PUBLIC "zmq")