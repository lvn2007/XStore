# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(benchmarkClient)

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

include(FetchContent)

# Protobuf-compiler
find_package(Protobuf REQUIRED)
find_package(Arrow REQUIRED)

# Logging library - spdlog
include_directories(include/Logger)

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# MongoDB
find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)
include_directories(${LIBMONGOCXX_INCLUDE_DIR})
include_directories(${LIBBSONCXX_INCLUDE_DIR})
include_directories("/usr/local/include/mongocxx/v_noabi")
include_directories("/usr/local/include/bsoncxx/v_noabi")
include_directories("/usr/local/include/libmongoc-1.0")
include_directories("/usr/local/include/libbson-1.0")
include_directories("/usr/local/lib")

# InfluxDB
find_package(InfluxDB)

# Proto file
get_filename_component(XStore_proto "../protoFile/XStore.proto" ABSOLUTE)
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

add_executable(benchmarkClient ${PROTO_SRCS} ${PROTO_HDRS} include/XStore_Client.h main.cpp include/zmq.hpp include/zmq_addon.hpp
        include/utilities.h include/Base_Adapter.h include/BS_thread_pool.hpp include/csv.hpp include/md5.h include/argparse.hpp
        include/Logger/Logger.h DataManager/Data_Manager.h DataManager/Data_Manager.cpp
        XStore/XStore_Adapter.h XStore/XStore_Adapter.cpp MongoDB/MongoDB_Adapter.h MongoDB/MongoDB_Adapter.cpp
        InfluxDB/InfluxDB_Adapter.h InfluxDB/InfluxDB_Adapter.cpp
        TimescaleDB/TimescaleDB_Adapter.h TimescaleDB/TimescaleDB_Adapter.cpp
        ProtoBench/ProtoBench.h ProtoBench/ProtoBench.cpp
)

target_link_libraries(benchmarkClient PUBLIC protobuf::libprotobuf "zmq" "parquet" "arrow"
        "mongo::mongocxx_shared" "mongo::bsoncxx_shared"
        InfluxData::InfluxDB
        pqxx)