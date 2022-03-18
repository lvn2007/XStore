# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(xstoreClient)

# C++ standard
set(CMAKE_CXX_STANDARD 17)
message(STATUS "Using C++ Standard: ${CMAKE_CXX_STANDARD}")

include(FetchContent)

# Protobuf-compiler
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

# Proto file
get_filename_component(hw_proto "xStore.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
#set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/xStore.pb.cc")
#set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/xStore.pb.h")
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

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS xStore.proto)

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

include_directories(.)

# WITHOUT gRPC
add_executable(xstoreClient_Protobuf ${PROTO_SRCS} ${PROTO_HDRS} xstoreClient_Protobuf.cpp)

if(${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS 9)
    target_link_libraries(xstoreClient_Protobuf PUBLIC ${Protobuf_LIBRARIES})
endif()

# WITH gRPC
#add_executable(XStore src/main.cpp src/XStore/XStore.cpp include/XStore/XStore.h src/dataStruct/dataStruct.cpp
#        include/dataStruct/dataStruct.h src/XStore_Networking/XStore_gRPC.cpp include/XStore_Networking/XStore_gRPC.h
#        ${PROTO_SRCS} ${PROTO_HDRS} ${hw_grpc_srcs} src/dataStruct/fileStruct.cpp include/dataStruct/fileStruct.h)

#if(${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS 9)
#    target_link_libraries(XStore PUBLIC "stdc++fs" ${Protobuf_LIBRARIES} ${_GRPC_GRPCPP})
#endif()