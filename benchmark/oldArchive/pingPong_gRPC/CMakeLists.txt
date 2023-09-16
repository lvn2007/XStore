cmake_minimum_required(VERSION 3.20)
project(pingPong)

set(CMAKE_CXX_STANDARD  17)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
set(_GRPC_GRPCPP gRPC::grpc++)

if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Proto file
get_filename_component(hw_proto "message.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# gRPC stuffs
#   Generated sources
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/message.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/message.grpc.pb.h")
add_custom_command(
        OUTPUT "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
        DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(.)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS message.proto)

# gRPC - Client/Server executables
#   Single threaded
add_executable(sServer_gRPC sServer_gRPC.cpp ${PROTO_SRCS} ${PROTO_HDRS} ${hw_grpc_srcs})
add_executable(sClient_gRPC sClient_gRPC.cpp ${PROTO_SRCS} ${PROTO_HDRS} ${hw_grpc_srcs})

# Libs
TARGET_LINK_LIBRARIES(sServer_gRPC ${Protobuf_LIBRARIES} ${_GRPC_GRPCPP})
TARGET_LINK_LIBRARIES(sClient_gRPC ${Protobuf_LIBRARIES} ${_GRPC_GRPCPP})

# Copy entire dir to XStore-Master
execute_process (
        COMMAND bash -c "scp -r /home/ubuntu/pingPong_gRPC 172.20.3.113:/home/ubuntu/XStore/benchmark/"
        OUTPUT_VARIABLE outVar
)