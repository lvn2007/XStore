cmake_minimum_required(VERSION 3.20)
project(pingPong)

set(CMAKE_CXX_STANDARD  17)

find_package(Protobuf REQUIRED)
include_directories()
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS message.proto)

add_executable(server server.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(client client.cpp ${PROTO_SRCS} ${PROTO_HDRS})
TARGET_LINK_LIBRARIES(server ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(client ${Protobuf_LIBRARIES})