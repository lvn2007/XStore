cmake_minimum_required(VERSION 3.20)
project(pingPong)

set(CMAKE_CXX_STANDARD  17)

find_package(Protobuf REQUIRED)
include_directories()
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS message.proto)

# Single threaded
add_executable(sServer sServer.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(sClient sClient.cpp ${PROTO_SRCS} ${PROTO_HDRS})

# Multi-threaded
add_executable(pServer pServer.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(pClient pClient.cpp ${PROTO_SRCS} ${PROTO_HDRS})

TARGET_LINK_LIBRARIES(sServer ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(sClient ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(pServer ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(pClient ${Protobuf_LIBRARIES})