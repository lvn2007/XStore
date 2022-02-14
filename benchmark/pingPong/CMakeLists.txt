cmake_minimum_required(VERSION 3.20)
project(pingPong)

set(CMAKE_CXX_STANDARD  17)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(Protobuf REQUIRED)
include_directories()
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS message.proto)

# Single threaded
add_executable(sServer_Protobuf sServer_Protobuf.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(sClient_Protobuf sClient_Protobuf.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(sServer_Raw sServer_Raw.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(sClient_Raw sClient_Raw.cpp ${PROTO_SRCS} ${PROTO_HDRS})

# Multi-threaded
add_executable(pServer pServer.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(pClient pClient.cpp ${PROTO_SRCS} ${PROTO_HDRS})

TARGET_LINK_LIBRARIES(sServer_Protobuf ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(sClient_Protobuf ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(sServer_Raw ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(sClient_Raw ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(pServer ${Protobuf_LIBRARIES} Threads::Threads)
TARGET_LINK_LIBRARIES(pClient ${Protobuf_LIBRARIES})

# Copy entire dir to XStore-Master
execute_process (
        COMMAND bash -c "scp -r /home/ubuntu/pingPong 172.20.3.113:/home/ubuntu/XStore/benchmark/"
        OUTPUT_VARIABLE outVar
)