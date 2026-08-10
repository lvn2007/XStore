# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Buildable standalone (cd ClientSrc/Cpp && cmake .) or pulled in via
# add_subdirectory() from the root XStore CMakeLists -- only declare our own
# project() when nobody above us already has.
if(NOT CMAKE_PROJECT_NAME)
    project(XStoreClient CXX)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT TARGET protobuf::libprotobuf)
    find_package(Protobuf REQUIRED)
endif()
find_package(Threads REQUIRED)

# Proto file lives once at the repo root (ProtoFile/XStore.proto) -- generate
# our own copy of the C++ bindings rather than depending on the server
# target's build directory.
get_filename_component(XSTORE_CLIENT_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/../../ProtoFile/XStore.proto" ABSOLUTE)
get_filename_component(XSTORE_CLIENT_PROTO_DIR "${XSTORE_CLIENT_PROTO}" PATH)

set(XSTORE_CLIENT_PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/XStore.pb.cc")
set(XSTORE_CLIENT_PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/XStore.pb.h")

add_custom_command(
        OUTPUT "${XSTORE_CLIENT_PROTO_SRCS}" "${XSTORE_CLIENT_PROTO_HDRS}"
        COMMAND $<TARGET_FILE:protobuf::protoc>
        ARGS --experimental_allow_proto3_optional
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${XSTORE_CLIENT_PROTO_DIR}" "${XSTORE_CLIENT_PROTO}"
        DEPENDS "${XSTORE_CLIENT_PROTO}")

add_library(XStoreClientProto STATIC ${XSTORE_CLIENT_PROTO_SRCS} ${XSTORE_CLIENT_PROTO_HDRS})
target_link_libraries(XStoreClientProto PUBLIC protobuf::libprotobuf)
target_include_directories(XStoreClientProto PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")

# Header-only client (XStore_Client.h) -- exposed as an INTERFACE target so
# other CMake targets can just target_link_libraries(... XStoreClient).
add_library(XStoreClient INTERFACE)
target_include_directories(XStoreClient INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(XStoreClient INTERFACE XStoreClientProto "zmq" Threads::Threads absl::log_internal_check_op)

# Smoke-build target: proves the header compiles and links standalone.
add_executable(XStore_Client_Example example/XStore_Client_Example.cpp)
target_link_libraries(XStore_Client_Example PRIVATE XStoreClient)
