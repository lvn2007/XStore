# Minimum CMake required
cmake_minimum_required(VERSION 3.20)

# Project
project(XStore)

# Optimization flag
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Warnings. -Wall -Wextra for all targets in this tree; in practice the
# generated protobuf translation units and the vendored third-party headers
# don't trigger these warnings, so the build stays clean without masking real
# warnings in the project's own code. If a future protobuf bump or dependency
# starts warning, exempt it per-file via set_source_files_properties(...)
# COMPILE_OPTIONS -w rather than re-adding a blanket -w.
add_compile_options(-Wall -Wextra)
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

# ------------------------------------------------------------------
# Dependencies (protobuf, libzmq) -- self-contained by default
# ------------------------------------------------------------------
# Both are fetched & built from source (pinned) so a fresh machine needs only
# a C/C++ toolchain, cmake and git -- no `apt install libprotobuf-dev
# libzmq3-dev`, no hand-built protobuf. Flip the switch OFF to use a
# preinstalled copy instead (protobuf must install protobuf-config.cmake so
# the protobuf::libprotobuf / protobuf::protoc imported targets exist; zmq
# just needs libzmq + <zmq.h>).

# --- Protobuf (pinned to the v35.1 release tag == commit 35cd01f) ---
option(XSTORE_FETCH_PROTOBUF "Fetch & build protobuf from source instead of a system install" ON)

if(TARGET protobuf::libprotobuf)
    message(STATUS "Protobuf: using pre-existing protobuf target")
elseif(XSTORE_FETCH_PROTOBUF)
    set(protobuf_BUILD_TESTS       OFF CACHE BOOL "" FORCE)
    set(protobuf_BUILD_EXAMPLES    OFF CACHE BOOL "" FORCE)
    set(protobuf_BUILD_CONTRIB     OFF CACHE BOOL "" FORCE)
    set(protobuf_BUILD_SHARED_LIBS ON  CACHE BOOL "" FORCE)
    # Force protobuf to fetch & build its own pinned abseil/zlib/upb instead of
    # picking up a possibly-older system copy (an old system absl lacks newer
    # targets like absl::absl_check and breaks the link).
    set(protobuf_FORCE_FETCH_DEPENDENCIES ON CACHE BOOL "" FORCE)
    FetchContent_Declare(
            protobuf
            GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
            GIT_TAG v35.1
            GIT_SHALLOW ON
    )
    FetchContent_MakeAvailable(protobuf)
    message(STATUS "Protobuf: fetched & building from source (v35.1)")
else()
    find_package(Protobuf CONFIG REQUIRED)
    message(STATUS "Protobuf: using system install ${Protobuf_VERSION}")
endif()

# --- libsodium (pinned to the 1.0.18 release) ---
# Needed only by libzmq's CURVE support (WITH_LIBSODIUM, below). libsodium
# ships no CMake build in its releases (autotools only), so it is configured
# & built out-of-band at configure time -- a one-time, ~minute step cached by
# the presence of the built library (same pattern XBench uses for OpenSSL).
# The SODIUM_* variables are pre-set so libzmq's Findsodium.cmake
# (find_path/find_library) skips its search and uses these instead of
# requiring a system libsodium-dev.
option(XSTORE_FETCH_SODIUM "Fetch & build libsodium from source instead of a system install" ON)

if(TARGET libsodium)
    message(STATUS "libsodium: using pre-existing libsodium target")
elseif(XSTORE_FETCH_SODIUM)
    FetchContent_Declare(
            libsodium
            URL https://github.com/jedisct1/libsodium/releases/download/1.0.18-RELEASE/libsodium-1.0.18.tar.gz
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_GetProperties(libsodium)
    if(NOT libsodium_POPULATED)
        FetchContent_Populate(libsodium)
    endif()
    set(_xs_sodium_prefix ${CMAKE_BINARY_DIR}/_deps/libsodium-install)
    if(NOT EXISTS "${_xs_sodium_prefix}/lib/libsodium.a")
        message(STATUS "libsodium: building from source (1.0.18, static) -- one-time step")
        # --with-pic is required: libzmq is built as a shared library and
        # links this static archive into libzmq.so. Without it libtool puts
        # non-PIC objects in libsodium.a and the link fails with
        # "relocation R_X86_64_PC32 ... can not be used when making a shared
        # object".
        execute_process(
                COMMAND ${libsodium_SOURCE_DIR}/configure --prefix=${_xs_sodium_prefix}
                        --enable-shared=no --enable-static=yes --with-pic
                WORKING_DIRECTORY ${libsodium_SOURCE_DIR}
                RESULT_VARIABLE _xs_sodium_result)
        if(NOT _xs_sodium_result EQUAL 0)
            message(FATAL_ERROR
                "libsodium configure step failed. "
                "Delete ${CMAKE_BINARY_DIR}/_deps/libsodium-* and reconfigure.")
        endif()
        execute_process(
                COMMAND make -j4
                WORKING_DIRECTORY ${libsodium_SOURCE_DIR}
                RESULT_VARIABLE _xs_sodium_result)
        if(NOT _xs_sodium_result EQUAL 0)
            message(FATAL_ERROR
                "libsodium build step failed. "
                "Delete ${CMAKE_BINARY_DIR}/_deps/libsodium-* and reconfigure.")
        endif()
        execute_process(
                COMMAND make install
                WORKING_DIRECTORY ${libsodium_SOURCE_DIR}
                RESULT_VARIABLE _xs_sodium_result)
        if(NOT _xs_sodium_result EQUAL 0)
            message(FATAL_ERROR
                "libsodium install step failed. "
                "Delete ${CMAKE_BINARY_DIR}/_deps/libsodium-* and reconfigure.")
        endif()
    endif()
    set(SODIUM_INCLUDE_DIRS "${_xs_sodium_prefix}/include" CACHE PATH "" FORCE)
    set(SODIUM_LIBRARIES "${_xs_sodium_prefix}/lib/libsodium.a" CACHE FILEPATH "" FORCE)
    message(STATUS "libsodium: built from source (1.0.18, static)")
else()
    # libzmq's Findsodium.cmake then resolves the system libsodium via
    # pkg-config (i.e. libsodium-dev must be installed).
endif()

# --- libzmq (C library; the cppzmq C++ headers are vendored in third_party) ---
option(XSTORE_FETCH_ZMQ "Fetch & build libzmq from source instead of a system install" ON)

if(TARGET zmq)
    message(STATUS "ZMQ: using pre-existing zmq target")
elseif(XSTORE_FETCH_ZMQ)
    set(BUILD_SHARED   ON  CACHE BOOL "" FORCE)
    # libzmq 4.3.5's CMake build requires both WITH_LIBSODIUM and ENABLE_CURVE
    # to be ON for ZMQ_HAVE_CURVE to be defined; its Findsodium.cmake then
    # resolves the in-tree libsodium via the SODIUM_* variables pre-set above
    # (or a system libsodium-dev when XSTORE_FETCH_SODIUM=OFF). Without both,
    # CURVE support is not compiled in.
    set(WITH_LIBSODIUM ON  CACHE BOOL "" FORCE)
    set(ENABLE_CURVE   ON  CACHE BOOL "" FORCE)
    set(WITH_TLS       OFF CACHE BOOL "" FORCE)
    set(BUILD_TESTS    OFF CACHE BOOL "" FORCE)
    set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
            zmq
            GIT_REPOSITORY https://github.com/zeromq/libzmq.git
            GIT_TAG v4.3.5
            GIT_SHALLOW ON
    )
    FetchContent_MakeAvailable(zmq)
    add_library(zmq ALIAS libzmq)
    message(STATUS "ZMQ: fetched & building libzmq from source (4.3.5)")
else()
    find_path(ZMQ_INCLUDE_DIR NAMES zmq.h)
    find_library(ZMQ_LIBRARY NAMES zmq)
    if(NOT ZMQ_LIBRARY OR NOT ZMQ_INCLUDE_DIR)
        message(FATAL_ERROR
            "libzmq not found (looked for libzmq + zmq.h). "
            "Install libzmq3-dev, or reconfigure with -DXSTORE_FETCH_ZMQ=ON.")
    endif()
    add_library(zmq SHARED IMPORTED)
    set_target_properties(zmq PROPERTIES
            IMPORTED_LOCATION "${ZMQ_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${ZMQ_INCLUDE_DIR}")
    message(STATUS "ZMQ: using system libzmq ${ZMQ_LIBRARY}")
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Vendored third-party headers (spdlog, cppzmq)
include_directories(third_party)

# Protobuf-compiler (imported target from the protobuf build above)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

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

# ------------------------------------------------------------------
# Extra protobuf language bindings (opt-in, for out-of-tree ZMQ clients)
# ------------------------------------------------------------------
# C++ is always generated above and compiled into XStore. The rest are
# written to <build>/generated/<lang>/ so client code in other languages can
# consume the same wire protocol. Built-in generators (python, java, ...)
# need no extra tooling; plugin-based ones (c, go, rust, ...) need the
# matching protoc-gen-<lang> on PATH -- if it isn't installed we skip that
# language with a note rather than failing. Node.js/JS has no standard protoc
# generator: ship proto/XStore.proto and parse it with protobuf.js in the
# Node project.
# Generate with: cmake --build build --target gen_proto
set(XSTORE_PROTO_LANGS "cpp" CACHE STRING
    "Comma-separated protobuf languages to generate: cpp,c,python,java,php,ruby,csharp,objc,go,rust (cpp is always compiled into XStore)")

string(REPLACE "," ";" _xstore_proto_langs "${XSTORE_PROTO_LANGS}")
# Built-in generators in the pinned protoc (v35.1); every other language name
# is treated as plugin-based and resolved via protoc-gen-<lang> on PATH.
set(_xstore_proto_builtin cpp java kotlin python php ruby csharp objc)
set(_xstore_proto_gen_targets "")
foreach(_xstore_lang IN LISTS _xstore_proto_langs)
    if(_xstore_lang STREQUAL "cpp")
        continue()   # already generated + compiled into XStore above
    endif()
    list(FIND _xstore_proto_builtin ${_xstore_lang} _xstore_builtin_idx)
    if(_xstore_builtin_idx EQUAL -1)
        string(TOUPPER "${_xstore_lang}" _xstore_lang_upper)
        find_program(_xstore_protoc_gen_${_xstore_lang_upper} NAMES protoc-gen-${_xstore_lang})
        if(NOT _xstore_protoc_gen_${_xstore_lang_upper})
            message(STATUS "gen_proto: protoc-gen-${_xstore_lang} not found on PATH -- skipping ${_xstore_lang} bindings")
            continue()
        endif()
    endif()
    set(_xstore_lang_out "${CMAKE_BINARY_DIR}/generated/${_xstore_lang}")
    file(MAKE_DIRECTORY "${_xstore_lang_out}")
    add_custom_target(gen_proto_${_xstore_lang}
            COMMAND ${_PROTOBUF_PROTOC} --experimental_allow_proto3_optional
                    --${_xstore_lang}_out ${_xstore_lang_out}
                    -I "${XStore_proto_path}" "${XStore_proto}"
            COMMENT "Generating ${_xstore_lang} protobuf bindings -> ${_xstore_lang_out}"
            VERBATIM)
    list(APPEND _xstore_proto_gen_targets gen_proto_${_xstore_lang})
endforeach()
if(_xstore_proto_gen_targets)
    add_custom_target(gen_proto DEPENDS ${_xstore_proto_gen_targets})
    message(STATUS "gen_proto: will generate bindings for: ${XSTORE_PROTO_LANGS}")
endif()

# Include generated *.pb.h files (protobuf's own headers come via the
# protobuf::libprotobuf target's INTERFACE include dirs, not a global include)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

include_directories(.)
include_directories(include)
include_directories(src)

add_executable(XStore src/main.cpp
        include/XStore/XStore_Internal.h src/XStore/XStore_Internal.cpp
        src/XStore/XStore_Management.cpp
        src/XStore/XStore_IO.cpp
        src/XStore/XStore_Wal.cpp
        src/XStore/XStore_Crc32c.cpp
        src/XStore/XStore_SysStats.cpp
        include/XStore/XStore_SysStats.h
        src/XStore/XStore_UnaryOps.cpp
        src/XStore/XStore_PreadPool.cpp
        include/XStore/XStore_PreadPool.h
        src/XStore/XStore_BatchOps.cpp
        src/XStore/XStore_RangeOps.cpp
        src/XStore/XStore_Aggregates.cpp
        include/XStore/XStore.h
        include/DataStructs/ValueStruct.h ${PROTO_SRCS} ${PROTO_HDRS}
        third_party/zmq.hpp third_party/zmq_addon.hpp
        include/Networking/ZMQ_Server.h src/XStore_Networking/ZMQ_Server.cpp
        include/Networking/XStore_Curve.h src/XStore_Networking/XStore_Curve.cpp
        third_party/zhelpers.hpp
        third_party/argparse/argparse.hpp
        include/DataStructs/TimeStruct.h src/DataStructs/TimeStruct.cpp
        include/DataStructs/FileRangeLock.h src/DataStructs/FileRangeLock.cpp
        include/Logger/Logger.h)

target_link_libraries(XStore PUBLIC "stdc++fs" protobuf::libprotobuf "zmq" Threads::Threads moodycamel)
if(TARGET absl::log_internal_check_op)
    target_link_libraries(XStore PUBLIC absl::log_internal_check_op)
endif()

add_subdirectory(tools/xstore-keys)

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