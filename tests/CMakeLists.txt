# xstore_tests exercises the XStore class directly against the filesystem
# (InfoQuery, CreateDB, RemoveDB/RemoveDBFiles, Unary/Batch/Range
# Insert+Query+Delete, the Min/Max/Sum/Avg aggregates, and concurrency),
# bypassing the ZMQ networking layer entirely -- that layer is a thin proto
# (de)serialization wrapper around these same methods, tested separately in
# xstore_network_tests below via the real wire protocol.
add_executable(xstore_tests
        integration/XStoreTest.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Internal.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Management.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_IO.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_UnaryOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_BatchOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_RangeOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Aggregates.cpp
        ${CMAKE_SOURCE_DIR}/src/DataStructs/TimeStruct.cpp
        ${CMAKE_SOURCE_DIR}/src/DataStructs/FileRangeLock.cpp
        ${PROTO_SRCS} ${PROTO_HDRS}
)

target_link_libraries(xstore_tests PRIVATE GTest::gtest_main protobuf::libprotobuf Threads::Threads "stdc++fs")
if(TARGET absl::log_internal_check_op)
    target_link_libraries(xstore_tests PRIVATE absl::log_internal_check_op)
endif()

include(GoogleTest)
gtest_discover_tests(xstore_tests)

# Run the suite automatically right after a successful build of this target,
# so `cmake --build .` compiles and tests in one step. Scoped to this
# binary's own suite (XStoreTest.*) so building just this target doesn't
# report the other binary's tests as "_NOT_BUILT" if it hasn't been built yet.
add_custom_command(TARGET xstore_tests POST_BUILD
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreTest\\."
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running xstore_tests via ctest"
)

# xstore_network_tests drives the same operations through the real ZMQ wire
# protocol (ZMQ_Server + a REQ client), unlike xstore_tests above which calls
# into the XStore class directly.
add_executable(xstore_network_tests
        integration/XStoreZmqNetworkTest.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Internal.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Management.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_IO.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_UnaryOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_BatchOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_RangeOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Aggregates.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore_Networking/ZMQ_Server.cpp
        ${CMAKE_SOURCE_DIR}/src/DataStructs/TimeStruct.cpp
        ${CMAKE_SOURCE_DIR}/src/DataStructs/FileRangeLock.cpp
        ${PROTO_SRCS} ${PROTO_HDRS}
)

target_link_libraries(xstore_network_tests PRIVATE GTest::gtest_main protobuf::libprotobuf Threads::Threads "stdc++fs" "zmq")
if(TARGET absl::log_internal_check_op)
    target_link_libraries(xstore_network_tests PRIVATE absl::log_internal_check_op)
endif()

gtest_discover_tests(xstore_network_tests)

add_custom_command(TARGET xstore_network_tests POST_BUILD
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreZmqNetworkTest\\.\\|^XStoreZmqConcurrentTest\\."
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running xstore_network_tests via ctest"
)

# xstore_client_tests exercises XStore_Client.h itself: XStoreClientTest.cpp
# covers the warmupOnInit flag in isolation, XStoreClientLiveTest.cpp drives
# real Unary/Batch/Range Insert+Query+Delete/aggregate round trips against a
# live XStore server over the actual network (opt-in via XSTORE_LIVE_ADDR /
# XSTORE_LIVE_PORT env vars -- GTEST_SKIPs otherwise, see that file's header).
# Both link the XStoreClient INTERFACE target from client_src/Cpp, which
# generates its own copy of the protobuf bindings -- kept in a separate
# binary from xstore_tests/xstore_network_tests above so their
# independently-generated XStore.pb.cc symbols never collide in one link.
if(TARGET XStoreClient)
    add_executable(xstore_client_tests
            integration/XStoreClientTest.cpp
            integration/XStoreClientLiveTest.cpp
    )
    target_link_libraries(xstore_client_tests PRIVATE GTest::gtest_main XStoreClient)

    include(GoogleTest)
    gtest_discover_tests(xstore_client_tests)

    add_custom_command(TARGET xstore_client_tests POST_BUILD
            COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreClientWarmupTest\\."
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
            COMMENT "Running xstore_client_tests via ctest"
    )
endif()

# xstore_perf_tests: throughput/latency micro-benchmarks for Unary/Batch
# Insert+Query (see tests/perf/XStorePerfTest.cpp for why these report
# numbers instead of asserting hard thresholds). Deliberately NOT wired into
# a POST_BUILD ctest run like xstore_tests/xstore_network_tests above --
# unlike those, this suite isn't meant to gate every build, only to be run
# on demand (`ctest -R XStorePerfTest` or the binary directly) when doing
# perf work.
add_executable(xstore_perf_tests
        perf/XStorePerfTest.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Internal.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Management.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_IO.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_UnaryOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_BatchOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_RangeOps.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_Aggregates.cpp
        ${CMAKE_SOURCE_DIR}/src/DataStructs/TimeStruct.cpp
        ${CMAKE_SOURCE_DIR}/src/DataStructs/FileRangeLock.cpp
        ${PROTO_SRCS} ${PROTO_HDRS}
)

target_link_libraries(xstore_perf_tests PRIVATE GTest::gtest_main protobuf::libprotobuf Threads::Threads "stdc++fs")
if(TARGET absl::log_internal_check_op)
    target_link_libraries(xstore_perf_tests PRIVATE absl::log_internal_check_op)
endif()

gtest_discover_tests(xstore_perf_tests)
