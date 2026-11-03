# xstore_tests exercises the XStore class directly against the filesystem
# (InfoQuery, CreateDB, RemoveDB/RemoveDBFiles, Unary/Batch/Range
# Insert+Query+Delete, the Min/Max/Sum/Avg aggregates, and concurrency),
# bypassing the ZMQ networking layer entirely. Request/response logic is
# covered here; xstore_network_tests below covers only the transport.
# Core sources come from the xstore_core static lib (top-level CMakeLists)
# -- never list them (or PROTO_SRCS/PROTO_HDRS) here: that reintroduces
# duplicate per-target compiles and the cross-directory protoc codegen race
# described there.
add_executable(xstore_tests
        integration/XStoreTest.cpp
        integration/XStoreWalTest.cpp
        integration/XStoreZoneMapTest.cpp
        integration/XStoreCrc32cTest.cpp
        integration/XStoreCatIdxTest.cpp
        integration/XStoreBitmapBuildLeaseTest.cpp
        integration/XStoreScanLockTest.cpp
        integration/XStoreRangeQueryResumeTest.cpp
        integration/XStoreFindByResumeTest.cpp
        integration/XStoreBatchQueryResumeTest.cpp
        integration/XStoreExactAggregatesTest.cpp
        integration/XStorePatternTest.cpp
        integration/XStoreMemoryTest.cpp
        integration/XStoreMemoryPoolTest.cpp
        integration/XStoreChunkPoolTest.cpp
        integration/XStoreQueryLeaseTest.cpp
        integration/XStoreDurabilityTest.cpp
        integration/XStoreWalRingTest.cpp
        integration/XStoreWalCheckpointSyncTest.cpp
        integration/XStoreInsertUnifyTest.cpp
        integration/XStoreRowScanTest.cpp
        integration/XStoreDeleteFaultTest.cpp
        integration/XStoreRebuildTest.cpp
        integration/XStoreParallelRebuildTest.cpp
        integration/XStoreRebuildProgressTest.cpp
        integration/XStoreLiveWidenTest.cpp
        integration/XStoreLiveWidenGovernorTest.cpp
        integration/XStoreLiveWidenChunkTest.cpp
        integration/XStoreFileRangeLockTest.cpp
        integration/XStoreSyscallFault.cpp
)
target_link_libraries(xstore_tests PRIVATE GTest::gtest_main xstore_core ${CMAKE_DL_LIBS})

include(GoogleTest)
# Single test trigger per target (S7): gtest_discover_tests registers every
# case with ctest, and the suite runs when YOU run ctest -- the build itself
# no longer auto-runs it via a POST_BUILD ctest (that was a second execution
# path, and the network one's unquoted `|` was a shell pipe in the generated
# rule, so it never ran its full regex anyway). Run suites with
# `ctest --test-dir build [-R <regex>]`.
gtest_discover_tests(xstore_tests)

# xstore_network_tests: transport only; request/response logic is covered in
# xstore_tests. Drives the real ZMQ wire protocol (ZMQ_Server + a REQ client):
# per-RPC-family dispatch/encoding round trips, error replies, CURVE/ZAP,
# graceful shutdown/signals, and multi-client concurrency.
add_executable(xstore_network_tests
        integration/XStoreZmqNetworkTest.cpp
        integration/XStoreCurveTest.cpp
        integration/XStoreGracefulShutdownTest.cpp
        integration/XStoreZmqBrokerTest.cpp
        integration/XStoreStreamFlowTest.cpp
        integration/XStoreParkedRepliesTest.cpp
)
# xstore_network brings the core (xstore_core) and curve-key handling
# (xstore_curve) with it.
target_link_libraries(xstore_network_tests PRIVATE GTest::gtest_main xstore_network)
# XStoreGracefulShutdownTest launches the real server binary (SIGTERM/SIGKILL
# black-box tests), so it must be built first.
target_compile_definitions(xstore_network_tests PRIVATE XSTORE_BIN="$<TARGET_FILE:XStore>")
add_dependencies(xstore_network_tests XStore)

gtest_discover_tests(xstore_network_tests)

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
            integration/XStoreClientCurveLiveTest.cpp
            integration/XStoreClientParallelTest.cpp
            integration/XStoreClientParallelParseTest.cpp
            integration/XStoreClientStreamTest.cpp
            integration/XStoreClientSplitTest.cpp
            integration/XStoreClientWindowTest.cpp
            integration/XStoreClientEncodeTest.cpp
    )
    target_link_libraries(xstore_client_tests PRIVATE GTest::gtest_main XStoreClient)

    include(GoogleTest)
    gtest_discover_tests(xstore_client_tests)
endif()

# xstore_keys_cli_tests: black-box smoke tests for the xstore-keys binary
# (tools/xstore-keys) -- invokes the real built binary via std::system(),
# not a unit test of its internals (those are covered by
# XStoreCurveKeyPairTest in xstore_network_tests).
add_executable(xstore_keys_cli_tests integration/XStoreKeysCliTest.cpp)
target_include_directories(xstore_keys_cli_tests PRIVATE ${CMAKE_SOURCE_DIR})
# moodycamel: XStoreTestSupport.h (shared with the other suites) pulls in
# XStore.h, which includes <blockingconcurrentqueue.h> and the generated protobuf headers (xstore_core supplies their include path).
target_link_libraries(xstore_keys_cli_tests PRIVATE GTest::gtest_main xstore_core xstore_curve moodycamel)
target_compile_definitions(xstore_keys_cli_tests PRIVATE XSTORE_KEYS_BIN="$<TARGET_FILE:xstore-keys>")
add_dependencies(xstore_keys_cli_tests xstore-keys)

include(GoogleTest)
gtest_discover_tests(xstore_keys_cli_tests)

# xstore_perf_tests: throughput/latency micro-benchmarks for Unary/Batch
# Insert+Query (see tests/perf/XStorePerfTest.cpp for why these report
# numbers instead of asserting hard thresholds). Deliberately NOT wired into
# a POST_BUILD ctest run like xstore_tests/xstore_network_tests above --
# unlike those, this suite isn't meant to gate every build, only to be run
# on demand (`ctest -R XStorePerfTest` or the binary directly) when doing
# perf work.
add_executable(xstore_perf_tests
        perf/XStorePerfTest.cpp
        perf/XStoreDirtyTrackerPerfTest.cpp
)
target_link_libraries(xstore_perf_tests PRIVATE GTest::gtest_main xstore_core)
if(XSTORE_USE_JEMALLOC)
    target_sources(xstore_perf_tests PRIVATE ${CMAKE_SOURCE_DIR}/src/XStore_Alloc/jemalloc_conf.cpp)
    target_compile_definitions(xstore_perf_tests PRIVATE XSTORE_JEMALLOC_THP=$<BOOL:${XSTORE_JEMALLOC_THP}>)
    target_link_libraries(xstore_perf_tests PRIVATE jemalloc::jemalloc)
    # See CMakeLists.txt's XStore target for why --whole-archive is required.
    target_link_options(xstore_perf_tests PRIVATE "-Wl,--whole-archive,$<TARGET_FILE:jemalloc::jemalloc>,--no-whole-archive")
endif()

gtest_discover_tests(xstore_perf_tests)

# xstore_zmq_perf_tests: BatchInsert over the real ZMQ server on loopback (the arena-allocated request path the
# in-process perf cases skip). Opt-in at run time as well (XSTORE_ZMQ_PERF=1, see the file header), so it is not
# registered with ctest.
add_executable(xstore_zmq_perf_tests
        perf/XStoreZmqInsertPerfTest.cpp
)
target_link_libraries(xstore_zmq_perf_tests PRIVATE GTest::gtest_main xstore_network)
if(XSTORE_USE_JEMALLOC)
    target_sources(xstore_zmq_perf_tests PRIVATE ${CMAKE_SOURCE_DIR}/src/XStore_Alloc/jemalloc_conf.cpp)
    target_compile_definitions(xstore_zmq_perf_tests PRIVATE XSTORE_JEMALLOC_THP=$<BOOL:${XSTORE_JEMALLOC_THP}>)
    target_link_libraries(xstore_zmq_perf_tests PRIVATE jemalloc::jemalloc)
    target_link_options(xstore_zmq_perf_tests PRIVATE "-Wl,--whole-archive,$<TARGET_FILE:jemalloc::jemalloc>,--no-whole-archive")
endif()

# xstore_zmq_insert_client: the same BatchInsert load as xstore_zmq_perf_tests, but a standalone program for a real
# XStore server on another process or machine (see the file header). Not part of any test run.
add_executable(xstore_zmq_insert_client
        perf/XStoreZmqInsertClient.cpp
)
target_include_directories(xstore_zmq_insert_client PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore_zmq_insert_client PRIVATE xstore_network)

# xstore_zmq_fault_client: SYNC BatchInserts with an acknowledgement log + a post-restart verifier, for the
# I/O-error injection run (dm-flakey under the server's data directory; see the file header). Not part of any test run.
add_executable(xstore_zmq_fault_client
        perf/XStoreZmqFaultClient.cpp
)
target_include_directories(xstore_zmq_fault_client PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore_zmq_fault_client PRIVATE xstore_network)

# xstore_zmq_mixed_client: SYNC scattered BatchInserts next to random point reads, from another machine (see the file header).
add_executable(xstore_zmq_mixed_client
        perf/XStoreZmqMixedClient.cpp
)
target_link_libraries(xstore_zmq_mixed_client PRIVATE xstore_network)

# xstore_zmq_widen_client: queries and a grow insert for the live-widen study, from another machine (see the file header).
add_executable(xstore_zmq_widen_client
        perf/XStoreZmqWidenClient.cpp
)
target_link_libraries(xstore_zmq_widen_client PRIVATE xstore_network)

# xstore_zmq_find_client: timed FindByValue probe for the bitmap-section study, from another machine (see the file header).
add_executable(xstore_zmq_find_client
        perf/XStoreZmqFindClient.cpp
)
target_link_libraries(xstore_zmq_find_client PRIVATE xstore_network)

# tests/local_bench: opt-in, single-machine benchmark harness (pure Python
# driver around the real XStore server + the XBench binary -- see
# tests/local_bench/README.md). Same "not part of the default build" stance
# as xstore_perf_tests above, one level further: these are plain custom
# targets, never added to ALL, so `cmake --build .` / `make -j` alone never
# touches them -- only `cmake --build . --target bench_local` (etc.) does.
find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
    set(XSTORE_LOCAL_BENCH_DIR ${CMAKE_SOURCE_DIR}/tests/local_bench)

    set(XSTORE_LOCAL_BENCH_CLIENT_BIN "/home/ubuntu/XBench/build/XBench" CACHE FILEPATH
        "Path to the XBench binary, used by bench_local_run/bench_local_seed")
    set(XSTORE_LOCAL_BENCH_TAG "default" CACHE STRING
        "Label for this bench_local_run invocation (e.g. 'baseline', 'my-change') -- rerun with a different value and bench_local_summarize compares tags")
    # Empty = let the Python scripts derive a sane default from this
    # machine's core count (see bench_common.DEFAULT_TIERS / seed_fixture's
    # --pool-size default) -- XBench itself refuses -t above its own
    # hardware_concurrency(), so there's no point requesting more.
    set(XSTORE_LOCAL_BENCH_TIERS "" CACHE STRING "Comma list of client tiers to benchmark (empty = auto, e.g. 1,8,16 on a 16-core box)")
    set(XSTORE_LOCAL_BENCH_WORKLOADS "" CACHE STRING "Comma list of workload names, or 'all' for all 12 (empty = auto, the script's own default: 4 INSERT + 4 QUERY + AGG_MIN)")
    set(XSTORE_LOCAL_BENCH_POOL_SIZE "" CACHE STRING "Max client tier bench_local_seed prepares a QUERY/AGG fixture for (empty = auto, this machine's core count)")

    set(_local_bench_tiers_arg "")
    if(XSTORE_LOCAL_BENCH_TIERS)
        set(_local_bench_tiers_arg --tiers ${XSTORE_LOCAL_BENCH_TIERS})
    endif()
    set(_local_bench_workloads_arg "")
    if(XSTORE_LOCAL_BENCH_WORKLOADS)
        set(_local_bench_workloads_arg --workloads ${XSTORE_LOCAL_BENCH_WORKLOADS})
    endif()
    set(_local_bench_pool_size_arg "")
    if(XSTORE_LOCAL_BENCH_POOL_SIZE)
        set(_local_bench_pool_size_arg --pool-size ${XSTORE_LOCAL_BENCH_POOL_SIZE})
    endif()

    # Standalone pre-warm target: run_local_bench.py/bench_local already seed
    # the fixture themselves on demand (see run_local_bench.py's docstring),
    # so this is only useful if you want to pay that one-time cost ahead of
    # time, e.g. before looping many tagged runs back to back.
    # USES_TERMINAL (CMake >=3.2): routes the job through the build tool's
    # console pool instead of the normal buffer-until-the-job-finishes-then-
    # dump-it-all-at-once behavior Ninja otherwise applies to every custom
    # command -- without it, run_local_bench.py's tqdm progress bar (and any
    # other live output) never appears until the whole target is done, and
    # only then as a garbled one-shot dump of \r-updates. Also serializes
    # these targets against any other console-pool job, which is fine, they
    # were never meant to run in parallel with anything else anyway.
    add_custom_target(bench_local_seed
        COMMAND ${Python3_EXECUTABLE} seed_fixture.py --start-server
                --server-build-dir ${CMAKE_BINARY_DIR}
                ${_local_bench_pool_size_arg}
        WORKING_DIRECTORY ${XSTORE_LOCAL_BENCH_DIR}
        COMMENT "[local_bench] pre-warming QUERY/AGG fixture (optional -- bench_local seeds itself on demand too)"
        USES_TERMINAL
        VERBATIM
    )
    add_dependencies(bench_local_seed XStore)

    add_custom_target(bench_local_run
        COMMAND ${Python3_EXECUTABLE} run_local_bench.py
                --tag ${XSTORE_LOCAL_BENCH_TAG}
                --server-build-dir ${CMAKE_BINARY_DIR}
                --client-bin ${XSTORE_LOCAL_BENCH_CLIENT_BIN}
                ${_local_bench_tiers_arg}
                ${_local_bench_workloads_arg}
        WORKING_DIRECTORY ${XSTORE_LOCAL_BENCH_DIR}
        COMMENT "[local_bench] running benchmark harness (tag=${XSTORE_LOCAL_BENCH_TAG})"
        USES_TERMINAL
        VERBATIM
    )
    add_dependencies(bench_local_run XStore)

    add_custom_target(bench_local_summarize
        COMMAND ${Python3_EXECUTABLE} summarize_local_bench.py --tags ${XSTORE_LOCAL_BENCH_TAG}
        WORKING_DIRECTORY ${XSTORE_LOCAL_BENCH_DIR}
        COMMENT "[local_bench] summarizing results (tag=${XSTORE_LOCAL_BENCH_TAG})"
        USES_TERMINAL
        VERBATIM
    )

    # Convenience target: seed (if needed) + run + summarize in one command.
    # Multiple COMMAND entries on one custom target execute in the listed
    # order, so this is guaranteed to summarize only after the run finishes.
    add_custom_target(bench_local
        COMMAND ${Python3_EXECUTABLE} run_local_bench.py
                --tag ${XSTORE_LOCAL_BENCH_TAG}
                --server-build-dir ${CMAKE_BINARY_DIR}
                --client-bin ${XSTORE_LOCAL_BENCH_CLIENT_BIN}
                ${_local_bench_tiers_arg}
                ${_local_bench_workloads_arg}
        COMMAND ${Python3_EXECUTABLE} summarize_local_bench.py --tags ${XSTORE_LOCAL_BENCH_TAG}
        WORKING_DIRECTORY ${XSTORE_LOCAL_BENCH_DIR}
        COMMENT "[local_bench] running + summarizing (tag=${XSTORE_LOCAL_BENCH_TAG})"
        USES_TERMINAL
        VERBATIM
    )
    add_dependencies(bench_local XStore)
else()
    message(STATUS "Python3 interpreter not found -- skipping tests/local_bench targets (bench_local, bench_local_seed, bench_local_run, bench_local_summarize)")
endif()
