# xstore_tests exercises the XStore class directly against the filesystem
# (InfoQuery, CreateDB, RemoveDB/RemoveDBFiles, Unary/Batch/Range
# Insert+Query+Delete, the Min/Max/Sum/Avg aggregates, and concurrency),
# bypassing the ZMQ networking layer entirely -- that layer is a thin proto
# (de)serialization wrapper around these same methods, tested separately in
# xstore_network_tests below via the real wire protocol.
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
        integration/XStorePatternTest.cpp
        integration/XStoreMemoryTest.cpp
        integration/XStoreDurabilityTest.cpp
        integration/XStoreWalRingTest.cpp
)
target_link_libraries(xstore_tests PRIVATE GTest::gtest_main xstore_core)

include(GoogleTest)
gtest_discover_tests(xstore_tests)

# Run the suite automatically right after a successful build of this target,
# so `cmake --build .` compiles and tests in one step. Scoped to THIS binary's
# suites so building just this target doesn't report the other binaries' tests
# as "_NOT_BUILT" if they haven't been built yet -- which means every suite
# name declared in this target's sources must be listed here. It used to be
# just "^XStoreTest\\.", silently skipping every zone-map and catidx suite
# (worked around by a manual `ctest -R CatIdx` in each task instead).
add_custom_command(TARGET xstore_tests POST_BUILD
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^(XStoreTest|XStoreWalTest|XStorePreadPoolTest|XStorePreadPoolDeathTest|XStoreCrc32cTest|XStoreZoneMapTest|ZoneMapEntryLayoutTest|ZoneMapFileTest|ZoneMapFileVersionTest|ZoneMapMathTest|XStoreCatIdxTestFixture|CatIdxFileTest|GetOrOpenCatIdxTest|EncodeFDataForCatIdxTest|PatternMatcherTest|XStorePatternTestFixture|XStoreMemoryTest|DurabilityHeaderLayoutTest|DurabilityMarkDirtyTest|DurabilityRebuildTest|DurabilityShutdownTest|WalRingFormatTest|WalRingWriterTest|WalCheckpointTest|WalReplayTest|WalLegacyUpgradeTest|WalDirectPathTest|WalCrashSimTest)\\."
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running xstore_tests via ctest"
        VERBATIM # the regex contains ( | ) -- without this CMake emits it unquoted and /bin/sh chokes
)

# xstore_network_tests drives the same operations through the real ZMQ wire
# protocol (ZMQ_Server + a REQ client), unlike xstore_tests above which calls
# into the XStore class directly.
add_executable(xstore_network_tests
        integration/XStoreZmqNetworkTest.cpp
        integration/XStoreCurveTest.cpp
        integration/XStoreGracefulShutdownTest.cpp
)
# xstore_network brings the core (xstore_core) and curve-key handling
# (xstore_curve) with it.
target_link_libraries(xstore_network_tests PRIVATE GTest::gtest_main xstore_network)
# XStoreGracefulShutdownTest launches the real server binary (SIGTERM/SIGKILL
# black-box tests), so it must be built first.
target_compile_definitions(xstore_network_tests PRIVATE XSTORE_BIN="$<TARGET_FILE:XStore>")
add_dependencies(xstore_network_tests XStore)

gtest_discover_tests(xstore_network_tests)

add_custom_command(TARGET xstore_network_tests POST_BUILD
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreZmqNetworkTest\\.\\|^XStoreZmqConcurrentTest\\.\\|^XStoreCurve\\|^XStoreGracefulShutdownTest\\."
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
            integration/XStoreClientCurveLiveTest.cpp
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

# xstore_keys_cli_tests: black-box smoke tests for the xstore-keys binary
# (tools/xstore-keys) -- invokes the real built binary via std::system(),
# not a unit test of its internals (those are covered by
# XStoreCurveKeyPairTest in xstore_network_tests).
add_executable(xstore_keys_cli_tests integration/XStoreKeysCliTest.cpp)
target_include_directories(xstore_keys_cli_tests PRIVATE ${CMAKE_SOURCE_DIR})
# moodycamel: XStoreTestSupport.h (shared with the other suites) pulls in
# XStore.h, which includes <blockingconcurrentqueue.h>.
target_link_libraries(xstore_keys_cli_tests PRIVATE GTest::gtest_main xstore_curve moodycamel)
target_compile_definitions(xstore_keys_cli_tests PRIVATE XSTORE_KEYS_BIN="$<TARGET_FILE:xstore-keys>")
add_dependencies(xstore_keys_cli_tests xstore-keys)

include(GoogleTest)
gtest_discover_tests(xstore_keys_cli_tests)

add_custom_command(TARGET xstore_keys_cli_tests POST_BUILD
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreKeysCliTest\\."
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running xstore_keys_cli_tests via ctest"
)

# xstore_perf_tests: throughput/latency micro-benchmarks for Unary/Batch
# Insert+Query (see tests/perf/XStorePerfTest.cpp for why these report
# numbers instead of asserting hard thresholds). Deliberately NOT wired into
# a POST_BUILD ctest run like xstore_tests/xstore_network_tests above --
# unlike those, this suite isn't meant to gate every build, only to be run
# on demand (`ctest -R XStorePerfTest` or the binary directly) when doing
# perf work.
add_executable(xstore_perf_tests
        perf/XStorePerfTest.cpp
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
