#!/usr/bin/env bash
#
# XStore -- build + deploy script (see README.md "Quick Build").
#
# Gates only on the XStore server target (XSTORE_BUILD_TESTS=OFF skips
# compiling the test suite here entirely): a previous version built the
# whole `all` target via plain `make -j`, which runs ctest as a side effect
# of building xstore_network_tests -- a pre-existing, independently-
# reproducible flaky test there could fail and abort the script BEFORE the
# deploy step ran, silently leaving a stale binary in ../XStore_Server with
# no error surfaced anywhere. Deploy should gate on "did the server binary
# build," never on an unrelated test suite.
#
# Output: build/XStore  (staged as a runnable server dir at ../XStore_Server)

set -euo pipefail

cd "$(dirname "$0")"

echo "==> [1/3] Configuring with CMake (Release, tests off)"
cmake -B build -DCMAKE_BUILD_TYPE=Release -DXSTORE_BUILD_TESTS=OFF

echo "==> [2/3] Building XStore (jobs: $(nproc))"
cmake --build build -j "$(nproc)" --target XStore

echo "==> [3/3] Staging runnable server dir (../XStore_Server)"
# DataStore/WAL included explicitly: XStore also creates them on demand, but
# the benchmark harness expects them to already exist here.
mkdir -p ../XStore_Server/DataStore/WAL
cp -r build ../XStore_Server

echo "Build complete: $(pwd)/build/XStore (server staged at ../XStore_Server)"
