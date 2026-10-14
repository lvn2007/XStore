#!/usr/bin/env bash
#
# XStore -- build + deploy script (see README.md "Quick Build").
#
# Every dependency (protobuf v35.1, libzmq 4.3.5, libsodium 1.0.18,
# jemalloc, RE2) is fetched & built from source by CMake -- the build is
# self-contained and does not use preinstalled system libraries. A fresh
# machine only needs a C/C++ toolchain, cmake >= 3.20, git and pkg-config;
# step [1/4] checks for those and installs the missing ones via apt
# (build-essential cmake git pkg-config, plus optional ccache) on
# Debian/Ubuntu, using sudo when not already root.
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

# ---------------------------------------------------------------------------
# [1/4] Prerequisites.
# Required: g++/make (build-essential), cmake >= 3.20, git, pkg-config.
# ccache is optional but included in the apt install: CMake picks it up
# automatically (see CMakeLists.txt) and speeds up rebuilds. No -dev
# packages are needed -- protobuf/libzmq/libsodium/jemalloc/RE2 are all
# fetched & built by CMake.
# ---------------------------------------------------------------------------
PKGS_DEBIAN=(build-essential cmake git pkg-config ccache)
NEEDED_CMDS=(g++ make cmake git pkg-config)
CMAKE_MIN="3.20"

missing=()
for cmd in "${NEEDED_CMDS[@]}"; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
done

if ((${#missing[@]} > 0)); then
    echo "==> [1/4] Prerequisites missing: ${missing[*]}"
    if command -v apt-get >/dev/null 2>&1; then
        sudo_cmd=()
        if [[ $EUID -ne 0 ]]; then
            if ! command -v sudo >/dev/null 2>&1; then
                echo "ERROR: need sudo (or run as root) to install: ${PKGS_DEBIAN[*]}" >&2
                exit 1
            fi
            sudo_cmd=(sudo)
        fi
        "${sudo_cmd[@]}" apt-get update
        "${sudo_cmd[@]}" apt-get install -y "${PKGS_DEBIAN[@]}"
    else
        echo "ERROR: apt-get not found. Install these manually (Debian/Ubuntu names):" >&2
        echo "       ${PKGS_DEBIAN[*]}" >&2
        exit 1
    fi
else
    echo "==> [1/4] Prerequisites present"
fi

cmake_ver="$(cmake --version | head -n1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -n1)"
if [[ "$(printf '%s\n' "$CMAKE_MIN" "$cmake_ver" | sort -V | head -n1)" != "$CMAKE_MIN" ]]; then
    echo "ERROR: cmake $cmake_ver < $CMAKE_MIN. Install a newer cmake (e.g. pip install cmake, or the KitWare APT repo)." >&2
    exit 1
fi

echo "==> [2/4] Configuring with CMake (Release, tests off)"
cmake -B build -DCMAKE_BUILD_TYPE=Release -DXSTORE_BUILD_TESTS=OFF

# Single-line progress bar for the build: parses the "[ NN%]" markers the
# Makefile generator emits and the "[ N/M]" counters the Ninja generator
# emits, shows the most recent output line as the current action, and tees
# every line to the log. Non-TTY output (CI) passes through.
PROGRESS_AWK='
BEGIN {
    tw = ENVIRON["COLUMNS"] + 0
    if (tw < 40 || tw > 500) tw = 80
    width = (tw >= 100) ? 40 : 24
    pct = 0
    action = "starting build..."
}
{
    print >> LOG
    line = $0
    if (line ~ /^\[ *[0-9]+%\]/) {
        p = line
        sub(/^\[ */, "", p)
        sub(/%\].*$/, "", p)
        pct = p + 0
        action = line
        sub(/^\[ *[0-9]+%\] */, "", action)
    } else if (line ~ /^\[ *[0-9]+\/ *[0-9]+ *\]/) {
        p = line
        sub(/^\[ */, "", p)
        n = p + 0
        sub(/^ *[0-9]+\/ */, "", p)
        d = p + 0
        if (d > 0) pct = int(n * 100 / d)
        action = line
        sub(/^\[ *[0-9]+\/ *[0-9]+ *\] */, "", action)
    } else if (line ~ /[^ \t]/) {
        action = line
    }
    maxa = tw - width - 12
    if (maxa < 12) maxa = 12
    if (length(action) > maxa) action = substr(action, 1, maxa - 2) ".."
    filled = int(width * pct / 100)
    bar = ""
    for (i = 0; i < filled; i++) bar = bar "#"
    for (i = filled; i < width; i++) bar = bar "."
    out = sprintf("[%s] %3d%% %s", bar, pct, action)
    pad = ""
    for (i = length(out); i < tw; i++) pad = pad " "
    printf "\r%s%s", out, pad
}
END { printf "\n" }
'

mkdir -p build/logs
BUILD_LOG="build/logs/build.log"
echo "==> [3/4] Building XStore (jobs: $(nproc)) -- full log: $BUILD_LOG"
if [ -t 1 ]; then
    if ! cmake --build build -j "$(nproc)" --target XStore 2>&1 | awk -v LOG="$BUILD_LOG" "$PROGRESS_AWK"; then
        echo "Build FAILED. Last 30 lines of $BUILD_LOG:"
        tail -n 30 "$BUILD_LOG"
        exit 1
    fi
else
    if ! cmake --build build -j "$(nproc)" --target XStore 2>&1 | tee "$BUILD_LOG"; then
        echo "Build FAILED. See $BUILD_LOG"
        exit 1
    fi
fi

echo "==> [4/4] Staging runnable server dir (../XStore_Server)"
# DataStore/WAL included explicitly: XStore also creates them on demand, but
# the benchmark harness expects them to already exist here.
mkdir -p ../XStore_Server/DataStore/WAL
cp -r --remove-destination build ../XStore_Server

echo "Build complete: $(pwd)/build/XStore (server staged at ../XStore_Server)"
