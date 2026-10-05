# XStore - High-performance time-series storage systems  

# Description
Source code for project XStore and its related work.

**Illinois Institute of Technology**  
**Data-Intensive Distributed Systems Laboratory**

Author: Lan Nguyen (Lnguyen18@hawk.illinoistech.edu)\
Advisor: Ioan Raicu (iraicu@illinoistech.edu)

This work has been published to:

<details open><summary>Accelerating CRUD with Chrono Dilation for Time-Series Storage Systems:</summary>

```
Lan Nguyen, Ioan Raicu.
“Accelerating CRUD with Chrono Dilation for Time-Series Storage Systems”,
IEEE/ACM Supercomputing/SC 2023
```

* Poster: [Link to Poster](https://sc23.supercomputing.org/proceedings/src_poster/poster_files/spostg124s3-file1.pdf)
* 2-page Summary: [Link to Extended Abstract](https://sc23.supercomputing.org/proceedings/src_poster/poster_files/spostg124s3-file2.pdf) 
* Presentation: [Link to Presentation](https://youtu.be/kYd0wFB3Zec)

</details>

# System Requirements
* Operating systems: Linux Ubuntu 24.04 LTS
* Build tools: build-essential, cmake, g++ 11.4.0 (C++ 17 standard), git, pkg-config, ccache (optional, auto-detected by CMake)
* Google Protocol Buffer — built from source, pinned to [commit `35cd01f`](https://github.com/protocolbuffers/protobuf/commit/35cd01f9fe9afbeea38cc7b979a3b6bfcde82c03) (v35.1)
* ZeroMQ — built from source (libzmq 4.3.5, pinned, with libsodium 1.0.18 for CURVE support) plus bundled cppzmq header ([client_src/Cpp/zmq.hpp](client_src/Cpp/zmq.hpp)); `libzmq3-dev` only needed when building with `-DXSTORE_FETCH_ZMQ=OFF`
* sysstat, tmux — used by the server deployment scripts (see below)

# How to Install

### Prerequisites
```bash
sudo apt update
sudo apt install -y build-essential cmake g++ git pkg-config ccache
```

### Quick Build
```bash
./build.sh
```
Builds XStore via CMake — all dependencies (protobuf, libzmq, libsodium) are fetched & built from source, so only the build tools above are needed — and stages a runnable server directory at `../XStore_Server`. If `ccache` is installed it is used automatically as a compile cache (content-addressed, so any code or flag change is a guaranteed cache miss — cached builds always reflect the latest code); `CCACHE_DISABLE=1` forces a cold build.

### Running the Server

From the staged directory (`../XStore_Server/build`):

| Mode | Command |
|---|---|
| Unsecured (default) | `./XStore --host 0.0.0.0 --port 9497` |
| Secured (CURVE) | `./XStore --host 0.0.0.0 --port 9497 --curve` |

Secured mode encrypts the ZMQ transport and authenticates clients against an allowlist. One-time key setup (`xstore-keys` binary builds at `build/tools/xstore-keys/`):

```bash
./tools/xstore-keys/xstore-keys gen server                                   # server keypair -> keys/server/
./tools/xstore-keys/xstore-keys gen client1                                  # client keypair -> keys/clients/client1/
./tools/xstore-keys/xstore-keys allow client1 --pubkey-file keys/clients/client1/client1.pub  # -> keys/allowed/
```

`--curve` defaults to `keys/server/server.key` + `keys/allowed/` (override with `--curve-secret-key` / `--curve-allowed-dir`); the allowlist is hot-reloaded, so `allow`/`revoke` take effect without a restart. Details: [Technical Reference](docs/XStore_Technical_Reference.md).

### Operating Notes: Start, Stop, Durability

* **First start.** Each WAL shard gets a pre-zeroed 64 MiB ring (`DataStore/WAL/shard_<N>.WAL`), created on first use; allow about a second per shard before the first insert is accepted. Indexes (`.zmap`/`.catidx`) that are stale or were left dirty by an unclean stop are rebuilt at startup (a full-year rebuild takes ~1 min at 700k rows); the server logs each rebuild.
* **Stopping.** `SIGTERM` or `SIGINT` shuts down cleanly and exits 0: WAL writers stop, one final checkpoint syncs the data files, and the indexes are marked clean (about 0.3 s after 700k rows), so the next start does not replay or rebuild. `kill -9`, a crash or power loss is safe for acknowledged SYNC inserts (they are replayed from the WAL), but the indexes of replayed years are rebuilt on the next start.
* **Durability.** SYNC inserts survive power loss once acknowledged; ASYNC inserts are acknowledged before the WAL sync and can lose a short window; BYPASS inserts are not WAL-covered. After a real disk write error the server logs a critical message and must be restarted. Details: [Technical Reference](docs/XStore_Technical_Reference.md) §4.18-4.19.
* **Checkpoint tuning (optional).** `XSTORE_CKPT_IDLE_MS` (default 50: writer idle time that triggers a checkpoint) and `XSTORE_CKPT_MAX_AGE_MS` (default 5000: longest unsynced data is left). Digits only; `0`/unset = default; the effective values are logged at startup.
* **WAL ring size (optional).** `XSTORE_WAL_RING_MB` (default 64, allowed 8..4096): size of WAL rings created after startup; an existing ring keeps the size it was created with. A bigger ring rides out longer write bursts between checkpoints but takes that much more disk per shard and longer to replay after a crash.
* **Transparent huge pages.** The server's heap uses THP (+23% BatchInsert, +45% RangeInsert at about 2x steady-state memory); the host THP mode must be `madvise` or `always`. Build with `-DXSTORE_JEMALLOC_THP=OFF` to disable.

### Server Deployment Scripts
[benchmark_artifact/scripts/XStore/](benchmark_artifact/scripts/XStore/) has scripts to provision and run an XStore server end-to-end:
* `initXStoreServer.sh <working_dir>` — installs bootstrap dependencies, clones XStore, compiles XStore (CMake fetches & builds protobuf, libzmq and libsodium from source), opens firewall port 9497.
* `runXStoreServer.sh <isPurge=true|false> <working_dir>` — starts the server in tmux session `XStore`; `isPurge=true` wipes the existing DataStore first, `false` keeps it.

# How to Use
* [XStore API](https://gitlab.com/lvn2007/XStore/-/wikis/api)
* [Data Structure](https://gitlab.com/lvn2007/XStore/-/wikis/Data%20Structure)
* [Technical Reference](docs/XStore_Technical_Reference.md)

### Client Libraries
* [C++ client](client_src/Cpp/) — see [example](client_src/Cpp/example/XStore_Client_Example.cpp)
* [Python client](client_src/Python/XStore.py)

### XStore Viewer
A Tauri + Rust GUI for browsing an XStore server: database and per-year file browser with row-slot fit badges, range queries rendered as plots or windowed raw tables, server-side min/max/sum/avg aggregates over any range, click-to-inspect raw rows, and a live server CPU/memory/network/disk monitor. Runs as a native desktop app or as a self-hosted web server. Source: [tools/xstore-viewer/](tools/xstore-viewer/)

![XStore Viewer](tools/xstore-viewer/XStore_Viewer.png)

#### 1. Install prerequisites
No Node.js/npm needed (the frontend is static HTML/JS/CSS), and ZeroMQ is compiled from source at build time (only a C compiler is required, no library install). Open the dropdown for your platform and run the numbered steps in order; every command is copy-pasteable.

<details><summary>Linux (Ubuntu/Debian)</summary>

```bash
# 1. Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"

# 2. Tauri CLI (v2)
curl --proto '=https' --tlsv1.2 -sSf https://tauri.app/installer | sh

# 3. System dependencies: WebKit/GTK (Tauri), C toolchain, protoc
sudo apt update
sudo apt install -y libwebkit2gtk-4.1-dev build-essential curl wget file libxdo-dev libssl-dev libgtk-3-dev protobuf-compiler
```

Other distros: install the webkit2gtk 4.1 + GTK 3 development packages and `protoc` via your package manager, plus steps 1–2.

</details>

<details><summary>macOS</summary>

```bash
# 1. Xcode command line tools (a dialog appears; click "Install")
xcode-select --install

# 2. Homebrew (skip if already installed; on Apple Silicon, run
#    eval "$(/opt/homebrew/bin/brew shellenv)" afterwards)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 3. Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -sSf | sh

# 4. protoc
brew install protobuf

# 5. Tauri CLI (v2)
curl --proto '=https' --tlsv1.2 -sSf https://tauri.app/installer | sh
```

</details>

<details><summary>Windows</summary>

Run in PowerShell:

```powershell
# 1. Rust toolchain
winget install -e --id Rustlang.Rustup

# 2. MSVC C++ toolchain (Visual Studio Build Tools)
winget install -e --id Microsoft.VisualStudio.2022.BuildTools --override "--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"

# 3. protoc (or download protoc-*-win64.zip from the
#    protobuf releases and add its bin/ to PATH)
winget install -e --id Google.Protobuf

# 4. Tauri CLI (v2)
powershell -c "irm https://tauri.app/installer | iex"
```

The WebView2 runtime (required by Tauri) is preinstalled on current Windows 10/11. Open a **new** terminal afterwards so `PATH` picks up the new tools.

</details>

#### 2. Build
From `tools/xstore-viewer/`:

```bash
cargo build --release
```

Executable lands at `target/release/xstore-viewer` (`xstore-viewer.exe` on Windows). `cargo tauri build` works too — same binary, plus it produces an installer if `bundle.active` is `true` in [`tauri.conf.json`](tools/xstore-viewer/tauri.conf.json) (`false` by default).

#### 3. Run

| Mode | Command | Use it when |
|---|---|---|
| Desktop (default) | `./target/release/xstore-viewer` | you're on a machine with a display |
| Web server | `./target/release/xstore-viewer web --port 8080`, then open `http://<host>:8080` | headless machine, or you'd rather use a browser |

Either mode connects to a running XStore server, `127.0.0.1:9497` by default (the port the deployment scripts open). Run with `-h`/`--help` or `-V`/`--version` for CLI details.

# Benchmarks
Performance evaluated across latency, throughput, aggregate/analytical queries, and backtesting workloads.

<details open><summary>Latency</summary>

![XStore Latency](benchmark_artifact/XStore_latencyNumerical.svg)

</details>

<details open><summary>Throughput</summary>

![XStore Throughput](benchmark_artifact/XStore_throughputNumerical.svg)

</details>

<details open><summary>Aggregates / Analytical Queries</summary>

![XStore Aggregates](benchmark_artifact/XStore_analyticalQueries.svg)

</details>

<details open><summary>Backtesting</summary>

![XStore Backtesting](benchmark_artifact/XStore_Backtester.svg)

</details>