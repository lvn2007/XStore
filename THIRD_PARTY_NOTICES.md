# Third-Party Notices

XStore's own code is licensed under the Apache License 2.0 (see [LICENSE](LICENSE)).
The components below are **not** covered by that license; each keeps its own license, listed here.

## Vendored in this repository

Copied into `third_party/` (and `client_src/Cpp/third_party/`); they ship with the source.

| Component | Version | License | Copyright |
|---|---|---|---|
| [spdlog](https://github.com/gabime/spdlog) | 1.14.1 | MIT | Copyright (c) 2016 Gabi Melman & spdlog contributors |
| [{fmt}](https://github.com/fmtlib/fmt) (bundled inside spdlog) | 10.2.1 | MIT | Copyright (c) 2012 - present, Victor Zverovich |
| [cppzmq](https://github.com/zeromq/cppzmq) (`zmq.hpp`, `zmq_addon.hpp`) | 4.10.0 | MIT | Copyright (c) 2016-2017 ZeroMQ community, (c) 2009-2011 250bpm s.r.o., (c) 2011 Botond Ballo, (c) 2007-2009 iMatix Corporation |
| [argparse](https://github.com/p-ranav/argparse) | see header | MIT | Copyright (c) 2019-2022 Pranav Srinivas Kumar |
| `zhelpers.hpp` | - | ZeroMQ Guide example code (MIT) | ZeroMQ community. Not referenced by the build. |

## Downloaded and built at configure time (CMake `FetchContent`)

Not stored in this repository. The upstream license file is in each source tree after configuring, at `build/_deps/<name>-src/`.

| Component | Version | License | Used for |
|---|---|---|---|
| [libzmq](https://github.com/zeromq/libzmq) | 4.3.5 | MPL-2.0 | Networking (built as a shared library) |
| [libsodium](https://github.com/jedisct1/libsodium) | 1.0.18 | ISC | CURVE encryption for libzmq |
| [Protocol Buffers](https://github.com/protocolbuffers/protobuf) | 35.1 | BSD-3-Clause | Wire format |
| [Abseil](https://github.com/abseil/abseil-cpp) | pinned by protobuf | Apache-2.0 | Dependency of protobuf |
| [RE2](https://github.com/google/re2) | pinned by protobuf | BSD-3-Clause | Dependency of protobuf |
| [concurrentqueue](https://github.com/cameron314/concurrentqueue) | 1.0.4 | BSD-2-Clause or BSL-1.0 (dual); its bundled semaphore is zlib | Lock-free queue |
| [indicators](https://github.com/p-ranav/indicators) | 2.3 | MIT (its `termcolor.hpp` is BSD-3-Clause, see `LICENSE.termcolor`) | Progress bars |
| [jemalloc](https://github.com/jemalloc/jemalloc) | 5.4.0 | BSD-2-Clause | Memory allocator (optional, `XSTORE_USE_JEMALLOC`) |
| [GoogleTest](https://github.com/google/googletest) | 1.17.0 | BSD-3-Clause | Tests only, not shipped |

Python client (`client_src/Python`): requires `protobuf` (BSD-3-Clause) and `pyzmq` (BSD-3-Clause), which users install themselves.

## Not covered here

`tools/xstore-viewer` is a Rust/Tauri application with its own Cargo and npm dependencies (see its `Cargo.lock`). If you distribute a viewer binary, generate its notices separately (for example with `cargo about`).

## If you redistribute binaries

Keep the copyright notices and license texts of the components above with the binaries. Notes on the less common licenses:

- **MPL-2.0 (libzmq):** file-level copyleft. You may link libzmq into a program under any license, but changes to libzmq's own files must be published under MPL-2.0.
- **Apache-2.0 (Abseil):** keep its `LICENSE` and any `NOTICE` file.

## License texts for vendored components

The MIT License (spdlog, {fmt}, cppzmq, argparse), with each component's copyright line from the table above:

```
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

The upstream copies, including {fmt}'s own license file, are kept in `third_party/spdlog/fmt/bundled/fmt.license.rst` and `client_src/Cpp/third_party/fmt/fmt.license.rst`.
