// Compiled-in jemalloc tuning (read by jemalloc at first allocation; the
// MALLOC_CONF environment variable still overrides). Values chosen by the
// 2026-09-27 measurements -- see docs/XStore_Technical_Reference.md.
// XSTORE_JEMALLOC_THP (CMake option, default ON): heap on transparent huge
// pages -- +23% BatchInsert / +45% RangeInsert, queries unchanged, at ~2x
// steady-state resident memory (bounded, not growing). Needs the host's THP
// mode to be "madvise" or "always".
#if XSTORE_JEMALLOC_THP
extern "C" const char *malloc_conf = "background_thread:true,thp:always";
#else
extern "C" const char *malloc_conf = "background_thread:true";
#endif
