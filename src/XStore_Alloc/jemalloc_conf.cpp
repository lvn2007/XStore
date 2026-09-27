// Compiled-in jemalloc tuning (read by jemalloc at first allocation; the
// MALLOC_CONF environment variable still overrides). Value chosen by the
// 2026-09-27 measurement -- see docs/XStore_Technical_Reference.md.
extern "C" const char *malloc_conf = "background_thread:true";
