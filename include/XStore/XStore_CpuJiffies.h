#pragma once
#include <cstdint>

namespace sysstats {

// /proc/stat's first line: "cpu  user nice system idle iowait irq softirq steal guest guest_nice". Sums the active
// fields (everything but idle and iowait -- iowait is idle time, just idle-while-a-request-is-outstanding) against the
// sum of every field for the percentage denominator. Both 0 when the file cannot be read or parsed. `path` is
// /proc/stat except in tests. Shared by the system stats (XStore_SysStats.cpp) and the live widen load source.
void ReadCpuJiffies(uint64_t &active, uint64_t &total, const char *path = "/proc/stat");

} // namespace sysstats
