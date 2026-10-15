#include "include/XStore/XStore_CpuJiffies.h"

#include <fstream>
#include <string>

namespace sysstats {

void ReadCpuJiffies(uint64_t &active, uint64_t &total, const char *path) {
    active = 0;
    total = 0;
    std::ifstream f(path);
    if (!f.is_open()) return;
    std::string label;
    f >> label; // "cpu"
    if (label != "cpu") return;
    uint64_t user, nice, system, idle, iowait, irq, softirq, steal;
    f >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
    if (f.fail()) return;
    active = user + nice + system + irq + softirq + steal;
    total = active + idle + iowait;
}

} // namespace sysstats
