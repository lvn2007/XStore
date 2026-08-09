#ifndef XSTORE_PROTOBENCH_H
#define XSTORE_PROTOBENCH_H

#include "XStore.pb.h"
#include "../include/Utilities.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"

class ProtoBench {
    public:
        static std::vector<std::vector<double>> Run(unsigned int nEntries, unsigned int byteSize, unsigned int nIterations);
    private:

};

#endif //XSTORE_PROTOBENCH_H
