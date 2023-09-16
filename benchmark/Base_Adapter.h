#ifndef XSTORE_BASE_ADAPTER_H
#define XSTORE_BASE_ADAPTER_H

#include <vector>
#include <random>

class Base_Adapter {
    public:
        // Query A -- Unary SEQ query 1k times
        std::vector<double> queryA(int nIterations);

        // Query B -- Unary RAND query 1k times
        std::vector<double> queryB(int nIterations, long int startTime, long int endTime);
};

#endif //XSTORE_BASE_ADAPTER_H
