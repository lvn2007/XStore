#ifndef XSTORE_BASE_ADAPTER_H
#define XSTORE_BASE_ADAPTER_H

#include <vector>
#include <random>
#include <any>

class Base_Adapter {
    public:
        // Query A -- Unary SEQ query 1k times
        virtual std::vector<double> queryA() = 0;

        // Query B -- Unary RAND query 1k times
        virtual std::vector<double> queryB() = 0;

    private:
        virtual std::vector<double> queryA_singleThread() = 0;
        virtual std::vector<double> queryB_singleThread() = 0;
};

#endif //XSTORE_BASE_ADAPTER_H
