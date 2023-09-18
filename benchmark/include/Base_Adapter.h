#ifndef XSTORE_BASE_ADAPTER_H
#define XSTORE_BASE_ADAPTER_H

#include <vector>
#include <random>

class Base_Adapter {
    public:
        // Query A -- Unary SEQ query 1k times
        virtual std::vector<double> queryA();

        // Query B -- Unary RAND query 1k times
        virtual std::vector<double> queryB();

    private:
        virtual std::vector<double> queryA_singleThread();
        virtual std::vector<double> queryB_singleThread();
};

#endif //XSTORE_BASE_ADAPTER_H
