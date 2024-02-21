#ifndef XSTORE_BASE_ADAPTER_H
#define XSTORE_BASE_ADAPTER_H

#include <vector>
#include <any>
#include <string>

class Base_Adapter {
    public:
        // UNARY QUERY
        //  Supports both SEQ & RAND access patterns
        virtual std::vector<double> unaryQuery() = 0;

        // RANGE QUERY
        //  A batch of queries with SEQ access pattern
        virtual std::vector<double> rangeQuery() = 0;

        // BATCH QUERY
        //  A batch of queries with RAND access pattern
        virtual std::vector<double> batchQuery() = 0;

        // UNARY INSERT
        //  Supports both SEQ & RAND access patterns
        virtual std::vector<double> unaryInsert() = 0;

        // RANGE INSERT
        //  A batch of inserts with SEQ access pattern
        virtual std::vector<double> rangeInsert() = 0;

        // BATCH INSERT
        //  A batch of inserts with RAND access pattern
        virtual std::vector<double> batchInsert() = 0;

    private:
        virtual std::vector<double> unaryQuery_singleThread() = 0;
        virtual std::vector<double> rangeQuery_singleThread() = 0;
        virtual std::vector<double> batchQuery_singleThread() = 0;

        virtual std::vector<double> unaryInsert_singleThread() = 0;
        virtual std::vector<double> rangeInsert_singleThread() = 0;
        virtual std::vector<double> batchInsert_singleThread() = 0;
};

#endif //XSTORE_BASE_ADAPTER_H
