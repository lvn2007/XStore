#ifndef XSTORE_BASE_ADAPTER_H
#define XSTORE_BASE_ADAPTER_H

#include <vector>
#include <any>
#include <string>

class Base_Adapter {
    public:
        // UNARY QUERY
        //  Supports both SEQ & RAND access patterns
        virtual std::vector<double> UnaryQuery() = 0;

        // RANGE QUERY
        //  A batch of queries with SEQ access pattern
        virtual std::vector<double> RangeQuery() = 0;

        // BATCH QUERY
        //  A batch of queries with RAND access pattern
        virtual std::vector<double> BatchQuery() = 0;

        // UNARY INSERT
        //  Supports both SEQ & RAND access patterns
        virtual std::vector<double> UnaryInsert() = 0;

        // RANGE INSERT
        //  A batch of inserts with SEQ access pattern
        virtual std::vector<double> RangeInsert() = 0;

        // BATCH INSERT
        //  A batch of inserts with RAND access pattern
        virtual std::vector<double> BatchInsert() = 0;

    private:
        virtual std::vector<double> UnaryQuery_singleThread() = 0;
        virtual std::vector<double> RangeQuery_singleThread() = 0;
        virtual std::vector<double> BatchQuery_singleThread() = 0;

        virtual std::vector<double> UnaryInsert_singleThread() = 0;
        virtual std::vector<double> RangeInsert_singleThread() = 0;
        virtual std::vector<double> BatchInsert_singleThread() = 0;
};

#endif //XSTORE_BASE_ADAPTER_H
