#ifndef XSTORE_XSTORE_ADAPTER_H
#define XSTORE_XSTORE_ADAPTER_H

#include "XStore.pb.h"
#include "XStore_Client.h"
#include "../Base_Adapter.h"

class XStore_Adapter: public Base_Adapter {
    XStore_Client xstoreClient;

    public:
        // Initialization & Connect to XStore server
        XStore_Adapter(std::string SERVER_ADDR, std::string SERVER_PORT);

        // Query A -- Unary SEQ query 1k times
        std::vector<double> queryA(int nIterations);

        // Query B -- Unary RAND query 1k times
        std::vector<double> queryB(int nIterations, long int startTime, long int endTime);
};

#endif //XSTORE_XSTORE_ADAPTER_H
