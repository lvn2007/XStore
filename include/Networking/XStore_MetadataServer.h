#ifndef XSTORE_METADATASERVER_H
#define XSTORE_METADATASERVER_H

#include <include/XStore/XStore.h>
#include <pthread.h>
#include "include/Networking/zmq.hpp"
#include "XStore.pb.h"
#include "include/Networking/zmq_addon.hpp"
#include "include/Logger/Logger.h"
#include "include/Utilities/Utilities.h"

class XStore_MetadataServer {
private:
    int NumThreads;
    std::string ServerAddr;

    // Work routines in each thread
    static void* WorkerRoutine(void *args);
    short int ServerRank(nlohmann::json &JsonConfigFile, std::vector<std::string> *IpAddresses);
    bool IsValidReplicationFactor(nlohmann::json &JsonConfigFile);

public:
    XStore_MetadataServer(int numThreads);
    void StartServer(const std::string& ListenAddr, const std::string& ListenPort, const std::string& ConfigFilename);
};

#endif //XSTORE_METADATASERVER_H
