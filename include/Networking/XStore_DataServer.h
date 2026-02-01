#ifndef XSTORE_DATASERVER_H
#define XSTORE_DATASERVER_H

#include <include/XStore/XStore.h>
#include <pthread.h>
#include "include/Networking/zmq.hpp"
#include "XStore.pb.h"
#include "include/Networking/zmq_addon.hpp"
#include "include/Logger/Logger.h"
#include "include/Utilities/Utilities.h"

struct ThreadArgs {
    zmq::context_t *zmqCtx;
    int NumThreads;
    std::string ServerAddr;
};

struct ZMQSocket {
    zmq::socket_t socket;
    std::string EndPoint;
};

class XStore_DataServer {
private:
    int NumThreads;
    std::string ServerAddr;

    // Work routines in each thread
    static void* WorkerRoutine(void *args);
    short int ServerRank(nlohmann::json &JsonConfigFile, std::vector<std::string> *IpAddresses);
    bool IsValidReplicationFactor(nlohmann::json &JsonConfigFile);
    void BuildMetadataEndpoints(nlohmann::json &JsonConfigFile, zmq::context_t *ctx, std::vector<ZMQSocket> *MetadataEndpoints);
    void UpdateMetadata();
    void BroadcastMetadata(std::vector<ZMQSocket> *MetadataEndpoints);

public:
    XStore_DataServer(int numThreads);
    void StartServer(const std::string& ListenAddr, const std::string& ListenPort, const std::string& ConfigFilename);
};

#endif //XSTORE_DATASERVER_H
