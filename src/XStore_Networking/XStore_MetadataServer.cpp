#include "include/Networking/XStore_MetadataServer.h"
#include <algorithm>
#include <mutex>

static std::mutex mutex_lock;
static XStore_Proto::Metadata metadata;

struct ThreadArgs {
    zmq::context_t *zmqCtx;
    int NumThreads;
    std::string ServerAddr;
};

XStore_MetadataServer::XStore_MetadataServer(int numThreads) {
    this->NumThreads = numThreads;
}

/* SCRATCH
*   // // DataNode 1
    XStore_Proto::DataNode* DataNode = &(*metadata.mutable_datanodemap())["192.168.1.1"];
    DataNode->set_ipaddress("192.168.1.1");
    DataNode->set_cpuload(20);

    XStore_Proto::DataBase* currDB = DataNode->mutable_databases();
    XStore_Proto::MonthYearTag* ThisDB = &(*currDB->mutable_dataentries())["AAPL"];
    ThisDB->add_monthyeartag("01-2015");
    ThisDB->add_monthyeartag("02-2015");

    // DataNode 2
    XStore_Proto::DataNodesArray* ThisDataNodeArray2 = metadata.add_datanodesarray();
    XStore_Proto::DataNode* DataNode2 = ThisDataNodeArray2->add_datanode();
    DataNode2->set_ipaddress("192.168.1.1");
    DataNode2->set_cpuload(30);

    XStore_Proto::DataBase* currDB2 = DataNode2->mutable_databases();
    XStore_Proto::MonthYearTag ThisDB2 = (*currDB2->mutable_dataentries())["TSLA"];
    ThisDB2.add_monthyeartag("01-2010");
    ThisDB2.add_monthyeartag("02-2010");
    metadata.PrintDebugString();
 */

short int XStore_MetadataServer::ServerRank(nlohmann::json &JsonConfigFile, std::vector<std::string> *IpAddresses)
{
    std::string CurrentIP;

    for (short int i = 0; i < JsonConfigFile["MetadataNodes"].size(); i++)
    {
        CurrentIP = JsonConfigFile["MetadataNodes"][i];

        if (std::find(IpAddresses->begin(), IpAddresses->end(), CurrentIP) != IpAddresses->end())
        {
            return i;
        }
    }
    return -1;
}

bool XStore_MetadataServer::IsValidReplicationFactor(nlohmann::json &JsonConfigFile)
{
    const short int ReplicationFactor = JsonConfigFile["ReplicationFactor"].get<int>();
    return (ReplicationFactor > 0 && JsonConfigFile["DataNodes"].size() >= ReplicationFactor);
}

void XStore_MetadataServer::StartServer(const std::string& ListenAddr, const std::string& ListenPort, const std::string& ConfigFilename) {
    nlohmann::json configJSON;

    Utilities::ReadJSON(ConfigFilename, configJSON);
    std::cout << configJSON.dump() << std::endl;

    std::vector<std::string> IpAddresses;
    Utilities::GetLocalIPAddresses(&IpAddresses);

    // Validate replication factor
    if (!IsValidReplicationFactor(configJSON))
    {
        spdlog::error("asdasd");
        exit(1);
    }

    // Starts Metadata Server
    zmq::context_t ctx;

    //  Socket to talk to CLIENTS
    zmq::socket_t ClientSocket(ctx, zmq::socket_type::router);
    std::string ServerAddr = "tcp://" + ListenAddr + ":" + ListenPort;

    // Set listenAddr & port internally
    this->ServerAddr = ServerAddr;

    ClientSocket.bind(ServerAddr);
    spdlog::info("[NETWORKING] - Listening on: {}", ServerAddr);

    //  Socket to talk to WORKERS
    zmq::socket_t WorkerSocket(ctx, zmq::socket_type::dealer);
    WorkerSocket.bind("inproc://workers");

    // Build args struct for threads
    ThreadArgs TArgs;
    TArgs.zmqCtx = &ctx;
    TArgs.NumThreads = NumThreads;
    TArgs.ServerAddr = ServerAddr.substr(6, ServerAddr.length());       // Proto string refuses to work with special char like ://

    //  Launch pool of WORKER threads
    for (int i = 0; i < NumThreads; i++) {
        // Spawn thread
        pthread_t worker;
        pthread_create(&worker, NULL, XStore_MetadataServer::WorkerRoutine, &TArgs);
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", NumThreads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(ClientSocket), static_cast<void*>(WorkerSocket), nullptr);
}

void* XStore_MetadataServer::WorkerRoutine(void *args) {
    // Unpack thread args
    ThreadArgs *TArgs = static_cast<ThreadArgs*>(args);
    std::string serverAddr = TArgs->ServerAddr;

    // Instantiate XStore for each thread
    XStore myStore;

    // Each thread owns its own ZMQ context
    zmq::context_t *ctx = TArgs->zmqCtx;
    zmq::socket_t socket (*ctx, zmq::socket_type::rep);
    socket.connect("inproc://workers");

    while (true) {
        //  RECEIVING - Wait for next request from CLIENT
        std::vector<zmq::message_t> clientMsgs;
        zmq::recv_multipart(socket, std::back_inserter(clientMsgs));

        //  Partially parse Protobuf request from CLIENT
        //      ONLY parse the CMD type -- We only interested in field "rType"
        //      Doesn't matter if actual msgLength > reqCMD protobuf obj
        XStore_Proto::ReqCMD clientReqCmd;
        clientReqCmd.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

        //  Parsed CMD from CLIENT
        int rType = clientReqCmd.rtype();
        clientReqCmd.Clear();

        switch (rType) {
            // INFO
            case XStore_Proto::METADATA_UPDATE: {
                    // Parse Protobuf obj from CLIENT
                    XStore_Proto::Metadata_Update_REQ protoFromClient;
                    protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                    // Update Metadata cache
                    {
                        std::scoped_lock lock(mutex_lock);
                        (*metadata.mutable_datanodemap())[protoFromClient.datanode().ipaddress()] = protoFromClient.datanode();
                    }
                    metadata.PrintDebugString();

                    //  Returning Protobuf obj
                    XStore_Proto::API_Response protoToClient;
                    protoToClient.set_statuscode(XStore_Proto::SUCCESS);
                    protoToClient.set_details("Updated metadata successfully");

                    //  Response message
                    size_t protoToClientSize = protoToClient.ByteSizeLong();
                    zmq::message_t responseToClient(protoToClientSize);

                    //  Serialize & Dispatch msg to CLIENT
                    protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                    socket.send(responseToClient, zmq::send_flags::dontwait);
                    break;
            }

        default: break;
        }
    }
}