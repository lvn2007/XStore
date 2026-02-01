#include "include/Networking/XStore_DataServer.h"
#include <algorithm>
#include <filesystem>
#include <mutex>

static std::mutex mutex_lock;
static XStore_Proto::DataNode metadata;
static std::vector<ZMQSocket> MetadataEndpoints;
int HeartbeatInvl = 10'000, HeartbeatTimeout = 60'000, HeartbeatTTL = 60'000;

XStore_DataServer::XStore_DataServer(int numThreads) {
    this->NumThreads = numThreads;
}

short int XStore_DataServer::ServerRank(nlohmann::json &JsonConfigFile, std::vector<std::string> *IpAddresses)
{
    std::string CurrentIP;

    for (int i = 0; i < JsonConfigFile["MetadataNodes"].size(); i++)
    {
        CurrentIP = JsonConfigFile["MetadataNodes"][i];

        if (std::find(IpAddresses->begin(), IpAddresses->end(), CurrentIP) != IpAddresses->end())
        {
            return i;
        }
    }
    return -1;
}

bool XStore_DataServer::IsValidReplicationFactor(nlohmann::json &JsonConfigFile)
{
    const short int ReplicationFactor = JsonConfigFile["ReplicationFactor"].get<int>();
    return (ReplicationFactor > 0 && JsonConfigFile["DataNodes"].size() >= ReplicationFactor);
}

void XStore_DataServer::BuildMetadataEndpoints(nlohmann::json &JsonConfigFile, zmq::context_t *ctx, std::vector<ZMQSocket> *MetadataEndpoints)
{
    std::string CurrentEndpoint;

    for (int i = 0; i < JsonConfigFile["MetadataNodes"].size(); i++)
    {
        CurrentEndpoint = JsonConfigFile["MetadataNodes"][i];
        MetadataEndpoints->emplace_back(ZMQSocket{zmq::socket_t(*ctx, zmq::socket_type::req), CurrentEndpoint});
        MetadataEndpoints->back().socket.set(zmq::sockopt::heartbeat_ivl, HeartbeatInvl);
        MetadataEndpoints->back().socket.set(zmq::sockopt::heartbeat_timeout, HeartbeatTimeout);
        MetadataEndpoints->back().socket.set(zmq::sockopt::heartbeat_ttl, HeartbeatTTL);
        MetadataEndpoints->back().socket.connect(fmt::format("tcp://{}", CurrentEndpoint));
    }
}

void XStore_DataServer::BroadcastMetadata(std::vector<ZMQSocket> *MetadataEndpoints)
{
    // Build metadata of this data server
    XStore_Proto::Metadata_Update_REQ ToMetadataServer;
    ToMetadataServer.set_rtype(XStore_Proto::METADATA_UPDATE);
    ToMetadataServer.mutable_datanode()->MergeFrom(metadata);
    ToMetadataServer.PrintDebugString();

    // Build ZMQ msg
    size_t ByteSize = ToMetadataServer.ByteSizeLong();
    zmq::message_t Message(ByteSize);
    ToMetadataServer.SerializeToArray(Message.data(), ByteSize);

    for (int i = 0; i < MetadataEndpoints->size(); i++)
    {
        MetadataEndpoints->at(i).socket.send(Message, zmq::send_flags::dontwait);
    }

    // TODO: Wait for Acknowledgement
}

void XStore_DataServer::UpdateMetadata()
{
    const std::string DataStoreDir = fmt::format("{}/DataStore", std::filesystem::current_path().string());
    std::string DBName, MonthYearTag;

    // Loop through each DB Name
    //  Add to database list if there's any data
    XStore_Proto::DataBase* CurrDB = metadata.mutable_databases();

    for (const std::filesystem::directory_entry &Entry : std::filesystem::directory_iterator(DataStoreDir))
    {
        // Now in each DB dir
        for (const std::filesystem::directory_entry &DataFile : std::filesystem::directory_iterator(Entry))
        {
            DBName = DataFile.path().parent_path().filename().string();

            // Only .XSTORE is valid
            if (DataFile.path().filename().extension().string() == ".XSTORE")
            {
                MonthYearTag = Utilities::SplitString(DataFile.path().filename().string(), ".").at(0);
                XStore_Proto::MonthYearTag* ThisDB = &(*CurrDB->mutable_dataentries())[DBName];
                ThisDB->add_monthyeartag(MonthYearTag);
            }
        }
    }
}

void XStore_DataServer::StartServer(const std::string& ListenAddr, const std::string& ListenPort, const std::string& ConfigFilename) {
    nlohmann::json configJSON;
    zmq::context_t ctx;

    Utilities::ReadJSON(ConfigFilename, configJSON);

    std::vector<std::string> IpAddresses;
    Utilities::GetLocalIPAddresses(&IpAddresses);

    // Initial build metadata of this data server
    metadata.set_ipaddress(fmt::format("{}:{}", IpAddresses[0], ListenPort));
    metadata.set_cpuload(Utilities::GetLoadAvg());
    XStore_DataServer::UpdateMetadata();

    // Metadata endpoints
    XStore_DataServer::BuildMetadataEndpoints(configJSON, &ctx, &MetadataEndpoints);
    XStore_DataServer::BroadcastMetadata(&MetadataEndpoints);

    // Starts Metadata Server
    //  Socket to talk to CLIENTS
    std::string ServerAddr = fmt::format("tcp://{}:{}", ListenAddr, ListenPort);
    zmq::socket_t FrontEnd(ctx, zmq::socket_type::router);
    FrontEnd.set(zmq::sockopt::heartbeat_ivl, HeartbeatInvl);
    FrontEnd.set(zmq::sockopt::heartbeat_timeout, HeartbeatTimeout);
    FrontEnd.set(zmq::sockopt::heartbeat_ttl, HeartbeatTTL);
    FrontEnd.bind(ServerAddr);
    spdlog::info("[NETWORKING] - Listening on: {}", ServerAddr);

    // Set listenAddr & port internally
    this->ServerAddr = ServerAddr;

    //  Socket to talk to WORKERS
    zmq::socket_t BackEnd(ctx, zmq::socket_type::dealer);
    BackEnd.bind("inproc://workers");

    // Build args struct for threads
    ThreadArgs TArgs;
    TArgs.zmqCtx = &ctx;
    TArgs.NumThreads = NumThreads;
    TArgs.ServerAddr = ServerAddr.substr(6, ServerAddr.length());       // Proto string refuses to work with special char like ://

    //  Launch pool of WORKER threads
    for (int i = 0; i < NumThreads; i++) {
        // Spawn thread
        pthread_t worker;
        pthread_create(&worker, NULL, XStore_DataServer::WorkerRoutine, &TArgs);
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", NumThreads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(FrontEnd), static_cast<void*>(BackEnd), nullptr);
}

void* XStore_DataServer::WorkerRoutine(void *args) {
    // Unpack thread args
    ThreadArgs *TArgs = (ThreadArgs*) args;
    std::string serverAddr = TArgs->ServerAddr;

    // Instantiate XStore for each thread
    XStore MyStore;

    // Each thread owns its own ZMQ context
    zmq::context_t *ctx = TArgs->zmqCtx;
    zmq::socket_t socket (*ctx, zmq::socket_type::rep);
    socket.set(zmq::sockopt::heartbeat_ivl, HeartbeatInvl);
    socket.set(zmq::sockopt::heartbeat_timeout, HeartbeatTimeout);
    socket.set(zmq::sockopt::heartbeat_ttl, HeartbeatTTL);
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
            case XStore_Proto::CREATE_DB: {
                    // Properly parse Protobuf obj from CLIENT
                    XStore_Proto::CreateDB_REQ ProtoFromClient;
                    ProtoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                    // XStore routine
                    MyStore.CreateDB(ProtoFromClient.dbname(), ProtoFromClient.timeresolution(), ProtoFromClient.rowlength());

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