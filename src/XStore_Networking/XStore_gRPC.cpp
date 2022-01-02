#include "include/XStore_Networking/XStore_gRPC.h"

grpc::Status xStoreServiceImplementation::unaryRequest(grpc::ServerContext *context, const xStoregRPC::unaryRequestMsg *request, xStoregRPC::responseData *response) {
    // Extract query request from CLIENT
    long int timestamp = request->timestamp();

    // Unary query result
    std::string queryResult = myStore.getSingleOpenClose(timestamp, "tsla");

    // Response data to CLIENT
    response->set_dataset(queryResult);

    return grpc::Status::OK;
}

grpc::Status xStoreServiceImplementation::batchRequest(grpc::ServerContext *context, const xStoregRPC::rangeRequestMsg *request, grpc::ServerWriter<xStoregRPC::responseData> *writer) {
    // Extract query request from CLIENT
    long int startTime = request->starttime();
    long int endTime = request->endtime();

    // Vector holding results from batch query
    std::vector<std::string> returnData = myStore.getBatch(startTime, endTime, "tsla");

    // Placeholder for server's response
    xStoregRPC::responseData currResponse;

    // Loop through each item in returnData
    //  On each loop, stream to CLIENT
    for (int i = 0; i < returnData.size(); i++) {
        // Set data per stream
        currResponse.set_dataset(returnData.at(i));

        // STREAM content to CLIENT
        writer->Write(currResponse);
    }

    return grpc::Status::OK;
}

//--- EXECUTION TIME in [s] ---
//-Min: 0.00
//-Max: 0.00
//-Mean: 0.00
//-Stdev: 0.00
//
//--- EXECUTION TIME in [ms] ---
//-Min: 1.57
//-Max: 3.30
//-Mean: 2.04
//-Stdev: 0.13
//
//--- EXECUTION TIME in [µs] ---
//-Min: 1,574.75
//-Max: 3,300.19
//-Mean: 2,036.18
//-Stdev: 128.35
//
//--- EXECUTION TIME in [ns] ---
//-Min: 1,574,754.71
//-Max: 3,300,189.97
//-Mean: 2,036,175.94
//-Stdev: 128,350.64

void xStoreServiceImplementation::Run() {
    std::string address("0.0.0.0:5000");
    xStoreServiceImplementation service;

    grpc::ServerBuilder builder;

    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port: " << address << std::endl;

    server->Wait();
}