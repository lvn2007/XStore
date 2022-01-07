#include "include/XStore_Networking/XStore_gRPC.h"

grpc::Status xStoreServiceImplementation::unaryRequest(grpc::ServerContext *context, const xStoregRPC::unaryRequestMsg *request, xStoregRPC::responseData *response) {
    // Extract query request from CLIENT
    long int timestamp = request->timestamp();
    std::string dbNameUnary = request->dbname();

    // Unary query result
    std::string queryResult = myStore.getSingleOpenClose(timestamp, dbNameUnary);

    // Response data to CLIENT
    response->set_dataset(queryResult);

    return grpc::Status::OK;
}

grpc::Status xStoreServiceImplementation::batchRequest(grpc::ServerContext *context, const xStoregRPC::rangeRequestMsg *request, grpc::ServerWriter<xStoregRPC::responseData> *writer) {
    // Extract query request from CLIENT
    long int startTime = request->starttime();
    long int endTime = request->endtime();
    std::string dbName = request->dbname();

    // Vector holding results from batch query
    std::vector<std::string> returnData = myStore.getBatch(startTime, endTime, dbName);

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