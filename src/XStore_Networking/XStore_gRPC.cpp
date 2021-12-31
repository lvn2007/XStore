#include "include/XStore_Networking/XStore_gRPC.h"

grpc::Status MathServiceImplementation::sendRequest(grpc::ServerContext *context, const mathtest::unaryRequest *request, mathtest::responseData *response) {
    // Extract query request from CLIENT
    long int timestamp = request->timestamp();

    // Unary query result
    std::string queryResult = myStore.getp(timestamp, "tsla");

    // Response data to CLIENT
    response->set_dataset(queryResult);

    return grpc::Status::OK;
}

grpc::Status MathServiceImplementation::batchRequest(grpc::ServerContext *context, const mathtest::rangeRequest *request, grpc::ServerWriter<mathtest::responseData> *writer) {
    // Extract query request from CLIENT
    long int startTime = request->starttime();
    long int endTime = request->endtime();

    // Vector holding results from batch query
    std::vector<std::string> returnData = myStore.getBatch(startTime, endTime, "tsla");

    // Placeholder for server's response
    mathtest::responseData currResponse;

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

void MathServiceImplementation::Run() {
    std::string address("0.0.0.0:5000");
    MathServiceImplementation service;

    grpc::ServerBuilder builder;

    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port: " << address << std::endl;

    server->Wait();
}