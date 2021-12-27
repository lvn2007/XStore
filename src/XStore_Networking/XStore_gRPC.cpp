#include "include/XStore_Networking/XStore_gRPC.h"

grpc::Status MathServiceImplementation::sendRequest(grpc::ServerContext *context, const mathtest::MathRequest *request, mathtest::MathReply *response) {
    long int timeStamp = request->timestamp();
        std::string queryResult = myStore.getp(timeStamp, "tsla");
//        std::cout << "Query result: " << queryResult << std::endl;
        response->set_dataset(queryResult);
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