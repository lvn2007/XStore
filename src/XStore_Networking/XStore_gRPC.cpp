#include "include/XStore_Networking/XStore_gRPC.h"

grpc::Status MathServiceImplementation::sendRequest(::grpc::ServerContext *context, const ::mathtest::MathRequest *request, ::mathtest::MathReply *response) {
        int a = request->a();
        int b = request->b();

        response->set_result(a * b);

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


//class MathServiceImplementation final : public mathtest::MathTest::Service {
//    grpc::Status sendRequest(
//            grpc::ServerContext* context,
//            const mathtest::MathRequest* request,
//            mathtest::MathReply* reply
//    ) override {
//        int a = request->a();
//        int b = request->b();
//
//        reply->set_result(a * b);
//
//        return grpc::Status::OK;
//    }
//};
//
//void Run() {
//    std::string address("0.0.0.0:5000");
//    MathServiceImplementation service;
//
//    grpc::ServerBuilder builder;
//
//    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
//    builder.RegisterService(&service);
//
//    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
//    std::cout << "Server listening on port: " << address << std::endl;
//
//    server->Wait();
//}