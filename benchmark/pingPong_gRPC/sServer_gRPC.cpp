#include <iostream>
#include <grpcpp/grpcpp.h>
#include <message.grpc.pb.h>

class pingPongServiceImplementation final : public tcpmessage::pingPong::Service {
    public:
        grpc::Status unaryRequest(grpc::ServerContext *context, const tcpmessage::tcpMessage *request, tcpmessage::tcpMessage *response) {
            // Extract msg from CLIENT
            std::string clientMsg = request->msgcontent();

            // Response data to CLIENT
            response->set_msgcontent(clientMsg);

            return grpc::Status::OK;
        }

        void Run() {
            std::string address("0.0.0.0:8080");
            pingPongServiceImplementation service;

            grpc::ServerBuilder builder;

            builder.AddListeningPort(address, grpc::InsecureServerCredentials());
            builder.RegisterService(&service);

            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            std::cout << "Server listening on port: " << address << std::endl;

            server->Wait();
        }
};

int main(int argc, char *argv[]) {
    pingPongServiceImplementation pingPongServiceImplementation;
    pingPongServiceImplementation.Run();
}