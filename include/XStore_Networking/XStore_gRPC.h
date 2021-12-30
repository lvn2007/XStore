#ifndef XSTORE_XSTORE_GRPC_H
#define XSTORE_XSTORE_GRPC_H

#include <grpcpp/grpcpp.h>
//#include <grpcpp/server.h>
//#include <grpcpp/server_builder.h>
//#include <grpcpp/server_context.h>
//#include <mathtest.pb.h>
#include <mathtest.grpc.pb.h>
#include "include/XStore/XStore.h"

class MathServiceImplementation final : public mathtest::MathTest::Service {
    public:
        // Instantiate XStore obj
        XStore myStore;

        // gRPC
        grpc::Status sendRequest(grpc::ServerContext *context, const mathtest::MathRequest *request, mathtest::MathReply *response);
        grpc::Status batchRequest(grpc::ServerContext *context, const mathtest::rangeRequest *request, grpc::ServerWriter<mathtest::MathReply>* writer);
        void Run();
};

#endif //XSTORE_XSTORE_GRPC_H