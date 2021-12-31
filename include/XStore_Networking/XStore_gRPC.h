#ifndef XSTORE_XSTORE_GRPC_H
#define XSTORE_XSTORE_GRPC_H

#include <grpcpp/grpcpp.h>
#include <mathtest.grpc.pb.h>
#include "include/XStore/XStore.h"

class MathServiceImplementation final : public mathtest::MathTest::Service {
    public:
        // Instantiate XStore obj
        XStore myStore;

        // gRPC
        grpc::Status sendRequest(grpc::ServerContext *context, const mathtest::unaryRequest *request, mathtest::responseData *response);
        grpc::Status batchRequest(grpc::ServerContext *context, const mathtest::rangeRequest *request, grpc::ServerWriter<mathtest::responseData>* writer);
        void Run();
};

#endif //XSTORE_XSTORE_GRPC_H