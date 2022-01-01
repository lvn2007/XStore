#ifndef XSTORE_XSTORE_GRPC_H
#define XSTORE_XSTORE_GRPC_H

#include <grpcpp/grpcpp.h>
#include <xStore.grpc.pb.h>
#include "include/XStore/XStore.h"

class xStoreServiceImplementation final : public xStoregRPC::xStore::Service {
    public:
        // Instantiate XStore obj
        XStore myStore;

        // gRPC
        grpc::Status unaryRequest(grpc::ServerContext *context, const xStoregRPC::unaryRequestMsg *request, xStoregRPC::responseData *response);
        grpc::Status batchRequest(grpc::ServerContext *context, const xStoregRPC::rangeRequestMsg *request, grpc::ServerWriter<xStoregRPC::responseData>* writer);
        void Run();
};

#endif //XSTORE_XSTORE_GRPC_H