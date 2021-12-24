#ifndef XSTORE_XSTORE_GRPC_H
#define XSTORE_XSTORE_GRPC_H

#include <grpcpp/grpcpp.h>
#include <mathtest.pb.h>
#include <mathtest.grpc.pb.h>

class MathServiceImplementation final : public mathtest::MathTest::Service{
    public:
        grpc::Status sendRequest(::grpc::ServerContext *context, const ::mathtest::MathRequest *request, ::mathtest::MathReply *response) override;
        void Run();
};


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
//    void Run();

#endif //XSTORE_XSTORE_GRPC_H