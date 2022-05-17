#include <iostream>
//#include "include/XStore_Networking/XStore_gRPC.h"
#include "include/XStore_Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"

int main(int argc, char *argv[]) {
    // ----DEV AREA
    // Instantiate XStore obj
    XStore myStore;
////    myStore.insert(1514764801, "123", "tsla");
////    std::cout << myStore.getSingleOpenClose(1514764802, "tsla") << std::endl;
//    myStore.createDB("tsla", 64);
//    myStore.insert(1483228802, "0,hello,NaN,NaN,NaN,NaN,NaN", "TSLA");
//    std::string res = myStore.getSingleOpenClose(1483228801, "TSLA");
//    std::cout << res << std::endl;

//    xstoreProto::unaryResponse ur = myStore.getSingleOpenClose(1483228801, "tsla");
//    ur.PrintDebugString();

    xstoreProto::batchResponse br = myStore.getBatch(1483228800, 1483228805, "tsla");
    br.PrintDebugString();
    // ----DEV AREA

//    XStore_zmqServer xStoreZmqServer(30);
//    xStoreZmqServer.startServer("172.20.3.113", "12345");
}