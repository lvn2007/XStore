#include <iostream>
//#include "include/XStore_Networking/XStore_gRPC.h"
#include "include/XStore_Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"
#include "include/dataStructs/clientCmdStruct.h"

int main(int argc, char *argv[]) {
    // ----DEV AREA
    // Instantiate XStore obj
    XStore myStore;
//    myStore.insert(1514764801, "123", "tsla");
//    std::cout << myStore.getSingleOpenClose(1514764802, "tsla") << std::endl;
    myStore.createDB("tsla", 64);
    myStore.insert(1483228802, "0,hello,NaN,NaN,NaN,NaN,NaN", "TSLA");
    std::string res = myStore.getSingleOpenClose(1483228801, "TSLA");
    std::cout << res << std::endl;
//    myStore.insert(1483228801, "0,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606400, "0,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606401, "NaN,1,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606402, "NaN,NaN,2,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451676402, "NaN,NaN,7,NaN,NaN,NaN,NaN", "tsla");

    // ----DEV AREA

//    XStore_zmqServer xStoreZmqServer(30);
//    xStoreZmqServer.startServer("172.20.3.113", "12345");
}