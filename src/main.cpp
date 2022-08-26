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

//    xstoreProto::unaryQuery_REP ur = myStore.unaryQuery(1483228802, "asd");
//    ur.PrintDebugString();

//    xstoreProto::batchResponse br = myStore.getBatch(1483228800, 1483228805, "tsla");
//    br.PrintDebugString();

    // INFO REP TEST
//    xstoreProto::infoResponse ir = myStore.infoQuery("192.168.1.1", "12345", 10);
//    ir.PrintDebugString();

    // UNARY INSERT TEST
//    xstoreProto::createDB_REP createDbRep = myStore.createDB("asd", 64);
//    xstoreProto::unaryInsert_REP insertRep = myStore.unaryInsert(1483228802, "hello", "asd");
//    createDbRep.PrintDebugString();
//    insertRep.PrintDebugString();

//    xstoreProto::removeDBFiles_REQ fromClient;
//    fromClient.set_rtype(xstoreProto::REMOVEDBFILES);
//    fromClient.set_dbname("ABC");
//    fromClient.add_filename("2017.XSTORE");
//    fromClient.add_filename("2018.XSTORE");
//
//    std::vector<std::string> targetVector(fromClient.filename().begin(), fromClient.filename().end());
//    xstoreProto::removeDBFiles_REP asd = myStore.removeDBFiles(fromClient.dbname(), targetVector);
//    asd.PrintDebugString();
    // ----DEV AREA

    XStore_zmqServer xStoreZmqServer(30);
    xStoreZmqServer.startServer("127.0.0.1", "12345");
}