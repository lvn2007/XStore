#include <iostream>
#include "include/XStore_Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"

int main(int argc, char *argv[]) {
    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR - Arguments] -- Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR - Arguments] -- Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BIND_ADDR = argv[1];
    std::string BIND_PORT = argv[2];
    int THREAD_COUNT = atoi(argv[3]);

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

    // REMOVE DB TEST
//    xstoreProto::removeDB_REQ fromClient;
//    fromClient.set_rtype(xstoreProto::removeDB_REQ);
//    fromClient.set_dbname("abc");

//    myStore.createDB("abc", 64);

//    xstoreProto::removeDB_REP removeDbRep = myStore.removeDB("abc");
//    removeDbRep.PrintDebugString();

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

    XStore_zmqServer xStoreZmqServer(THREAD_COUNT);
    xStoreZmqServer.startServer(BIND_ADDR, BIND_PORT);
}