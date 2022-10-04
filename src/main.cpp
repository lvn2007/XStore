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
//    myStore.unaryInsert(1483228802, "0,hello,NaN,NaN,NaN,NaN,NaN", "TSLA");
//    myStore.unaryInsert(1583228802, "0,hello,NaN,NaN,NaN,NaN,NaN", "TSLA");
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

    // REMOVE DB FILES TEST
//    XStore_Proto::removeDBFiles_REQ fromClient;
//    fromClient.set_dbname("TSLA");
//    fromClient.add_filename("2017.XSTORE");
//    fromClient.add_filename("2020.XSTORE");

//    std::vector<std::string> asd(fromClient.filename().begin(), fromClient.filename().end());
//
//    for (int i = 0; i < asd.size(); i++) {
//        std::cout << "File no: " << i << " -- " << asd[i] << std::endl;
//    }
//    myStore.removeDBFiles(fromClient.dbname(), &fromClient);

//    fromClient.set_dbname("ABC");
//    fromClient.add_filename("2017.XSTORE");
//    fromClient.add_filename("2018.XSTORE");

    // BATCH INSERT TEST
//    myStore.createDB("ABC", 64);
//    XStore_Proto::batchInsert_REQ fromClient;
//    fromClient.set_rtype(XStore_Proto::BATCHINSERT);
//
//    for (int i = 0; i < 3; i++) {
//        XStore_Proto::batchInsert_DATA *batchInsert_DATA = fromClient.add_batchinsert_data();
//        batchInsert_DATA->set_dbname("ABC");
//        batchInsert_DATA->set_timestamp(1483228800 + i);
//        batchInsert_DATA->set_data("hello " + std::to_string(i));
//    }
//
//    XStore_Proto::batchInsert_REP batch_insert_rep = myStore.batchInsert(&fromClient);
//    batch_insert_rep.PrintDebugString();
//    XStore_Proto::unaryQuery_REP qRep = myStore.unaryQuery(1483228801, "abc");
//    qRep.PrintDebugString();

//    asd.PrintDebugString();
    // ----DEV AREA
    XStore_zmqServer xStoreZmqServer(THREAD_COUNT);
    xStoreZmqServer.startServer(BIND_ADDR, BIND_PORT);
}