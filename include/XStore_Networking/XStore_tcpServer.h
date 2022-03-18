#ifndef XSTORE_XSTORE_TCPSERVER_H
#define XSTORE_XSTORE_TCPSERVER_H

#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <iostream>
#include <unistd.h>
#include "include/XStore/XStore.h"
#include "include/dataStruct/clientCmdStruct.h"
#include "xStore.pb.h"

class XStore_tcpServer {
    private:
        int PORT = 12345;
        int serverSocket, clientSocket;
        XStore xStore;
    public:
        void startServer();

        void acceptClient();

        void sendMsgInfoToClient(uint32_t nItems, uint32_t totalBytes);

        void sendDataToClient(std::time_t timestamp, std::string dbName);

        void streamDataToClient(std::time_t startTime, std::time_t endTime, std::string dbName);

        clientCmdStruct recvCmdFromClient();

        xStoregRPC::unaryRequestMsg recvCmdFromClientProto();

        void closeServer();
};

#endif //XSTORE_XSTORE_TCPSERVER_H