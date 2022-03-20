#include "include/XStore_Networking/XStore_tcpServer.h"

void XStore_tcpServer::startServer() {
    int opt = 1;
    int flag = 1;
    struct sockaddr_in socketAddr;

    // Creating socket
    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("[ERROR] -- Failed to create a socket descriptor");
        exit(EXIT_FAILURE);
    }

    // Applying socket settings
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("[ERROR] -- Failed to apply socket settings");
        exit(EXIT_FAILURE);
    }
//    setsockopt(serverSocket, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int));
    socketAddr.sin_family = AF_INET;
    socketAddr.sin_addr.s_addr = INADDR_ANY;
    socketAddr.sin_port = htons(PORT);

    // Binding socket to PORT
    if (bind(serverSocket, (struct sockaddr *) &socketAddr, sizeof(socketAddr)) < 0) {
        perror("[ERROR] -- Failed to bind socket");
        exit(EXIT_FAILURE);
    }

    // Start listening
    if (listen(serverSocket, 3) < 0) {
        perror("[ERROR] -- Failed to listen");
        exit(EXIT_FAILURE);
    }
}

void XStore_tcpServer::acceptClient() {
    struct sockaddr_in clientAddr;
    char clientIP[INET6_ADDRSTRLEN];
    int addrLen = sizeof(clientAddr);

    if ((clientSocket = accept(serverSocket, (struct sockaddr *) &clientAddr, (socklen_t *) &addrLen)) < 0) {
        perror("[ERROR] -- Failed to accept client");
        exit(EXIT_FAILURE);
    }

    // Convert client's address to string
    inet_ntop(AF_INET, &clientAddr.sin_addr, clientIP, sizeof(clientIP));
    int clientPort = htons(clientAddr.sin_port);

    std::cout << "[INFO] -- Accepted connection from " << clientIP << ":" <<clientPort << std::endl;
}

//void XStore_tcpServer::sendMsgInfoToClient(uint32_t nItems, uint32_t totalBytes) {
//    // Preparing protobuf obj
//    xStoregRPC::messageInfo messageInfo;
//
//    // Configuring messageInfo
//    messageInfo.set_nitems(nItems);
//    messageInfo.set_msglength(totalBytes);
//
//    // messageInfoSize -- Size about message to be sent
//    size_t messageInfoSize = messageInfo.ByteSizeLong();
//
//    // Dispatch messageInfo to CLIENT
//    char messageInfoBuff[messageInfoSize];
//    messageInfo.SerializeToArray(messageInfoBuff, messageInfoSize);
//    size_t n = send(clientSocket, messageInfoBuff, 128, 0);
//
//    if (n <= 0) {
//        close(serverSocket);
//        close(clientSocket);
//        std::cout << "[ERROR] Could not send data to client!" << std::endl;
//    }
//}

void XStore_tcpServer::sendDataToClient(std::time_t timestamp, std::string dbName) {
    // XStore query
    std::string queryResult = xStore.getSingleOpenClose(timestamp, dbName);

    // Preparing protobuf obj
    xStoregRPC::responseData toClient;
    toClient.set_timestamp(timestamp);
    toClient.set_dataset(queryResult);

    // actualData -- Info about message to be sent
    size_t currentByte = 0;
    size_t totalBytes = toClient.ByteSizeLong();
    size_t bytesleft = totalBytes;
    size_t n;

    // Serialized Protobuf obj to bytes array
    char buff[totalBytes];
    toClient.SerializeToArray(buff, totalBytes);

    n = send(clientSocket, buff, 32, 0);
}

xStoregRPC::messageInfo XStore_tcpServer::recvMsgInfoFromClient() {
    // Instantiate protobuf obj
    xStoregRPC::messageInfo fromClient;
    char clientMsgInfo[128];

    // Read incoming messageInfo from CLIENT
    int valRead = read(clientSocket, clientMsgInfo, 128);
    fromClient.ParseFromArray(clientMsgInfo, 128);

    return fromClient;
}

clientCmdStruct XStore_tcpServer::recvCmdFromClient() {
    // Instantiate protobuf obj
    xStoregRPC::messageInfo msgInfoFromClient;
    xStoregRPC::unaryRequestMsg fromClient;
    char clientCmd[32];

    // Read incoming messageInfo from CLIENT
//    msgInfoFromClient = recvMsgInfoFromClient();

    // Read incoming message from CLIENT
    int valRead = recv(clientSocket, clientCmd, 32, 0);
    fromClient.ParseFromArray(clientCmd, 32);

    // Parse for command type requested by CLIENT
    std::string cmdType = fromClient.requestType_Name(fromClient.rtype());

    // Construct clientCmdStruct to return
    clientCmdStruct clientCmdStruct(cmdType, fromClient.timestamp(), 123456789, fromClient.dbname());

    return clientCmdStruct;
}

xStoregRPC::unaryRequestMsg XStore_tcpServer::recvCmdFromClientProto() {
    // Instantiate protobuf obj
    xStoregRPC::unaryRequestMsg fromClient;
    int valRead;
    char clientCmd[32];

    // Read incoming message from CLIENT
    valRead = read(clientSocket, clientCmd, 32);
    fromClient.ParseFromArray(clientCmd, 32);
//    std::cout << "Requested timestamp: " << fromClient.timestamp() << std::endl;
//    std::cout << "Requested dbName: " << fromClient.dbname() << std::endl;
//    std::cout << "Client CMD: " << fromClient.requestType_Name(fromClient.rtype()) << std::endl;

    return fromClient;
}