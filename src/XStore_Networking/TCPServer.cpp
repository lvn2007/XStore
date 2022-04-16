#include "TCPServer.hpp"

namespace scalptrade
{
    void TCPServer::startServer()
    {
        int rc;
        struct addrinfo hints, *serverInfo;

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        memset(&hints, 0, sizeof(hints));
        rc = getaddrinfo(listenAddress.c_str(), listenPort.c_str(), &hints, &serverInfo);
        if (rc != 0) {
            throw TCPServerException("FATAL: Could not get server address information!");
        }
        
        serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype, serverInfo->ai_protocol);
        if (serverSocket < 0) {
            throw TCPServerException("FATAL: Could not create server socket!");
        }

        rc = bind(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
        if (rc < 0) {
            close(serverSocket);
            throw TCPServerException("FATAL: Could not bind server socket!");
        }
        
        rc = listen(serverSocket, 10);
        if (rc == -1) {
            close(serverSocket);
            throw TCPServerException("FATAL: Could not bind server socket!");
        }

        std::cout << "INFO: started TCP server (" << listenAddress << ", " << listenPort << ")" << std::endl;
        std::cout << "INFO: ready to accept connections ..." << std::endl;

        freeaddrinfo(serverInfo);
    }

    void TCPServer::acceptClient()
    {
        socklen_t addrlen;
        struct sockaddr_storage clientInfo;
        char clientAddress[INET6_ADDRSTRLEN];

        clientSocket = accept(serverSocket, (struct sockaddr*) &clientInfo, &addrlen);
        if (clientSocket < 0) {
            close(serverSocket);
            throw TCPServerException("ERROR: Could not accept client!");
        }

        if (((struct sockaddr*) &clientInfo)->sa_family == AF_INET) {
            inet_ntop(clientInfo.ss_family,
                      &(((struct sockaddr_in*) &clientInfo)->sin_addr),
                      clientAddress,
                      sizeof(clientAddress));
        } else {
            inet_ntop(clientInfo.ss_family,
                      &(((struct sockaddr_in6*) &clientInfo)->sin6_addr),
                      clientAddress,
                      sizeof(clientAddress));
        }

        std::cout << "INFO: got connection from: " << clientAddress << std::endl;
    }

    void TCPServer::sendDataToClient(char* buffer, size_t length)
    {
        size_t total = 0;
        size_t bytesleft = length;
        size_t n;

        while (total < length) {
            n = send(clientSocket, &buffer[total], bytesleft, 0);
            if (n == -1) {
                close(serverSocket);
                close(clientSocket);
                throw TCPServerException("ERROR: Could not send data to client!");
            }
            total += n;
            bytesleft -= n;
        }
    }

    void TCPServer::recvDataFromClient(char* buffer, size_t length)
    {
        size_t total = 0;
        size_t bytesleft = length;
        size_t n;

        while (total < length) {
            n = recv(clientSocket, &buffer[total], bytesleft, 0);
            if (n == -1 or n == 0) {
                close(serverSocket);
                close(clientSocket);
                throw TCPServerException("ERROR: Could not receive data from client!");
            }
            total += n;
            bytesleft -= n;
        }
    }

    void TCPServer::closeServer()
    {
        if (clientSocket != -1) {
            close(clientSocket);
        }

        if (serverSocket != -1) {
            close(serverSocket);
        }

        std::cout << "INFO: closed TCP server and client connection!" << std::endl;
    }
}