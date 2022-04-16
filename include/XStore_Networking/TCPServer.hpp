#ifndef SCALPTRADE_TCP_SERVER_H
#define SCALPTRADE_TCP_SERVER_H

#include <iostream>
#include <exception>
#include <string>
#include <cstring>

extern "C"
{
    #include <stdlib.h>
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <arpa/inet.h>
    #include <netinet/in.h>
    #include <netdb.h>
    #include <unistd.h>
}

namespace scalptrade
{
    class TCPServerException: public std::exception
    {
        private:
            std::string msg;
        public:
            explicit TCPServerException(const std::string s) : msg(s) { }
            virtual ~TCPServerException() = default;

            virtual const char* what() const throw() { return msg.c_str(); }
    };

    /* TCPServer is an implementation of a very simple TCP server that accepts one client. The server then can be 
     * used to send data exclusivelly to that client
     */
    class TCPServer
    {
        private:
            std::string listenAddress;      // listeninig address for the TCP/IP server
            std::string listenPort;         // listening port for the TCP/IP server
            int serverSocket, clientSocket;
        public:
            explicit TCPServer(std::string listenAddress, std::string listenPort) : 
                listenAddress(listenAddress), listenPort(listenPort), serverSocket(-1), clientSocket(-1) { }
            virtual ~TCPServer() = default;

            // start the TCP server, by creating, binding and listening to the server socket
            virtual void startServer();

            // accept the client and store the client socket
            virtual void acceptClient();

            // send data to the client. This method may call send() multiple times
            virtual void sendDataToClient(char* buffer, size_t length);

            // receive data from the client. This method may call recv() multiple times
            virtual void recvDataFromClient(char* buffer, size_t length);

            // close the server and client sockets if they were propery created
            virtual void closeServer();
    };
}

#endif