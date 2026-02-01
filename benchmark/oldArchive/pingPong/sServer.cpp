#include <iostream>
#include "message.pb.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>

#define PORT 8080

// Server side C/C++ program to demonstrate Socket programming
int main(int argc, char const *argv[])
{
    int server_fd, new_socket, valread;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    char buffer[1024] = {0};
    void *buff = malloc(10);

    // Instantiate protobuf obj
    tcpmessage::tcpMessage toClient;
    tcpmessage::tcpMessage fromClient;

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0)
    {
        perror("accept");
        exit(EXIT_FAILURE);
    }

    // Listen forever
    while (true) {
        // Read incoming msg from CLIENT
        valread = read(new_socket , buffer, 10);
        fromClient.ParseFromArray(buffer, 10);
//        std::cout << "From client: " << buffer << "\n" << std::endl;

        // Prepare protobuf msg
        toClient.set_msgcontent(buffer);
//        size_t size = pingPongMsg.ByteSizeLong();
//        buff = malloc(size);
//        std::cout << "To client: " << toClient.msgcontent() << "\n" << std::endl;
        toClient.SerializeToArray(buff, 10);

        // Send serialized protobuf to CLIENT
        send(new_socket, buff, 10, 0);
    }
}