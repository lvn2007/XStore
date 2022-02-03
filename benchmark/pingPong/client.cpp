#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "message.pb.h"

#define IP_ADDR "127.0.0.1"
#define PORT 8080

int main(int argc, char const *argv[])
{
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char buffer[1024] = {0};
    void *sendBuff = malloc(5);

    // Instantiate protobuf obj
    tcpmessage::tcpMessage toServer;
    tcpmessage::tcpMessage fromServer;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, IP_ADDR, &serv_addr.sin_addr)<=0)
    {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed \n");
        return -1;
    }

    // Prepare protobuf msg
    toServer.set_msgcontent("123");
    toServer.SerializeToArray(sendBuff, 5);

    // Send serialized protobuf to SERVER
    send(sock, sendBuff, 5, 0);

    // Read incoming msg from SERVER
    valread = read(sock , buffer, 1024);
    fromServer.ParseFromArray(buffer, 5);
    std::cout << "From server: " <<  fromServer.msgcontent() << "\n" << std::endl;
}