#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <fstream>
#include "message.pb.h"

void * cientThread(void *arg) {
    char buffer[1024] = {0};
    void *sendBuff = malloc(5);
    int clientSocket, valRead;
    struct sockaddr_in serverAddr;
    socklen_t addr_size;

    // Create the socket.
    clientSocket = socket(PF_INET, SOCK_STREAM, 0);

    //Configure settings of the server address
    // Address family is Internet
    serverAddr.sin_family = AF_INET;

    //Set port number, using htons function
    serverAddr.sin_port = htons(8080);

    //Set IP address to localhost
    serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

    //Connect the socket to the server using the address
    addr_size = sizeof serverAddr;
    connect(clientSocket, (struct sockaddr *) &serverAddr, addr_size);

    // Instantiate protobuf obj
    tcpmessage::tcpMessage toServer;
    tcpmessage::tcpMessage fromServer;

    // Prepare protobuf msg
    toServer.set_msgcontent("123");
    toServer.SerializeToArray(sendBuff, 5);
    std::cout << "To server: " << toServer.msgcontent() << "\n" << std::endl;

    // Send serialized protobuf to SERVER
    send(clientSocket, sendBuff, 5, 0);

    // Read incoming msg from SERVER
    valRead = read(clientSocket, buffer, 5);
    fromServer.ParseFromArray(buffer, 5);

    std::cout << "From server: " << fromServer.msgcontent() << "\n" << std::endl;

    close(clientSocket);
    pthread_exit(NULL);
}

int main(){
    int i = 0;
    pthread_t tid[51];
    while(i< 50)
    {
        if( pthread_create(&tid[i], NULL, cientThread, NULL) != 0 )
            printf("Failed to create thread\n");
        i++;
    }
    i = 0;
    while(i< 50)
    {
        pthread_join(tid[i++],NULL);
        printf("%d:\n",i);
    }
    return 0;
}