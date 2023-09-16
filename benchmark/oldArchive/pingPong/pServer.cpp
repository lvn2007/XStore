#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <linux/in.h>
#include <unistd.h>
#include "message.pb.h"

#define PORT 8080

typedef struct
{
    int sock;
    struct sockaddr address;
    socklen_t addr_len;
} connection_t;

void * process(void * ptr)
{
    char buffer[1024] = {0};
    void *buff = malloc(5);
    int len;
    connection_t * conn;
    long addr = 0;

    // Initiate protobuf obj
    tcpmessage::tcpMessage toClient;
    tcpmessage::tcpMessage fromClient;

    if (!ptr) pthread_exit(0);
    conn = (connection_t *)ptr;

    // Read incoming msg from CLIENT
    int valRead = read(conn->sock, buffer, 5);
    fromClient.ParseFromArray(buffer, 5);
    std::cout << "From client: " << buffer << "\n" << std::endl;

    // Prepare protobuf msg
    toClient.set_msgcontent("123");
    toClient.SerializeToArray(buff, 5);

    // Send serialized protobuf to CLIENT
    send(conn->sock, buff, 5, 0);

    // Close socket and clean-up
    close(conn->sock);
    free(conn);
//    free(&buffer);
    free(buff);
    pthread_exit(0);
}

int main(int argc, char ** argv)
{
    int server_fd;
    struct sockaddr_in address;
    connection_t * connection;
    pthread_t thread;

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
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

    // Listen on port
    if (listen(server_fd, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    std::cout << "[INIT] - Listening on port: " << PORT << std::endl;

    // Listen forever
    while (1)
    {
        // Accept incoming connections
        connection = (connection_t *)malloc(sizeof(connection_t));
        connection->sock = accept(server_fd, &connection->address, &connection->addr_len);

        if (connection->sock <= 0)
        {
            free(connection);
        }

        // Spin new thread for each connection
        else
        {
            pthread_create(&thread, 0, process, (void *)connection);
            pthread_detach(thread);
        }
    }
}