#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <linux/in.h>
#include <unistd.h>

#define PORT 8080

typedef struct
{
    int sock;
    struct sockaddr address;
    socklen_t addr_len;
} connection_t;

void * process(void * ptr)
{
    char * buffer;
    int len;
    connection_t * conn;
    long addr = 0;

    if (!ptr) pthread_exit(0);
    conn = (connection_t *)ptr;

    /* read length of message */
    read(conn->sock, &len, sizeof(int));
    if (len > 0)
    {
        addr = (long)((struct sockaddr_in *)&conn->address)->sin_addr.s_addr;
        buffer = (char *)malloc((len+1)*sizeof(char));
        buffer[len] = 0;

        /* read message */
        read(conn->sock, buffer, len);

        /* print message */
        printf("%d.%d.%d.%d: %s\n",
               (int)((addr      ) & 0xff),
               (int)((addr >>  8) & 0xff),
               (int)((addr >> 16) & 0xff),
               (int)((addr >> 24) & 0xff),
               buffer);
        free(buffer);
    }

    /* close socket and clean up */
    close(conn->sock);
    free(conn);
    pthread_exit(0);
}

int main(int argc, char ** argv)
{
    int server_fd;
    struct sockaddr_in address;
    connection_t * connection;
    pthread_t thread;

    /* create socket */
//    server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    /* bind socket to port */
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    /* listen on port */
    if (listen(server_fd, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("%s: ready and listening\n", argv[0]);

    while (1)
    {
        /* accept incoming connections */
        connection = (connection_t *)malloc(sizeof(connection_t));
        connection->sock = accept(server_fd, &connection->address, &connection->addr_len);
        if (connection->sock <= 0)
        {
            free(connection);
        }
        else
        {
            /* start a new thread but do not wait for it */
            pthread_create(&thread, 0, process, (void *)connection);
            pthread_detach(thread);
        }
    }

    return 0;
}