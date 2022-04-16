//
//  Custom routing Router to Mama (ROUTER to REQ)
//

//#include "zmq.hpp"
#include "zhelpers.hpp"
#include <pthread.h>

static void *
worker_thread(void *arg) {
    zmq::context_t context(1);
    zmq::socket_t worker(context, ZMQ_REQ);

    s_set_id(worker);
    worker.connect("ipc://routing.ipc");

    int total = 0;
    while (1) {
        //  Tell the broker we're ready for work
        s_send(worker, "Hi Boss");

        //  Get workload from broker, until finished
        std::string workload = s_recv(worker);
        if ("Fired!" == workload) {
            std::cout << "Processed: " << total << " tasks" << std::endl;
            break;
        }
        total++;

        //  Do some random work
        s_sleep(within(500) + 1);
    }
    return NULL;
}

int main() {
    zmq::context_t context(1);
    zmq::socket_t broker(context, ZMQ_ROUTER);

    broker.bind("ipc://routing.ipc");

    const int NBR_WORKERS = 10;
    pthread_t workers[NBR_WORKERS];
    for (int worker_nbr = 0; worker_nbr < NBR_WORKERS; worker_nbr++) {
        pthread_create(workers + worker_nbr, NULL, worker_thread, (void *)(intptr_t)worker_nbr);
    }

    //  Run for five seconds and then tell workers to end
    int64_t end_time = s_clock() + 5000;
    int workers_fired = 0;
    while (1) {
        //  Next message gives us least recently used worker
        std::string identity = s_recv(broker);
        s_recv(broker);     //  Envelope delimiter
        s_recv(broker);     //  Response from worker       
        
        s_sendmore(broker, identity);
        s_sendmore(broker, "");
        //  Encourage workers until it's time to fire them
        if (s_clock() < end_time)
            s_send(broker, "Work harder");
        else {
            s_send(broker, "Fired!");
            if (++workers_fired == NBR_WORKERS)
                break;
        }
    }

    for (int worker_nbr = 0; worker_nbr < NBR_WORKERS; worker_nbr++) {
        pthread_join(workers[worker_nbr], NULL);
    }
    return 0;
}
