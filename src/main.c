#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "validate_args.h"
#include "vars.h"
#include "server.h"
#include "csapp.h"

#include <signal.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

int listen_FD, *conn_FD;

static void terminate(int status);

CLIENT_REGISTRY *client_registry;

typedef struct {
    CLIENT_REGISTRY *cr;
    pthread_mutex_t mutex;
} ThreadData;

// void* register_thread(void* arg) {
//     ThreadData* td = (ThreadData*)arg;

//     for (int i = 0; i < __FD_SETSIZE; ++i) {
//         // Simulate client registration
//         creg_register(td->cr, i);
//         sleep(0.5);
//         creg_unregister(td->cr, i);
//     }

//     pthread_exit(NULL);
// }

void sighup_handler(int siginfo, siginfo_t* info, void* context) {
    debug("Inside the handler");
    cont = 0;
    terminate(EXIT_SUCCESS);
}

int main(int argc, char* argv[]){
    // Option processing should be performed here.
    // Option '-p <port>' is required in order to specify the port number
    // on which the server should listen.

    int port = validate_args(argc, argv);

    if (port == -1) {
        fprintf(stderr, "Usage: bin/xacto -p <port>\n");
        exit(EXIT_SUCCESS);
    }

    // Perform required initializations of the client_registry,
    // transaction manager, and object store.
    client_registry = creg_init();
    trans_init();
    store_init();

    // TODO: Set up the server socket and enter a loop to accept connections
    // on this socket.  For each connectEXIT_FAILUREion, a thread should be started to
    // run function xacto_client_service().  In addition, you should install
    // a SIGHUP handler, so that receipt of SIGHUP will perform a clean
    // shutdown of the server.

    struct sigaction act_sighup = { 0 };
    act_sighup.sa_sigaction = &sighup_handler;

    if (sigaction(SIGHUP, &act_sighup, NULL) == -1) {
        perror("sigaction: Error setting up signal handler for SIGHUP");
        exit(EXIT_FAILURE);
    }

    socklen_t clientlen;

    struct sockaddr_storage client_addr;
    char client_hostname[256], client_port[256];

    listen_FD = Open_listenfd(argv[2]);

    if (listen_FD == -1) {
        perror("listen FD: Error accepting connections. Please try again");
        terminate(EXIT_FAILURE);
    }

    pthread_t tid;

    while (cont) {
        clientlen = sizeof(struct sockaddr_storage);
        conn_FD = (int*) malloc(sizeof(int));
        if (conn_FD == NULL) {
            perror("conn_FD null ptr: Error accepting connections. Please try again");
            terminate(EXIT_FAILURE);
        }
        *conn_FD = Accept(listen_FD, (struct sockaddr *)&client_addr, &clientlen);

        if (*conn_FD == -1) {
            perror("conn_fd: Error accepting connections. Please try again");
            terminate(EXIT_FAILURE);
        }

        if (getnameinfo((struct sockaddr *) &client_addr, clientlen, client_hostname, MAXLINE, client_port, MAXLINE, 0) == -1) {
            perror("fetnameinfo: Error accepting connections. Please try again");
            terminate(EXIT_FAILURE);
        }

        if (pthread_create(&tid, NULL, xacto_client_service, conn_FD) == -1) {
            perror("Error creating thread");
            terminate(EXIT_FAILURE);
        }

    }

    // fprintf(stderr, "You have to finish implementing main() "
	//     "before the Xacto server will function.\n");

    // for (int i = 0; i < 50; i++) {
    //     creg_register(client_registry, i);
    // }

    // pthread_t register_threads[NUM_THREADS];

    // ThreadData threadData = { .cr = client_registry, .mutex = PTHREAD_MUTEX_INITIALIZER};

    // for (int i = 0; i < NUM_THREADS; i++) {
    //     pthread_create(&register_threads[i], NULL, register_thread, (void*)&threadData);
    // }

    // for (int i = 0; i < NUM_THREADS; i++) {
    //     pthread_join(register_threads[i], NULL);
    // }

    // // terminate(EXIT_SUCCESS);

    // creg_wait_for_empty(client_registry);
    // creg_fini(client_registry);


    terminate(EXIT_SUCCESS);
}

/*
 * Function called to cleanly shut down the server.
 */
void terminate(int status) {
    if (listen_FD)
        close(listen_FD);
    
    if (conn_FD)
        free(conn_FD);
            
    // Shutdown all client connections.
    // This will trigger the eventual termination of service threads.
    creg_shutdown_all(client_registry);
    
    debug("Waiting for service threads to terminate...");
    creg_wait_for_empty(client_registry);
    debug("All service threads terminated.");

    // Finalize modules.
    creg_fini(client_registry);
    trans_fini();
    store_fini();

    debug("Xacto server terminating");
    exit(status);
}
