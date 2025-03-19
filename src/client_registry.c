#include "client_registry.h"

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include <unistd.h>
#include <sys/socket.h>

#include "debug.h"

typedef struct client_registry {
    pthread_mutex_t mutex;
    sem_t sem;
    int* fds_list;
    int fds_count;
    size_t capacity;
} client_registry;


CLIENT_REGISTRY *creg_init() {
    CLIENT_REGISTRY *clr = malloc(sizeof(CLIENT_REGISTRY));
    if (clr == NULL) {
        perror("Error mallocing client registry");
        return NULL;
    }

    // start capacity = 10
    clr->capacity = 10;
    clr->fds_list = (int*)malloc(sizeof(int) * clr->capacity);

    sem_init(&clr->sem, 0, 0);

    if (clr->fds_list == NULL) {
        perror("Error mallocing fds list");
        free(clr);
        return NULL;
    }

    for (int i = 0; i < 10; i++) {
        clr->fds_list[i] = -1;
    }

    clr->fds_count = 0;

    if (pthread_mutex_init(&clr->mutex, NULL) != 0) {
        perror("Error creating the mutex");
        free(clr->fds_list);
        free(clr);
        return NULL;
    }

    return clr;
}

int creg_register(CLIENT_REGISTRY *cr, int fd) {

    if (pthread_mutex_lock(&cr->mutex) < 0) {
        perror("Cannot lock");
        return -1;
    }

    // first check if its not already registered

    for (int i = 0; i < cr->capacity; i++) {
        int cur_fd = cr->fds_list[i];
        if (cur_fd == fd) {
            debug("Already registered\n");
            if (pthread_mutex_unlock(&cr->mutex) < 0) {
                perror("cannot unlock");
                return -1;
            }
            return 0;
        }
    }

    if (cr->fds_count < cr->capacity) {
        for (int i = 0; i < cr->capacity; i++) {
            if (cr->fds_list[i] == -1) {
                cr->fds_list[i] = fd;
                cr->fds_count++;
                debug("Register: %d\n", fd);
                break;
            }
        }
    } else {
        debug("Realloc Register: %d\n", fd);
        cr->capacity *= 2;
        cr->fds_list = realloc(cr->fds_list, sizeof(int) * cr->capacity); // TODO: Make sure realloc is alright here
        if (cr->fds_list == NULL) {
            perror("Error reallocing");
            if (pthread_mutex_unlock(&cr->mutex) < 0) {
                perror("cannot unlock");
                return -1;
            }
            return -1;
        }
        for (int i = cr->fds_count; i < cr->capacity; i++) {
            cr->fds_list[i] = -1;
        }        
        cr->fds_list[cr->fds_count++] = fd;
    }

    if (pthread_mutex_unlock(&cr->mutex) < 0) {
        perror("cannot unlock");
        return -1;
    }

    return 0;
}

int creg_unregister(CLIENT_REGISTRY *cr, int fd) {
    if (pthread_mutex_lock(&cr->mutex) < 0) {
        perror("Cannot lock");
        return -1;
    }

    for (int i = 0; i < cr->capacity; i++) {
        if (cr->fds_list[i] == fd) {
            cr->fds_list[i] = -1;
            cr->fds_count--;
            debug("Unregister: %d\n", fd);
            if (cr->fds_count == 0) {
                sem_post(&cr->sem);
            }
            if (pthread_mutex_unlock(&cr->mutex) < 0) {
                perror("cannot unlock");
                return -1;
            }

            close(fd);

            return 0;
        }
    }
    if (pthread_mutex_unlock(&cr->mutex) < 0) {
        perror("cannot unlock");
        return -1;
    }

    return -1;
}

void creg_wait_for_empty(CLIENT_REGISTRY *cr) {
    sem_wait(&cr->sem);
}

void creg_fini(CLIENT_REGISTRY *cr) {
    free(cr->fds_list);

    sem_destroy(&cr->sem);
    pthread_mutex_unlock(&cr->mutex);
    pthread_mutex_destroy(&cr->mutex);

    free(cr);
}

void creg_shutdown_all(CLIENT_REGISTRY *cr) {
    if (pthread_mutex_lock(&cr->mutex) < 0) {
        perror("Cannot lock");
    }

    for (int i = 0; i < cr->capacity; i++) {
        int fds = cr->fds_list[i];
        if (fds != -1) {
            if (shutdown(fds, SHUT_WR) == -1) {
                perror("Error closing socket");
            }
        }
    }

    if (pthread_mutex_unlock(&cr->mutex) < 0) {
        perror("cannot unlock");
    }

}