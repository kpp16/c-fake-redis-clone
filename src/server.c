#include "server.h"
#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "protocol.h"
#include "data.h"
#include "store.h"

#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>

CLIENT_REGISTRY* client_registry;

struct timespec tspec; // get the seconds and nanoseconds

void send_packet_no_data(XACTO_PACKET* pkt, void* data, int* cont, int* is_abort, TRANSACTION* tp, int connfd, int is_commit) {
    // pkt->type = XACTO_REPLY_PKT;
    if (is_commit == 1) {
        pkt->status = TRANS_COMMITTED;
    } else if (is_commit == -1) {
        pkt->status = TRANS_ABORTED;
    } else {
        pkt->status = trans_get_status(tp);
    }
    pkt->null = 1;
    pkt->size = 0;

    clock_gettime(CLOCK_MONOTONIC, &tspec);
    pkt->timestamp_sec = tspec.tv_sec;
    pkt->timestamp_nsec = tspec.tv_nsec;

    if (proto_send_packet(connfd, pkt, data) == -1) {
        *cont = 0;
        *is_abort = 1;
        return;
    }   
}

void send_packet_data(XACTO_PACKET* pkt, void* data, int* cont, int* is_abort, TRANSACTION* tp, int connfd, int size) {
    // pkt->type = XACTO_VALUE_PKT;
    pkt->status = trans_get_status(tp);
    pkt->null = 0;
    pkt->size = size;

    clock_gettime(CLOCK_MONOTONIC, &tspec);
    pkt->timestamp_sec = tspec.tv_sec;
    pkt->timestamp_nsec = tspec.tv_nsec;

    if (proto_send_packet(connfd, pkt, data) == -1) {
        *cont = 0;
        *is_abort = 1;
        return;
    }  
}

void *xacto_client_service(void* fd) {
    int connfd = *((int*) fd);

    if (pthread_detach(pthread_self()) == -1) {
        perror("Error detaching pthread self");
        exit(EXIT_FAILURE);
    }

    free(fd);

    creg_register(client_registry, connfd);

    TRANSACTION* tp = trans_create();
    XACTO_PACKET* pkt = (XACTO_PACKET*) calloc(1, sizeof(XACTO_PACKET));
    
    int cont = 1;
    int is_abort = 0;

    void** datap = (void**) calloc(1, sizeof(void*));

    while (cont == 1) {
        if (proto_recv_packet(connfd, pkt, datap) == -1) {
            trans_ref(tp, "inc so not free abort");
            trans_abort(tp);
            cont = 0;
            is_abort = 1;
            continue;
        }

        *datap = NULL; // next packet the first packet could be GET, PUT, COMMIT

        if (pkt->type == XACTO_PUT_PKT) {
            // current packet is the PUT packet, expect 2 more, i.e, key and then value
            // get the key packet

            if (proto_recv_packet(connfd, pkt, datap) == -1) {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                cont = 0;
                is_abort = 1;
                continue;
            }

            // datap now has the key

            size_t size = ntohl(pkt->size);

            BLOB* bp = blob_create((char*)(*datap), size);
            KEY* key = key_create(bp);

            blob_unref(bp, "created key");

            // recv packet again to get the value
            free(*datap);
            *datap = NULL;

            if (proto_recv_packet(connfd, pkt, datap) == -1) {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                cont = 0;
                is_abort = 1;
                continue;
            }

            size = ntohl(pkt->size);
            BLOB* v_blob = blob_create((char*)(*datap), size);

            // set the store and transaction
            tp->status = store_put(tp, key, v_blob);

            blob_unref(v_blob, "created version unref extra");

            if (tp->status == TRANS_ABORTED) {
                is_abort = 1;
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                cont = 0;
                continue;
            }

            free(*datap);
            *datap = NULL;

            // // send reply
            pkt->type = XACTO_REPLY_PKT;

            send_packet_no_data(pkt, *datap, &cont, &is_abort, tp, connfd, 0);
            if (is_abort == 1) {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                continue;
            }

        } else if (pkt->type == XACTO_GET_PKT) {
            // require another packet => key
            if (proto_recv_packet(connfd, pkt, datap) == -1) {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                cont = 0;
                is_abort = 1;
                continue;
            }

            size_t size = ntohl(pkt->size);

            BLOB* bp = blob_create((char*)(*datap), size);
            KEY* key = key_create(bp);

            blob_unref(bp, "passing down key blob");

            BLOB** val_holder = calloc(1, sizeof(BLOB*));
            store_get(tp, key, val_holder);

            BLOB* value = *val_holder;

            free(*datap);
            *datap = NULL;

            // send an empty reply packet first
            // then send a VALUE packet and then send the value

            pkt->type = XACTO_REPLY_PKT;
            send_packet_no_data(pkt, *datap, &cont, &is_abort, tp, connfd, 0);

            if (is_abort == 1) {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                key_dispose(key);
                continue;
            }

            key_dispose(key);
            // trans_unref(tp, "finish get");

            // send value packet

            pkt->type = XACTO_VALUE_PKT;

            if (value == NULL) {
                *datap = NULL;
                send_packet_no_data(pkt, *datap, &cont, &is_abort, tp, connfd, 0);
            } else {
                debug("Value size: %ld\n", value->size);
                size_t sz = htonl(value->size);
                *datap = value->content;
                send_packet_data(pkt, *datap, &cont, &is_abort, tp, connfd, sz);
            }

            if (is_abort == 1) {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                free(val_holder);
                continue;
            }

            *datap = NULL;
            free(val_holder);

        } else if (pkt->type == XACTO_COMMIT_PKT) {
            trans_ref(tp, "commit transaction"); // Do this so that garbage collector in store works
            // if a comitted transaction is free'd, store.c can't access the dependency list properly.
            if (trans_commit(tp) == TRANS_COMMITTED) {
                pkt->type = XACTO_REPLY_PKT;
                cont = 0;
                send_packet_no_data(pkt, *datap, &cont, &is_abort, tp, connfd, 1);
                if (is_abort == 1) {
                    trans_ref(tp, "inc so not free abort");
                    trans_abort(tp);
                    continue;
                }
            } else {
                trans_ref(tp, "inc so not free abort");
                trans_abort(tp);
                cont = 0;
                is_abort = 1;
                continue;
            }
        } else {
            // abort
            trans_ref(tp, "inc so not free abort");
            trans_abort(tp);
            cont = 0;
            is_abort = 1;
            continue;            
        }
        if (*datap)
            free(*datap);
        *datap = NULL;
    }

    if (is_abort) {
        debug("\n\nIN ABORT\n\n");
        send_packet_no_data(pkt, *datap, &cont, &is_abort, tp, connfd, -1);
    }

    if (*datap)
        free(*datap);
    
    free(datap);

    free(pkt);
    creg_unregister(client_registry, connfd);
    // do stuff here...

    debug("testestset");

    close(connfd);
    return NULL;
}