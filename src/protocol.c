#include "protocol.h"
#include "debug.h"

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

int proto_send_packet(int fd, XACTO_PACKET *pkt, void *data) {

    size_t write_size = sizeof(XACTO_PACKET);
    ssize_t cur_w = 0;

    void* pktptr = pkt;

    while (write_size) {
        cur_w = write(fd, pktptr, write_size);

        if (cur_w == -1 || cur_w == 0) {
            debug("send packet: error writing");
            return -1;
        }

        write_size -= cur_w;
        pktptr += cur_w;
        // perror("Error sending packet to the network!");
        // return -1;
    }

    // pkt->type = ntohs(pkt->type);
    // pkt->status = ntohs(pkt->status);
    // pkt->null = ntohs(pkt->null);
    // pkt->serial = ntohl(pkt->serial);
    uint32_t pd_sz = ntohl(pkt->size);
    // pkt->timestamp_sec = ntohl(pkt->timestamp_sec);
    // pkt->timestamp_nsec = ntohl(pkt->timestamp_nsec);

    if (data == NULL) {
        pd_sz = 0;
    }    

    if (data != NULL && pd_sz > 0) {
        write_size = pd_sz;
        debug("Write PD_SZ: %ld", write_size);
        cur_w = 0;
        void* dp = data;
        while (write_size) {
            cur_w = write(fd, dp, write_size);
            if (cur_w == -1 || cur_w == 0) {
                debug("send packet: error writing");
                return -1;
            }
            write_size -= cur_w;
            dp += cur_w;
            // perror("Error sending packet data to the network!");
            // return -1;           
        }       
    }      

    return 0;
}

int proto_recv_packet(int fd, XACTO_PACKET *pkt, void **datap) {

    size_t read_sz = sizeof(XACTO_PACKET);
    void* pkptr = (void*) pkt;
    
    while (read_sz) {
        debug("readsize!\n");
        ssize_t cur_read = read(fd, pkptr, read_sz);
        if (cur_read == -1 || cur_read == 0) {
            debug("proto_recv_packet(): Error reading");
            return -1;
        }
        read_sz -= (size_t) cur_read;
        pkptr = (void*) (pkptr + (size_t)cur_read);
    }

    // pkt->status = ntohs(pkt->status);
    // pkt->null = ntohs(pkt->null);
    // pkt->serial = ntohl(pkt->serial);
    size_t psize = ntohl(pkt->size);
    // pkt->timestamp_nsec = ntohl(pkt->timestamp_nsec);
    // pkt->timestamp_sec = ntohl(pkt->timestamp_sec);
    
    if (pkt->size == 0) {
        datap = NULL;
        return 0;
    }

    if (datap == NULL) {
        perror("datap NULL?");
        return 0;
    }

    debug("PSIZE: %ld", psize);

    *datap = calloc(1, psize);
    if (datap == NULL) {
        perror("proto_recv_packet(): Error mallocing");
        return -1;
    }

    void* dp = *datap;

    read_sz = psize;
    while (read_sz) {
        ssize_t cur_read = read(fd, *datap, read_sz);
        if (cur_read == -1 || cur_read == 0) {
            debug("proto_recv_packet(): Error reading");
            free(datap);
            return -1;
        }

        read_sz -= (size_t) cur_read;

        dp = (void*)(dp + (size_t)cur_read);
    }

   return 0;

}