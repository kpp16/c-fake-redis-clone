#include "data.h"
#include "debug.h"
#include "vars.h"

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>

BLOB *blob_create(char *content, size_t size) {
    BLOB *blob = (BLOB*)calloc(1, sizeof(BLOB));

    if (blob == NULL) {
        perror("blob_create(): Could not malloc");
        return NULL;
    }

    if (content == NULL) {
        perror("Content cannot be null!");
        return NULL;
        blob->prefix = NULL;
    } else {
        blob->content = calloc(1, size + 1);
        if (blob->content == NULL) {
            free(blob);
            perror("blob_create(): Could not malloc");
            return NULL;        
        }

        memcpy(blob->content, content, size);

        blob->size = size; // TODO: make sure that the size is right

        blob->prefix = calloc(1, size + 1);
        if (blob->prefix == NULL) {
            free(blob);
            perror("blob_create(): Could not malloc");
            return NULL;        
        }

        memcpy(blob->prefix, content, size);

    }

    if (pthread_mutex_init(&blob->mutex, NULL) == -1) {
        free(blob);
        perror("blob_create(): Could not initalize the mutex");
        return NULL;
    }

    blob->refcnt = 1;
    return blob;
}

BLOB *blob_ref(BLOB *bp, char *why) {
    if (bp == NULL) {
        debug("Empty bp!");
        return NULL;
    }
    debug("blob ref: %s\n", why);
    pthread_mutex_lock(&bp->mutex);

    if (bp->prefix) {
        free(bp->prefix);
        bp->prefix = calloc(1, bp->size + 1);
        memcpy(bp->prefix, why, bp->size);
    }

    bp->refcnt++;

    pthread_mutex_unlock(&bp->mutex);

    return bp;
}

void blob_unref(BLOB *bp, char *why) {
    if (bp == NULL) {
        debug("BP is NULL!");
        return;
    }
    debug("blob unref: %s\n", why);
    pthread_mutex_lock(&bp->mutex);

    if (bp->prefix) {
        free(bp->prefix);
        bp->prefix = calloc(1, bp->size + 1);
        memcpy(bp->prefix, why, bp->size);
    }

    bp->refcnt--;

    if (bp->refcnt == 0) {
        free(bp->content);
        free(bp->prefix);

        pthread_mutex_unlock(&bp->mutex);
        pthread_mutex_destroy(&bp->mutex);

        free(bp);

        return;
    }

    pthread_mutex_unlock(&bp->mutex);
}

int blob_compare(BLOB *bp1, BLOB *bp2) {

    int res;

    pthread_mutex_lock(&bp1->mutex);
    pthread_mutex_lock(&bp2->mutex);

    res = strncmp(bp1->content, bp2->content, bp1->size);

    pthread_mutex_unlock(&bp1->mutex);
    pthread_mutex_unlock(&bp2->mutex);

    return res;

}

// TODO: Redo hash with a better algo
int blob_hash(BLOB *bp) {
    int i = 0;
    int hash = 0;

    pthread_mutex_lock(&bp->mutex);

    char* str = bp->content;

    while (*str != '\0') {
        int cur_ch = *str;
        hash += cur_ch + (i % 5 == 0 ? 256 : 128);
        i++;
        str = (char*)((void*)str + 1);
    }

    pthread_mutex_unlock(&bp->mutex);

    return hash;
}

KEY *key_create(BLOB *bp) {
    int hash = blob_hash(bp);

    pthread_mutex_lock(&bp->mutex);

    KEY *kp = (KEY*)calloc(1, sizeof(KEY));

    if (kp == NULL) {
        perror("key_create(): error callocing");
        pthread_mutex_unlock(&bp->mutex);
        return NULL;
    }

    kp->hash = hash;
    kp->blob = bp;

    pthread_mutex_unlock(&bp->mutex);

    bp = blob_ref(bp, "create key");

    return kp;
}

void key_dispose(KEY *kp) {
    BLOB* bp = kp->blob;
    kp->blob = NULL;

    blob_unref(bp, "dispose key");    
    free(kp);
}

int key_compare(KEY *kp1, KEY *kp2) {
    BLOB* bp1 = kp1->blob;
    BLOB* bp2 = kp2->blob;

    if (kp1->hash == kp2->hash && blob_compare(bp1, bp2) == 0) {
        return 0;
    }

    return -1;
}

VERSION *version_create(TRANSACTION *tp, BLOB *bp) {

    VERSION* version = (VERSION*) calloc(1, sizeof(VERSION));

    if (version == NULL) {
        perror("version_create(): error callocing");
        pthread_mutex_unlock(&bp->mutex);
        return NULL;        
    }

    version->blob = bp;
    blob_ref(bp, "create version");

    version->creator = tp;
    trans_ref(tp, "create version");

    version->prev = NULL;
    version->next = NULL;

    return version;
}

void version_dispose(VERSION *vp) {

    BLOB* bp = vp->blob;

    blob_unref(bp, "dispose version");

    if (dest_tr == 1) {
        TRANSACTION* tp = vp->creator;
        trans_unref(tp, "dispose version");
    }

    vp->blob = NULL;
    vp->creator = NULL;
    vp->prev = NULL;
    vp->next = NULL;

    free(vp);

    vp = NULL;
}
