#include "store.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>

struct map* mp;

void store_init(void) {
    mp = calloc(1, sizeof(struct map));
    mp->table = calloc(NUM_BUCKETS, sizeof(MAP_ENTRY));
    mp->num_buckets = NUM_BUCKETS;
    pthread_mutex_init(&mp->mutex, NULL);
}

void garbage_collect(MAP_ENTRY* mpe) {
    VERSION* versions = mpe->versions;
    VERSION* vptr;

    if (versions == NULL) return;

    while (versions != NULL) {
        TRANSACTION* tp = versions->creator;
        int vpid;

        pthread_mutex_lock(&tp->mutex);
        vpid = tp->id;
        pthread_mutex_unlock(&tp->mutex);        

        if (vpid != -1 && trans_get_status(tp) == TRANS_ABORTED) {
            vptr = versions;
            if (versions != NULL) {
                vptr = versions->prev;
            }
            while (versions != NULL) {
                VERSION* next_v = versions->next;
                trans_ref(versions->creator, "ref v disp");
                version_dispose(versions);
                versions = next_v;
            }
            if (vptr)
                vptr->next = NULL;
            mpe->versions = vptr;
            break;
        }
        versions = versions->next;
    }

    if (mpe->versions == NULL) {
        return;
    }

    VERSION* recent = mpe->versions;
    versions = mpe->versions;
    int found = 0;

    int rid;

    pthread_mutex_lock(&recent->creator->mutex);
    rid = recent->creator->id;
    pthread_mutex_unlock(&recent->creator->mutex);      

    while (versions != NULL) {

        int vpid;

        pthread_mutex_lock(&versions->creator->mutex);
        vpid = versions->creator->id;
        pthread_mutex_unlock(&versions->creator->mutex);  


        if (vpid != -1 && trans_get_status(versions->creator) == TRANS_COMMITTED) {
            if (rid <= vpid) {
                recent = &(*versions);
                found = 1;
            }
        }
        versions = versions->next;
    }

    if (!found) return;

    versions = mpe->versions;

    while (versions->creator != recent->creator) {
        VERSION* next_v = versions->next;
        trans_ref(versions->creator, "ref v disp");
        version_dispose(versions);
        versions = next_v;
    }

    mpe->versions = NULL;
    mpe->versions = recent;
}

TRANS_STATUS store_put(TRANSACTION *tp, KEY *key, BLOB *value) {
    pthread_mutex_lock(&mp->mutex);

    int hash = key->hash;
    int idx = hash % NUM_BUCKETS;

    MAP_ENTRY* mpe = mp->table[idx];

    pthread_mutex_lock(&tp->mutex);

    int tid = tp->id;

    pthread_mutex_unlock(&tp->mutex);


    while (mpe != NULL) {
        if (key_compare(key, mpe->key) == 0) {
            debug("Collecting new garbage\n");

            garbage_collect(mpe);

            debug("Garbage collected\n");

            if (mpe->versions == NULL) {
                VERSION* new_v = version_create(tp, value);
                new_v->next = NULL;
                // blob_unref(value, "just passing down value");
                // trans_unref(tp, "just passing down transaction");
                debug("versions is null so create a new version\n");
                mpe->versions = new_v;
            } else {
                VERSION* vptr = mpe->versions;

                debug("found version\n");

                while (vptr->next != NULL) {
                    vptr = vptr->next;
                }

                if (vptr != NULL) {
                    int vpid;

                    pthread_mutex_lock(&vptr->creator->mutex);
                    vpid = vptr->creator->id;
                    pthread_mutex_unlock(&vptr->creator->mutex);

                    if (vpid== tid) {
                        vptr->blob = value;
                        blob_ref(value, "inc refcnt so net 0");
                        debug("replace value\n");

                    } else if (tid > vpid) {
                        while (vptr->next != NULL) {
                            vptr = vptr->next;
                        }

                        VERSION* new_v = version_create(tp, value);
                        vptr->next = new_v;
                        new_v->prev = vptr;
                        new_v->next = NULL;

                        debug("Create new transaction\n");

                        // blob_unref(value, "just passing down value");
                        // trans_unref(tp, "just passing down transaction");

                    } else {
                        debug("Abort new transaction\n");
                        trans_ref(tp, "trans aborted");
                        trans_abort(tp);
                        pthread_mutex_unlock(&mp->mutex);
                        return TRANS_ABORTED;
                    }

                    vptr = mpe->versions;
                    while (vptr->next != NULL) {
                        if (trans_get_status(vptr->creator) == TRANS_PENDING) {
                            // if (vptr->creator != tp)
                            // trans_add_dependency(vptr->creator, tp);
                            trans_add_dependency(tp, vptr->creator);
                        }
                        vptr = vptr->next;
                    }
                }
            }

            mpe->key = key;
            pthread_mutex_unlock(&mp->mutex);
            return trans_get_status(tp);
        }
        mpe = mpe->next;
    }

    VERSION* new_v = version_create(tp, value);
    MAP_ENTRY* n_mpe = calloc(1, sizeof(MAP_ENTRY));

    // blob_unref(value, "just passing down value");
    trans_unref(tp, "just passing down transaction");


    n_mpe->key = key;
    n_mpe->versions = new_v;
    n_mpe->next = NULL;

    mp->table[idx] = n_mpe;

    pthread_mutex_unlock(&mp->mutex);

    TRANS_STATUS status = trans_get_status(tp);
    // trans_unref(tp, "final");

    return status;
}

TRANS_STATUS store_get(TRANSACTION *tp, KEY *key, BLOB **valuep) {
    pthread_mutex_lock(&mp->mutex);

    int hash = key->hash;
    int idx = hash % NUM_BUCKETS;

    MAP_ENTRY* mpe = mp->table[idx];

    pthread_mutex_lock(&tp->mutex);

    int tid = tp->id;

    pthread_mutex_unlock(&tp->mutex);    

    while (mpe != NULL) {
        if (key_compare(key, mpe->key) == 0) {
            debug("Before garbage collect new transaction\n");

            garbage_collect(mpe);

            debug("After garbage collect new transaction\n");


            VERSION* vptr = mpe->versions;

            if (vptr == NULL) {
                break;
            }

            while (vptr->next != NULL && vptr->creator->id < tid) {
                vptr = vptr->next;
            }

            if (vptr != NULL) {
                int vpid;

                pthread_mutex_lock(&vptr->creator->mutex);
                vpid = vptr->creator->id;
                pthread_mutex_unlock(&vptr->creator->mutex);

                debug("VPTR CREATOR ID: %d ; Transaction ID: %d", vpid, tid);

                if (vpid == tid) {
                    *valuep = vptr->blob;
                    // blob_ref(*valuep, "inc get net 0");
                } else if (tid > vpid) {
                    while (vptr->next != NULL) {
                        vptr = vptr->next;
                    }

                    *valuep = vptr->blob;
                    VERSION* new_v = version_create(tp, *valuep);
                    vptr->next = new_v;
                    new_v->prev = vptr;
                    new_v->next = NULL;

                    debug("Create new transaction\n");

                    // blob_unref(*valuep, "just passing down value");

                    // blob_ref(*valuep, "get valuep");
                    // trans_ref(tp, "store ref");

                } else {
                    debug("Abort new transaction\n");
                    trans_ref(tp, "trans aborted");
                    trans_abort(tp);
                    pthread_mutex_unlock(&mp->mutex);
                    return TRANS_ABORTED;
                }

                vptr = mpe->versions;
                while (vptr->next != NULL) {
                    if (trans_get_status(vptr->creator) == TRANS_PENDING) {
                        // if (vptr->creator != tp)
                        // trans_add_dependency(vptr->creator, tp);
                        trans_add_dependency(tp, vptr->creator);
                    }
                    vptr = vptr->next;
                }
            }

            // trans_unref(tp, "final");
            pthread_mutex_unlock(&mp->mutex);
            return trans_get_status(tp);
        }
        mpe = mpe->next;
    }

    pthread_mutex_unlock(&mp->mutex);

    TRANS_STATUS status = trans_get_status(tp);
    // trans_unref(tp, "final");

    return status;    
}

void store_show(void) {
    for (int i = 0; i < NUM_BUCKETS; i++) {
        MAP_ENTRY* table = mp->table[i];

        while (table) {
            debug("key: %s\n", table->key->blob->prefix);
            
            VERSION* vptr = table->versions;
            while (vptr) {
                debug("Version blob: %s", vptr->blob->prefix);
                trans_show(vptr->creator);
                vptr = vptr->next;
            }
            table = table->next;
        }
    }
}

void store_fini(void) {
    for (int i = 0; i < NUM_BUCKETS; i++) {
        MAP_ENTRY* table = mp->table[i];

        while (table) {
            MAP_ENTRY* next_e = table->next;
            KEY* key = table->key;
            key_dispose(key);
            
            VERSION* vptr = table->versions;
            while (vptr) {
                VERSION* next = vptr->next;
                version_dispose(vptr);
                vptr = next;
            }
            free(table);
            table = next_e;
        }
    }

    free(mp->table);

    pthread_mutex_unlock(&mp->mutex);
    pthread_mutex_destroy(&mp->mutex);

    free(mp);
}