#include "transaction.h"
#include "debug.h"
#include "vars.h"

#include <stdlib.h>
#include <stdio.h>

int t_ctr;

void trans_init(void) {
    t_ctr = 0;
    trans_list.id = t_ctr;
    trans_list.depends = NULL;

    trans_list.next = &trans_list;
    trans_list.prev = &trans_list;
}

TRANSACTION *trans_create(void) {
    t_ctr++;
    TRANSACTION* tp = calloc(1, sizeof(TRANSACTION));

    if (tp == NULL) {
        perror("could calloc transaction");
        return NULL;
    }

    tp->id = t_ctr;
    tp->refcnt = 1;
    tp->status = TRANS_PENDING;
    tp->depends = NULL;
    tp->waitcnt = 0;

    TRANSACTION* prev_tr = trans_list.prev;

    prev_tr->next = tp;
    tp->next = &trans_list;

    tp->prev = prev_tr;
    trans_list.prev = tp;    
    
    if (sem_init(&tp->sem, 0, 0) == -1) {
        perror("Could init sem");
        return NULL;
    }

    if (pthread_mutex_init(&tp->mutex, NULL) == -1) {
        perror("Could init sem");
        return NULL;
    }

    return tp;
}

TRANSACTION *trans_ref(TRANSACTION *tp, char *why) {
    pthread_mutex_lock(&tp->mutex);

    tp->refcnt++;

    debug("trans ref why: %s", why);

    pthread_mutex_unlock(&tp->mutex);

    return tp;
}


void freeTrans(TRANSACTION* tp) {
    if (tp->id == -1) {
        return;
    }

    tp->id = -1;
    // free the dependencies
    DEPENDENCY* dp = tp->depends;

    while (dp != NULL) {
        DEPENDENCY* dp2 = dp->next;

        trans_unref(dp->trans, "destory dependency");

        dp->trans = NULL;
        dp->next = NULL;

        free(dp);
        dp = dp2;
    }

    TRANSACTION* ptp = tp->prev;
    TRANSACTION* ntp = tp->next;

    ptp->next = ntp;
    ntp->prev = ptp;

    tp->prev = NULL;
    tp->next = NULL;

    tp->refcnt = 0;

    sem_destroy(&tp->sem);

    pthread_mutex_unlock(&tp->mutex);
    pthread_mutex_destroy(&tp->mutex);        

    free(tp);

    tp = NULL;

}

void trans_fini(void) {

    TRANSACTION* tlp = &trans_list;
    tlp = tlp->next;

    while (tlp != &trans_list) {
        TRANSACTION* ptp = tlp;
        tlp = tlp->next;
        freeTrans(ptp);
    }      
    // freeTrans(tlp, 1);

    dest_tr = 0;

}

void trans_unref(TRANSACTION *tp, char *why) {
    pthread_mutex_lock(&tp->mutex);

    debug("trans unref: %s\n", why);

    if (tp->refcnt == 0) {
        pthread_mutex_unlock(&tp->mutex);
        return;
    }

    tp->refcnt--;

    if (tp->refcnt == 0) {        
        freeTrans(tp);
        return;
    }

    pthread_mutex_unlock(&tp->mutex);
}


void trans_add_dependency(TRANSACTION *tp, TRANSACTION *dtp) {
    DEPENDENCY* dp = calloc(1, sizeof(DEPENDENCY));
    if (dp == NULL) {
        perror("error callocing dependency");
        return;
    }
    dp->next = NULL;
    dp->trans = dtp;

    DEPENDENCY* tdp = tp->depends;

    if (tdp == NULL) {
        tdp = dp;
    } else {
        while (tdp->next != NULL) {
            if (tdp->trans == dtp) {
                return;
            }
            tdp = tdp->next;
        }
        
        tp->depends->next = dp;
    }

    pthread_mutex_lock(&dtp->mutex);
    dtp->waitcnt++;
    pthread_mutex_unlock(&dtp->mutex);

    trans_ref(dtp, "add dependency");
}

TRANS_STATUS trans_commit(TRANSACTION *tp) {
    pthread_mutex_lock(&tp->mutex);

    debug("Trans commit called\n");

    DEPENDENCY* dp = tp->depends;

    while (dp != NULL) {
        if (sem_wait(&dp->trans->sem) == -1) {
            perror("could not sem wait");
            pthread_mutex_unlock(&tp->mutex);
            debug("Could not sem wait\n");  
            return TRANS_ABORTED;
        }
        if (dp->trans->status == TRANS_ABORTED) {
            pthread_mutex_unlock(&tp->mutex);
            trans_abort(tp);
            debug("commit trans aborted\n");
            return TRANS_ABORTED;
        }
        debug("Next dependency\n");
        dp = dp->next;
    }


    for (int i = 0; i < tp->waitcnt; i++) {
        sem_post(&tp->sem);
    }

    tp->status = TRANS_COMMITTED;
    dp = tp->depends;

    while (dp != NULL) {
        DEPENDENCY* next_dp = dp->next;
        trans_unref(dp->trans, "commit dependency");
        dp->trans = NULL;
        dp = next_dp;
    }

    pthread_mutex_unlock(&tp->mutex);

    trans_unref(tp, "commit current transaction");

    return TRANS_COMMITTED;
}

TRANS_STATUS trans_abort(TRANSACTION *tp) {
    pthread_mutex_lock(&tp->mutex);

    if (tp->status == TRANS_ABORTED) {
        pthread_mutex_unlock(&tp->mutex);
        return TRANS_ABORTED;
    } else if (tp->status == TRANS_COMMITTED) {
        perror("fatal error transaction already comitted");
        abort();
    }

    tp->status = TRANS_ABORTED;
    DEPENDENCY* dp = tp->depends;

    while (dp != NULL) {
        DEPENDENCY* next_dp = dp->next;
        trans_unref(dp->trans, "abort dependency");
        dp->trans = NULL;
        dp = next_dp;
    }    

    pthread_mutex_unlock(&tp->mutex);

    trans_unref(tp, "abort current transaction");

    return TRANS_ABORTED;

}

TRANS_STATUS trans_get_status(TRANSACTION *tp) {
    TRANS_STATUS status;

    pthread_mutex_lock(&tp->mutex);
    
    status = tp->status;

    pthread_mutex_unlock(&tp->mutex);

    return status;
}

void trans_show(TRANSACTION *tp) {
    debug("ID: %d\n", tp->id);
    debug("refcnt: %d\n", tp->refcnt);
    debug("status: %d\n", tp->status);
    debug("waitcnt: %d\n", tp->waitcnt);
    debug("next: %p\n", tp->next);
    debug("prev: %p\n", tp->prev);
    debug("dp: %p\n", tp->depends);
}

void trans_show_all(void) {
    TRANSACTION* tlptr = &trans_list;
    tlptr = tlptr->next;
    while(tlptr->next != &trans_list) {
        trans_show(tlptr);
        tlptr = tlptr->next;
    }
}