/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_CONDITION_H_
#define CVMFS_UTIL_CONDITION_H_

#include <pthread.h>

#include <cassert>
#include <errno.h>
#include <queue>
#include <set>
#include <sys/time.h>
#include <vector>
#include <openssl/crypto.h>

#include "atomic.h"
#include "smalloc.h"
#include "util/async.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Implements a wrapper class around a condition variable
 *
 * Note: a Mutex object should not be copied!
 */
class Condition : SingleCopy {
 public:
  inline virtual ~Condition() { pthread_mutex_destroy(&mutex_);
                                pthread_cond_destroy(&cond_); }

  void Lock()    const       { pthread_mutex_lock(&mutex_);    }
  void Unlock()  const       { pthread_mutex_unlock(&mutex_);  }

  int Broadcast() const      { if (ownmtx_) pthread_mutex_lock(&mutex_);
                               int rc = pthread_cond_broadcast(&cond_);
                               if (ownmtx_) pthread_mutex_unlock(&mutex_);
                               return rc;
                             }

  int Signal() const         { if (ownmtx_) pthread_mutex_lock(&mutex_);
                               int rc = pthread_cond_signal(&cond_);
                               if (ownmtx_) pthread_mutex_unlock(&mutex_);
                               return rc;
                             }
  
  int Wait() const           { if (ownmtx_) Lock();
                               int rc = pthread_cond_wait(&cond_, &mutex_);
                               if (ownmtx_) Unlock();
                               return rc;
                            }
  int Wait(int64_t ms) const;

  Condition(bool ownmtx = true) : ownmtx_(ownmtx) {
    int retval = pthread_cond_init(&cond_, NULL);
    retval |= pthread_mutex_init(&mutex_, NULL);
    assert(retval == 0);
  }

 private:
  bool                    ownmtx_;
  mutable pthread_mutex_t mutex_;
  mutable pthread_cond_t  cond_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_CONDITION_H_
