/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "util_concurrency.h"

#include <unistd.h>

#include <cassert>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

unsigned int GetNumberOfCpuCores() {
  const int numCPU = sysconf(_SC_NPROCESSORS_ONLN);

  if (numCPU <= 0) {
    LogCvmfs(kLogSpooler, kLogWarning, "Unable to determine the available "
                                       "number of processors in the system... "
                                       "falling back to default '%d'",
             kFallbackNumberOfCpus);
    return kFallbackNumberOfCpus;
  }

  return static_cast<unsigned int>(numCPU);
}

Signal::Signal() : fired_(false) {
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
  retval = pthread_cond_init(&signal_, NULL);
  assert(retval == 0);
}


Signal::~Signal() {
  pthread_cond_destroy(&signal_);
  pthread_mutex_destroy(&lock_);
}


void Signal::Wait() {
  RAII<pthread_mutex_t> guard(lock_);
  while (!fired_) {
    int retval = pthread_cond_wait(&signal_, &lock_);
    assert(retval == 0);
  }
}


void Signal::Wakeup() {
  RAII<pthread_mutex_t> guard(lock_);
  fired_ = true;
  int retval = pthread_cond_broadcast(&signal_);
  assert(retval == 0);
}

//
// +----------------------------------------------------------------------------
// |  Condition
//

int Condition::Wait(int64_t msec) const
{
  timespec timeout;
  timeval now;
  if (ownmtx_) Lock();
  gettimeofday(&now, NULL);
  int64_t nsecs = now.tv_usec * 1000 + (msec % 1000)*1000*1000;
  int carry = 0;
  if (nsecs >= 1000*1000*1000) {
     carry = 1;
     nsecs -= 1000*1000*1000;
  }
  timeout.tv_sec = now.tv_sec + msec/1000 + carry;
  timeout.tv_nsec = nsecs;

  // Now wait for the condition or timeout
  int rc = 0;
  do {
    rc = pthread_cond_timedwait(&cond_, &mutex_, &timeout);
  } while (rc && (rc == EINTR));

  if (ownmtx_) Unlock();

  assert(rc == 0 || rc == ETIMEDOUT);

  return rc;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
