/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "util_condition.h"

#include <unistd.h>

#include <cassert>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


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
