/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "util_mutex.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

void LockMutex(Mutex &mutex_) {
  int retval = mutex_.Lock();
  assert(retval == 0);
}


void UnlockMutex(Mutex &mutex_) {
  int retval = mutex_.Unlock();
  assert(retval == 0);
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
