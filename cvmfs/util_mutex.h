/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_MUTEX_H_
#define CVMFS_UTIL_MUTEX_H_

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
 * Implements a simple interface to lock objects of derived classes. Classes that
 * inherit from Lockable are also usable with the LockGuard template for scoped
 * locking semantics.
 *
 * Note: a Lockable object should not be copied!
 */
class Lockable : SingleCopy {
 public:
  inline virtual ~Lockable() {        pthread_mutex_destroy(&mutex_); }

  void Lock()    const       {        pthread_mutex_lock(&mutex_);    }
  int  TryLock() const       { return pthread_mutex_trylock(&mutex_); }
  void Unlock()  const       {        pthread_mutex_unlock(&mutex_);  }

 protected:
  Lockable() {
    const int retval = pthread_mutex_init(&mutex_, NULL);
    assert(retval == 0);
  }

 private:
  mutable pthread_mutex_t mutex_;
};

/**
 * Implements a simple interface to mutexes.
 *
 * Note: a Mutex object should not be copied!
 */

class MutexBase : SingleCopy {
 public:
  inline virtual __attribute__((used)) ~MutexBase() { }

  int Lock()    const       { if (mutex_) return pthread_mutex_lock(mutex_);    return (int)EINVAL; }
  int TryLock() const       { if (mutex_) return pthread_mutex_trylock(mutex_); return (int)EINVAL; }
  int Unlock()  const       { if (mutex_) return pthread_mutex_unlock(mutex_);  return (int)EINVAL; }

  MutexBase() : mutex_(NULL) { }

 protected:
  pthread_mutex_t *mutex_;
};


class Mutex : public MutexBase {
 public:
  inline virtual __attribute__((used)) ~Mutex() {        pthread_mutex_destroy(&rmutex_); }

  Mutex(bool recursive = false) {
    pthread_mutexattr_t mtxattr;
    int retval = pthread_mutexattr_init(&mtxattr);
    if (!retval && recursive) {
       retval |= pthread_mutexattr_settype(&mtxattr, PTHREAD_MUTEX_RECURSIVE);
       retval |= pthread_mutex_init(&rmutex_, &mtxattr);
    } else {
       retval = pthread_mutex_init(&rmutex_, NULL);
    }
    assert(retval == 0);
    mutex_ = &rmutex_;
  }

 private:
  mutable pthread_mutex_t rmutex_;
};

class SMutex : public MutexBase {
 public:
  inline virtual __attribute__((used)) ~SMutex() {        pthread_mutex_destroy(mutex_);
                                                          free(mutex_); mutex_ =  NULL; }

  SMutex(bool recursive = false) : MutexBase() {

    int retval = 0;
    if ((mutex_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t))))) {
       pthread_mutexattr_t mtxattr;
       retval |= pthread_mutexattr_init(&mtxattr);
       if (!retval && recursive) {
          retval |= pthread_mutexattr_settype(&mtxattr, PTHREAD_MUTEX_RECURSIVE);
          retval |= pthread_mutex_init(mutex_, &mtxattr);
       } else {
          retval = pthread_mutex_init(mutex_, NULL);
       }
    }
    assert(retval == 0);
  }
};

// Macros
void LockMutex(Mutex &mutex_);
void UnlockMutex(Mutex &mutex_);

// Arrays of mutexes


class MutexArrayBase : SingleCopy {

 public:
  inline virtual __attribute__((used)) ~MutexArrayBase() { }

  int  Lock(unsigned int i)    const       { if (i >= num) return (int)EINVAL; return pthread_mutex_lock(&mutex_[i]);    }
  int  TryLock(unsigned int i) const       { if (i >= num) return (int)EINVAL; return pthread_mutex_trylock(&mutex_[i]); }
  int  Unlock(unsigned int i)  const       { if (i >= num) return (int)EINVAL; return pthread_mutex_unlock(&mutex_[i]);  }

  MutexArrayBase(int nthreads, bool recursive = false) : num((unsigned int)nthreads), mutex_(NULL) {

    mutex_ = Allocate();

    int retval = 0;
    if (mutex_) {
       pthread_mutexattr_t mtxattr;
       retval |= pthread_mutexattr_init(&mtxattr);
       if (!retval && recursive) {
          retval |= pthread_mutexattr_settype(&mtxattr, PTHREAD_MUTEX_RECURSIVE);
          for (unsigned int i = 0; i < num && !retval; ++i) {
             retval |= pthread_mutex_init(&(mutex_[i]), &mtxattr);
          }
       } else {
          for (unsigned int i = 0; i < num && !retval; ++i) {
             retval = pthread_mutex_init(&(mutex_[i]), NULL);
          }
       }
    }
    assert(retval == 0);
 }


 protected:
  unsigned int  num;
  mutable pthread_mutex_t *mutex_;

  virtual pthread_mutex_t *Allocate() { return NULL; }
};


// This is for arrays of mutexex using smalloc

class MutexArray : public MutexArrayBase {

 public:
  inline virtual __attribute__((used)) ~MutexArray() {
     for (unsigned int i = 0; i < num; ++i) {
        pthread_mutex_destroy(&(mutex_[i]));
     }
     free(mutex_);
  }

  MutexArray(int nthreads, bool recursive = false) : MutexArrayBase(num, recursive) { }

 protected:
  pthread_mutex_t *Allocate() { return static_cast<pthread_mutex_t *>(malloc( num * sizeof(pthread_mutex_t))); }

};

// This is for arrays of mutexex using smalloc

class SMutexArray : public MutexArrayBase {

 public:
  inline virtual __attribute__((used)) ~SMutexArray() {
     for (unsigned int i = 0; i < num; ++i) {
        pthread_mutex_destroy(&(mutex_[i]));
     }
     free(mutex_);
  }

  SMutexArray(int nthreads, bool recursive = false) : MutexArrayBase(num, recursive) { }

 protected:
  pthread_mutex_t *Allocate() { return static_cast<pthread_mutex_t *>(smalloc( num * sizeof(pthread_mutex_t))); }

};

// This is for arrays of mutexex using the OpenSSL allocator

class CryptoMutexArray : public MutexArrayBase {

 public:
  inline virtual __attribute__((used)) ~CryptoMutexArray() {
     for (unsigned int i = 0; i < num; ++i) {
        pthread_mutex_destroy(&(mutex_[i]));
     }
     OPENSSL_free(mutex_);
  }

  CryptoMutexArray(int nthreads, bool recursive = false) : MutexArrayBase(num, recursive) { }

 protected:
  pthread_mutex_t *Allocate() { return static_cast<pthread_mutex_t *>(OPENSSL_malloc( num * sizeof(pthread_mutex_t))); }

};


/**
 * Implements a simple interface to RW locks.
 *
 * Note: a RW lock object should not be copied!
 */
class RWLock : SingleCopy {
 public:
  inline virtual __attribute__((used)) ~RWLock() { pthread_rwlock_destroy(&lock_); }

  void RLock()    const       {        pthread_rwlock_rdlock(&lock_); }
  int  TryRLock() const       { return pthread_rwlock_tryrdlock(&lock_); }

  void WLock()    const       {        pthread_rwlock_wrlock(&lock_); }
  int  TryWLock() const       { return pthread_rwlock_trywrlock(&lock_); }

  int  Unlock()   const       { return pthread_rwlock_unlock(&lock_);  }

  RWLock() {
    int retval = pthread_rwlock_init(&lock_, NULL);
    assert(retval == 0);
  }

 private:
  mutable pthread_rwlock_t lock_;
};


//
// -----------------------------------------------------------------------------
//

/**
 * Used to allow for static polymorphism in the RAII template to statically
 * decide which 'lock' functions to use, if we have more than one possiblity.
 * (I.e. Read/Write locks)
 * Note: Static Polymorphism - Strategy Pattern
 *
 * TODO: eventually replace this by C++11 typed enum
 */
struct _RAII_Polymorphism {
  enum T {
    None,
    ReadLock,
    WriteLock
  };
};


/**
 * Basic template wrapper class for any kind of RAII-like behavior.
 * The user is supposed to provide a template specialization of Enter() and
 * Leave(). On creation of the RAII object it will call Enter() respectively
 * Leave() on destruction. The gold standard example is a LockGard (see below).
 *
 * Note: Resource Acquisition Is Initialization (Bjarne Stroustrup)
 */
template <typename T, _RAII_Polymorphism::T P = _RAII_Polymorphism::None>
class RAII : SingleCopy {
 public:
  inline explicit RAII(T &object) : ref_(object)  { Enter(); }
  inline explicit RAII(T *object) : ref_(*object) { Enter(); }
  inline ~RAII()                         { Leave(); }

  inline void Leave() { ref_.Unlock(); }  // for interleaved scopes

 protected:
  inline void Enter() { ref_.Lock();   }

 private:
  T &ref_;
};


/**
 * This is a simple scoped lock implementation. Every object that provides the
 * methods Lock() and Unlock() should work with it. Classes that will be used
 * with this template should therefore simply inherit from Lockable.
 *
 * Creating a LockGuard object on the stack will lock the provided object. When
 * the LockGuard runs out of scope it will automatically release the lock. This
 * ensures a clean unlock in a lot of situations!
 *
 * TODO: C++11 replace this by a type alias to RAII
 */
template <typename LockableT>
class LockGuard : public RAII<LockableT> {
 public:
  inline explicit LockGuard(LockableT *object) : RAII<LockableT>(object) {}
};

#if 0
template <>
inline void RAII<Mutex>::Enter() { ref_.Lock();   }
template <>
inline void RAII<Mutex>::Leave() { ref_.Unlock(); }
typedef RAII<Mutex> MutexLockGuard;
#else
template <>
inline void RAII<MutexBase>::Enter() { ref_.Lock();   }
template <>
inline void RAII<MutexBase>::Leave() { ref_.Unlock(); }
typedef RAII<MutexBase> MutexLockGuard;
#endif

template <>
inline void RAII<RWLock, _RAII_Polymorphism::ReadLock>::Enter() { ref_.RLock(); }
template <>
inline void RAII<RWLock, _RAII_Polymorphism::ReadLock>::Leave() { ref_.Unlock(); }
template <>
inline void RAII<RWLock, _RAII_Polymorphism::WriteLock>::Enter() { ref_.WLock(); }
template <>
inline void RAII<RWLock, _RAII_Polymorphism::WriteLock>::Leave() { ref_.Unlock(); }
typedef RAII<RWLock, _RAII_Polymorphism::ReadLock>  ReadLockGuard;
typedef RAII<RWLock, _RAII_Polymorphism::WriteLock> WriteLockGuard;


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_MUTEX_H_
