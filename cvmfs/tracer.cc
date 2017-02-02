/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "tracer.h"

#include <pthread.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "atomic.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT


void Tracer::Activate(
  const int buffer_size,
  const int flush_threshold,
  const string &trace_file)
{
  trace_file_ = trace_file;
  buffer_size_ = buffer_size;
  flush_threshold_ = flush_threshold;
  assert(buffer_size_ > 1);
  assert(0 <= flush_threshold_ && flush_threshold_ < buffer_size_);

  ring_buffer_ = new BufferEntry[buffer_size_];
  commit_buffer_ = new atomic_int32[buffer_size_];
  for (int i = 0; i < buffer_size_; i++)
    atomic_init32(&commit_buffer_[i]);

  active_ = true;
}


/**
 * Trace a message.  This is usually a lock-free procedure that just
 * requires two fetch_and_add operations and a gettimeofday syscall.
 * There are two exceptions:
 *   -# If the ring buffer is full, the function blocks until the flush
 *      thread made some space.  Avoid that by carefully choosing size
 *      and threshold.
 *   -# If this message reaches the threshold, the flush thread gets
 *      signaled.
 *
 * \param[in] event Arbitrary code, for consistency applications should use one
 *            of the TraceEvents constants. Negative codes are reserved
 *            for internal use.
 * \param[in] id Arbitrary id, for example file name or module name which is
 *            doing the trace.
 * \return The sequence number which was used to trace the record
 */
int32_t Tracer::DoTrace(
  const int event,
  const PathString &path,
  const string &msg)
{
  int32_t my_seq_no = atomic_xadd32(&seq_no_, 1);
  timeval now;
  gettimeofday(&now, NULL);
  int pos = my_seq_no % buffer_size_;

  while (my_seq_no - atomic_read32(&flushed_) >= buffer_size_) {
    int retval = sig_continue_trace_.Wait(25);
    assert(retval == ETIMEDOUT || retval == 0);
  }

  ring_buffer_[pos].time_stamp = now;
  ring_buffer_[pos].code = event;
  ring_buffer_[pos].path = path;
  ring_buffer_[pos].msg = msg;
  atomic_inc32(&commit_buffer_[pos]);

  if (my_seq_no - atomic_read32(&flushed_) == flush_threshold_) {
    int err_code = sig_flush_.Signal();
    assert(err_code == 0 && "Could not signal flush thread");
  }

  return my_seq_no;
}


void Tracer::Flush() {
  if (!active_) return;

  int32_t save_seq_no = DoTrace(kEventFlush, PathString("Tracer", 6),
                                "flushed ring buffer");
  while (atomic_read32(&flushed_) <= save_seq_no) {
    int retval;

    atomic_cas32(&flush_immediately_, 0, 1);
    retval = sig_flush_.Signal();
    assert(retval == 0);
    retval = sig_continue_trace_.Wait(250);
    assert(retval == ETIMEDOUT || retval == 0);
  }
}

void *Tracer::MainFlush(void *data) {
  Tracer *tracer = reinterpret_cast<Tracer *>(data);
  int retval;
  tracer->sig_flush_.Lock();
  FILE *f = fopen(tracer->trace_file_.c_str(), "a");
  assert(f != NULL && "Could not open trace file");

  do {
    while ((atomic_read32(&tracer->terminate_flush_thread_) == 0) &&
           (atomic_read32(&tracer->flush_immediately_) == 0) &&
           (atomic_read32(&tracer->seq_no_) -
              atomic_read32(&tracer->flushed_)
              <= tracer->flush_threshold_))
    {
      retval = tracer->sig_flush_.Wait(2000);
      assert(retval != EINVAL);
    }

    int base = atomic_read32(&tracer->flushed_) % tracer->buffer_size_;
    int pos, i = 0;
    while ((i <= tracer->flush_threshold_) &&
           (atomic_read32(&tracer->commit_buffer_[
             pos = ((base + i) % tracer->buffer_size_)]) == 1))
    {
      string tmp;
      tmp = StringifyTimeval(tracer->ring_buffer_[pos].time_stamp);
      retval = tracer->WriteCsvFile(f, tmp);
      retval |= fputc(',', f) - ',';
      tmp = StringifyInt(tracer->ring_buffer_[pos].code);
      retval = tracer->WriteCsvFile(f, tmp);
      retval |= fputc(',', f) - ',';
      retval |= tracer->WriteCsvFile(
        f, tracer->ring_buffer_[pos].path.ToString());
      retval |= fputc(',', f) - ',';
      retval |= tracer->WriteCsvFile(f, tracer->ring_buffer_[pos].msg);
      retval |= (fputc(13, f) - 13) | (fputc(10, f) - 10);
      assert(retval == 0);

      atomic_dec32(&tracer->commit_buffer_[pos]);
      ++i;
    }
    retval = fflush(f);
    assert(retval == 0);
    atomic_xadd32(&tracer->flushed_, i);
    atomic_cas32(&tracer->flush_immediately_, 1, 0);

    retval = tracer->sig_continue_trace_.Broadcast();
  } while ((atomic_read32(&tracer->terminate_flush_thread_) == 0) ||
           (atomic_read32(&tracer->flushed_) <
             atomic_read32(&tracer->seq_no_)));

  tracer->sig_flush_.Unlock();
  retval = fclose(f);
  assert(retval == 0);
  return NULL;
}


void Tracer::Spawn() {
  if (active_) {
    int retval = pthread_create(&thread_flush_, NULL, MainFlush, this);
    assert(retval == 0);

    spawned_ = true;
    DoTrace(kEventStart, PathString("Tracer", 6), "Trace buffer created");
  }
}


Tracer::Tracer()
  : active_(false)
  , spawned_(false)
  , buffer_size_(0)
  , flush_threshold_(0)
  , ring_buffer_(NULL)
  , commit_buffer_(NULL)
  , sig_flush_(true)
  , sig_continue_trace_(true)
{
  atomic_init32(&seq_no_);
  atomic_init32(&flushed_);
  atomic_init32(&terminate_flush_thread_);
  atomic_init32(&flush_immediately_);
}


Tracer::~Tracer() {
  if (!active_)
    return;
  int retval;

  if (spawned_) {
    DoTrace(kEventStop, PathString("Tracer", 6), "Destroying trace buffer...");

    // Trigger flushing and wait for it
    atomic_inc32(&terminate_flush_thread_);
    retval = sig_flush_.Signal();
    assert(retval == 0);
    retval = pthread_join(thread_flush_, NULL);
    assert(retval == 0);
  }

  delete[] ring_buffer_;
  delete[] commit_buffer_;
}


int Tracer::WriteCsvFile(FILE *fp, const string &field) {
  if (fp == NULL)
    return 0;

  int retval;

  if ((retval = fputc('"', fp)) != '"')
    return retval;

  for (unsigned i = 0, l = field.length(); i < l; ++i) {
    if (field[i] == '"') {
      if ((retval = fputc('"', fp)) != '"')
        return retval;
    }
    if ((retval = fputc(field[i], fp)) != field[i])
      return retval;
  }

  if ((retval = fputc('"', fp)) != '"')
    return retval;

  return 0;
}
