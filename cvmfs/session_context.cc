/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

#include <algorithm>

#include "curl/curl.h"
#include "cvmfs_config.h"
#include "gateway_util.h"
#include "util/string.h"

namespace {
size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp) {
  std::string* my_buffer = static_cast<std::string*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  *my_buffer = static_cast<char*>(buffer);

  return my_buffer->size();
}
}  // namespace

namespace upload {

SessionContextBase::SessionContextBase()
    : upload_results_(1000, 1000),
      api_url_(),
      session_token_(),
      key_id_(),
      secret_(),
      drop_lease_(true),
      queue_was_flushed_(1, 1),
      max_pack_size_(ObjectPack::kDefaultLimit),
      active_handles_(),
      current_pack_(NULL),
      current_pack_mtx_() {}

SessionContextBase::~SessionContextBase() {}

bool SessionContextBase::Initialize(const std::string& api_url,
                                    const std::string& session_token,
                                    const std::string& key_id,
                                    const std::string& secret, bool drop_lease,
                                    uint64_t max_pack_size) {
  bool ret = true;

  // Initialize session context lock
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr) ||
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) ||
      pthread_mutex_init(&current_pack_mtx_, &attr) ||
      pthread_mutexattr_destroy(&attr)) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Could not initialize SessionContext lock.");
    return false;
  }

  // Set upstream URL and session token
  api_url_ = api_url;
  session_token_ = session_token;
  key_id_ = key_id;
  secret_ = secret;
  drop_lease_ = drop_lease;
  max_pack_size_ = max_pack_size;

  atomic_init64(&objects_dispatched_);
  bytes_committed_ = 0u;
  bytes_dispatched_ = 0u;

  // Ensure that the upload job and result queues are empty
  upload_results_.Drop();

  queue_was_flushed_.Drop();
  queue_was_flushed_.Enqueue(true);

  // Ensure that there are not open object packs
  if (current_pack_) {
    LogCvmfs(
        kLogUploadGateway, kLogStderr,
        "Could not initialize SessionContext - Existing open object packs.");
    ret = false;
  }

  ret = InitializeDerived() && ret;

  return ret;
}

bool SessionContextBase::Finalize() {
  assert(active_handles_.empty());
  {
    MutexLockGuard lock(current_pack_mtx_);

    if (current_pack_ && current_pack_->GetNoObjects() > 0) {
      Dispatch();
      current_pack_ = NULL;
    }
  }

  bool results = true;
  int64_t jobs_finished = 0;
  while (!upload_results_.IsEmpty() || (jobs_finished < NumJobsSubmitted())) {
    Future<bool>* future = upload_results_.Dequeue();
    results = future->Get() && results;
    delete future;
    jobs_finished++;
  }

  if (drop_lease_ && !DropLease()) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "SessionContext finalization - Could not drop active lease");
  }

  results =
      FinalizeDerived() && (bytes_committed_ == bytes_dispatched_) && results;

  pthread_mutex_destroy(&current_pack_mtx_);
  return results;
}

void SessionContextBase::WaitForUpload() {
  if (!upload_results_.IsEmpty()) {
    queue_was_flushed_.Dequeue();
  }
}

ObjectPack::BucketHandle SessionContextBase::NewBucket() {
  MutexLockGuard lock(current_pack_mtx_);
  if (!current_pack_) {
    current_pack_ = new ObjectPack(max_pack_size_);
  }
  ObjectPack::BucketHandle hd = current_pack_->NewBucket();
  active_handles_.push_back(hd);
  return hd;
}

bool SessionContextBase::CommitBucket(const ObjectPack::BucketContentType type,
                                      const shash::Any& id,
                                      const ObjectPack::BucketHandle handle,
                                      const std::string& name,
                                      const bool force_dispatch) {
  MutexLockGuard lock(current_pack_mtx_);

  if (!current_pack_) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Error: Called SessionBaseContext::CommitBucket without an open "
             "ObjectPack.");
    return false;
  }

  uint64_t size0 = current_pack_->size();
  bool committed = current_pack_->CommitBucket(type, id, handle, name);

  if (committed) {  // Current pack is still not full
    active_handles_.erase(
        std::remove(active_handles_.begin(), active_handles_.end(), handle),
        active_handles_.end());
    uint64_t size1 = current_pack_->size();
    bytes_committed_ += size1 - size0;
    if (force_dispatch) {
      Dispatch();
      current_pack_ = NULL;
    }
  } else {  // Current pack is full and can be dispatched
    ObjectPack* new_pack = new ObjectPack(max_pack_size_);
    for (size_t i = 0u; i < active_handles_.size(); ++i) {
      current_pack_->TransferBucket(active_handles_[i], new_pack);
    }

    Dispatch();
    current_pack_ = new_pack;

    CommitBucket(type, id, handle, name, false);
  }

  return true;
}

int64_t SessionContextBase::NumJobsSubmitted() const {
  return atomic_read64(&objects_dispatched_);
}

void SessionContextBase::Dispatch() {
  MutexLockGuard lock(current_pack_mtx_);

  if (!current_pack_) {
    return;
  }

  atomic_inc64(&objects_dispatched_);
  bytes_dispatched_ += current_pack_->size();
  upload_results_.Enqueue(DispatchObjectPack(current_pack_));
}

SessionContext::SessionContext()
    : SessionContextBase(), upload_jobs_(1000, 900) {}

bool SessionContext::InitializeDerived() {
  // Start worker thread
  atomic_init32(&worker_terminate_);
  atomic_write32(&worker_terminate_, 0);

  upload_jobs_.Drop();

  int retval =
      pthread_create(&worker_, NULL, UploadLoop, reinterpret_cast<void*>(this));

  return !retval;
}

bool SessionContext::FinalizeDerived() {
  atomic_write32(&worker_terminate_, 1);

  pthread_join(worker_, NULL);

  return true;
}

bool SessionContext::DropLease() { return true; }

Future<bool>* SessionContext::DispatchObjectPack(ObjectPack* pack) {
  UploadJob* job = new UploadJob;
  job->pack = pack;
  job->result = new Future<bool>();
  upload_jobs_.Enqueue(job);
  return job->result;
}

bool SessionContext::DoUpload(const SessionContext::UploadJob* job) {
  // Set up the object pack serializer
  ObjectPackProducer serializer(job->pack);

  shash::Any payload_digest(shash::kSha1);
  serializer.GetDigest(&payload_digest);
  const std::string json_body =
      "{\"session_token\" : \"" + session_token_ +
      "\", \"payload_digest\" : \"" + Base64(payload_digest.ToString(false)) +
      "\", \"api_version\" : \"" + StringifyInt(gateway::APIVersion()) + "\"}";

  // Compute HMAC
  shash::Any hmac(shash::kSha1);
  shash::HmacString(secret_, json_body, &hmac);

  // TODO(radu): The copying is inefficient; Use CURLOPT_READFUNCTION
  std::vector<unsigned char> payload(0);
  std::vector<unsigned char> buffer(4096);
  unsigned nbytes = 0;
  do {
    nbytes = serializer.ProduceNext(buffer.size(), &buffer[0]);
    std::copy(buffer.begin(), buffer.begin() + nbytes,
              std::back_inserter(payload));
  } while (nbytes > 0);
  const std::string payload_text =
      json_body +
      Base64(std::string(reinterpret_cast<char*>(&payload[0]), payload.size()));

  // Prepare the Curl POST request
  CURL* h_curl = curl_easy_init();

  if (!h_curl) {
    return false;
  }

  // Set HTTP headers (Authorization and Message-Size)
  std::string header_str = std::string("Authorization: ") + key_id_ + " " +
                           Base64(hmac.ToString(false));
  struct curl_slist* auth_header = NULL;
  auth_header = curl_slist_append(auth_header, header_str.c_str());
  header_str = std::string("Message-Size: ") + StringifyInt(json_body.size());
  auth_header = curl_slist_append(auth_header, header_str.c_str());
  curl_easy_setopt(h_curl, CURLOPT_HTTPHEADER, auth_header);

  std::string reply;
  curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(h_curl, CURLOPT_USERAGENT, "cvmfs/" VERSION);
  curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
  curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, "POST");
  curl_easy_setopt(h_curl, CURLOPT_TCP_KEEPALIVE, 1L);
  curl_easy_setopt(h_curl, CURLOPT_URL, (api_url_ + "/payloads").c_str());
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(payload_text.length()));
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, payload_text.c_str());
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, &reply);

  // Perform the Curl POST request
  CURLcode ret = curl_easy_perform(h_curl);

  const bool ok = (reply == "{\"status\":\"ok\"}");

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return ok && !ret;
}

void* SessionContext::UploadLoop(void* data) {
  SessionContext* ctx = reinterpret_cast<SessionContext*>(data);

  int64_t jobs_processed = 0;
  while (!ctx->ShouldTerminate()) {
    while (jobs_processed < ctx->NumJobsSubmitted()) {
      UploadJob* job = ctx->upload_jobs_.Dequeue();
      ctx->DoUpload(job);
      job->result->Set(true);
      delete job->pack;
      delete job;
      jobs_processed++;
    }
    if (ctx->queue_was_flushed_.IsEmpty()) {
      ctx->queue_was_flushed_.Enqueue(true);
    }
  }

  return NULL;
}

bool SessionContext::ShouldTerminate() {
  return atomic_read32(&worker_terminate_);
}

}  // namespace upload
