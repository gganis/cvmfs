/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "glue_buffer.h"

#include <cassert>
#include <cstdlib>
#include <cstring>

#include <string>
#include <vector>

#include "logging.h"
#include "platform.h"
#include "smalloc.h"

using namespace std;  // NOLINT

namespace glue {

PathStore &PathStore::operator= (const PathStore &other) {
  if (&other == this)
    return *this;

  delete string_heap_;
  CopyFrom(other);
  return *this;
}


PathStore::PathStore(const PathStore &other) {
  CopyFrom(other);
}


void PathStore::CopyFrom(const PathStore &other) {
  map_ = other.map_;

  string_heap_ = new StringHeap(other.string_heap_->used());
  shash::Md5 empty_path = map_.empty_key();
  for (unsigned i = 0; i < map_.capacity(); ++i) {
    if (map_.keys()[i] != empty_path) {
      (map_.values() + i)->name =
      string_heap_->AddString(map_.values()[i].name.length(),
                              map_.values()[i].name.data());
    }
  }
}


//------------------------------------------------------------------------------

void InodeTracker::CopyFrom(const InodeTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  path_map_ = other.path_map_;
  inode_map_ = other.inode_map_;
  inode_references_ = other.inode_references_;
  statistics_ = other.statistics_;
}


InodeTracker::InodeTracker() {
  version_ = kVersion;
}


InodeTracker::InodeTracker(const InodeTracker &other) {
  CopyFrom(other);
}


InodeTracker &InodeTracker::operator= (const InodeTracker &other) {
  if (&other == this)
    return *this;

  CopyFrom(other);
  return *this;
}

}  // namespace glue
