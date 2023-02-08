//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  LOG_INFO("New lru k replacer %ld %ld", num_frames, k);
  cands_ = new cand_t[num_frames];
  for (size_t i = 0; i < num_frames; i++) {
    cands_[i] = cand_t{static_cast<frame_id_t>(i), k, false, std::list<size_t>{}};
  }
}

LRUKReplacer::~LRUKReplacer() { delete[] cands_; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool ok = EvictInternal(frame_id);
  LOG_INFO("Evict %d %d curr %ld", ok, *frame_id, cache_.size());
  return ok;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("Record Access %d", frame_id);
  return RecordAccessInternal(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("SetEvictable %d %d curr %ld", frame_id, set_evictable, cache_.size());
  return SetEvictableInternal(frame_id, set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("Remove %d curr %ld", frame_id, cache_.size());
  return RemoveInternal(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return SizeInternal();
}

auto LRUKReplacer::EvictInternal(frame_id_t *frame_id) -> bool {
  if (cache_.empty()) {
    return false;
  }
  auto it = cache_.begin();
  cand_t_ptr pcand = *it;
  cache_.erase(it);
  *frame_id = pcand->frame_id;
  pcand->clear_history();
  return true;
}

void LRUKReplacer::RecordAccessInternal(frame_id_t frame_id) {
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "RecordAccess: invalid frame_id");
  cand_t_ptr pcand = &cands_[frame_id];
  if (pcand->incache()) {
    cache_.erase(pcand);
  }
  pcand->add_history(current_timestamp_);
  if (pcand->evictable) {
    cache_.insert(pcand);
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictableInternal(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "SetEvictable: invalid frame_id");
  cand_t_ptr pcand = &cands_[frame_id];
  if (set_evictable == pcand->evictable) {
    return;
  }
  if (set_evictable) {
    if (pcand->inuse()) {
      cache_.insert(pcand);
    }
  } else {
    if (pcand->incache()) {
      cache_.erase(pcand);
    }
  }
  pcand->evictable = set_evictable;
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  cand_t_ptr pcand = &cands_[frame_id];
  if (pcand->incache()) {
    cache_.erase(pcand);
  }
  pcand->clear_history();
}

auto LRUKReplacer::SizeInternal() -> size_t { return cache_.size(); }

}  // namespace bustub
