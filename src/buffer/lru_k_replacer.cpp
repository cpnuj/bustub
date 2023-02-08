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
  for (size_t i = 0; i < num_frames; i++) {
    access_history_.emplace_back(history_t{});
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool ok = EvictInternal(frame_id);
  LOG_INFO("Evict %d %d curr %ld", ok, *frame_id, candidates_.size());
  return ok;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("Record Access %d", frame_id);
  return RecordAccessInternal(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("SetEvictable %d %d curr %ld", frame_id, set_evictable, candidates_.size());
  return SetEvictableInternal(frame_id, set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("Remove %d curr %ld", frame_id, candidates_.size());
  return RemoveInternal(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return SizeInternal();
}

auto LRUKReplacer::EvictInternal(frame_id_t *frame_id) -> bool {
  auto cmp = [this](frame_id_t f1, frame_id_t f2) { return this->CompareFrame(f1, f2); };

  if (candidates_.empty()) {
    return false;
  }

  auto cands = std::vector<frame_id_t>(candidates_.begin(), candidates_.end());
  std::sort(cands.begin(), cands.end(), cmp);
  *frame_id = cands.front();

  RemoveInternal(*frame_id);

  return true;
}

void LRUKReplacer::RecordAccessInternal(frame_id_t frame_id) {
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "RecordAccess: invalid frame_id");
  history_t &history = access_history_[frame_id];
  history.emplace_back(current_timestamp_);
  if (history.size() > k_) {
    history.erase(history.begin());
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictableInternal(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "SetEvictable: invalid frame_id");
  if (set_evictable) {
    if (!access_history_[frame_id].empty()) {
      candidates_.insert(frame_id);
    }
  } else {
    candidates_.erase(frame_id);
  }
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  auto it = candidates_.find(frame_id);
  if (it != candidates_.end()) {
    candidates_.erase(it);
    access_history_[frame_id].clear();
  }
}

auto LRUKReplacer::SizeInternal() -> size_t { return candidates_.size(); }

}  // namespace bustub
