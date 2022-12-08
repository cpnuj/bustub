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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i < num_frames; i++) {
    access_history_.emplace_back(history_t{});
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  auto cmp = [this](frame_id_t f1, frame_id_t f2) {
    return this->CompareFrame(f1, f2);
  };

  if (candidates_.size() == 0) {
    return false;
  }

  auto cands = std::vector<frame_id_t>(candidates_.begin(), candidates_.end());
  std::sort(cands.begin(), cands.end(), cmp);
  *frame_id = cands.front();

  Remove(*frame_id);

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "RecordAccess: invalid frame_id");
  history_t &history = access_history_[frame_id];
  history.emplace_back(current_timestamp_);
  if (history.size() > k_) {
    history.erase(history.begin());
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (set_evictable) {
    BUSTUB_ASSERT(!access_history_[frame_id].empty(), "Try to set frame without access history to evictable");
    candidates_.insert(frame_id);
  } else {
    candidates_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // auto search = candidates_.find(frame_id);
  // BUSTUB_ASSERT(search != candidates_.end(), "Removing unevictable frame");
  candidates_.erase(frame_id);
  access_history_[frame_id].clear();
}

auto LRUKReplacer::Size() -> size_t { return candidates_.size(); }

}  // namespace bustub
