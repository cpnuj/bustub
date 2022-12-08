//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }

  *page_id = AllocatePage();
  page_table_tmp_[*page_id] = frame_id;

  Page *page = pages_ + frame_id;
  ResetPageMeta(page, *page_id);
  page->pin_count_++;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  Page *page;

  if (auto search = page_table_tmp_.find(page_id); search != page_table_tmp_.end()) {
    frame_id = search->second;
    page = pages_ + frame_id;
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    return page;
  }

  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }

  page_table_tmp_[page_id] = frame_id;

  page = pages_ + frame_id;
  ResetPageMeta(page, page_id);
  disk_manager_->ReadPage(page_id, page->GetData());
  page->pin_count_++;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  Page *page;

  auto search = page_table_tmp_.find(page_id);

  if (search == page_table_tmp_.end()) {
    return false;
  }

  frame_id = search->second;
  page = pages_ + frame_id;

  if (page->GetPinCount() <= 0) {
    return false;
  }

  page->pin_count_--;
  page->is_dirty_ = is_dirty;

  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  auto search = page_table_tmp_.find(page_id);
  if (search == page_table_tmp_.end()) {
    return false;
  }
  frame_id_t frame_id = search->second;
  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (auto it = page_table_tmp_.begin(); it != page_table_tmp_.end(); it++) {
    FlushPgImp(it->first);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::ResetPageMeta(Page *page, page_id_t page_id) {
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
}

auto BufferPoolManagerInstance::GetFreeFrameInternal(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  return replacer_->Evict(frame_id);
}

auto BufferPoolManagerInstance::GetFreeFrame(frame_id_t *frame_id) -> bool {
  if (!GetFreeFrameInternal(frame_id)) {
    return false;
  }

  Page *page = pages_ + (*frame_id);

  page_table_tmp_.erase(page->GetPageId());

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page->ResetMemory();

  return true;
}

}  // namespace bustub
