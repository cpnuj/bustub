/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int cursor)
    : bpm_(bpm), page_id_(page_id), cursor_(cursor) {
  if (page_id_ != INVALID_PAGE_ID) {
    page_ = reinterpret_cast<LeafPage *>(bpm_->FetchPage(page_id_));
    data_ = MappingType(page_->KeyAt(cursor_), page_->ValueAt(cursor_));
  } else {
    page_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return data_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (!IsEnd()) {
    cursor_++;
    if (cursor_ >= page_->GetSize()) {
      page_id_ = page_->GetNextPageId();
      bpm_->UnpinPage(page_->GetPageId(), false);
      page_ = reinterpret_cast<LeafPage *>(bpm_->FetchPage(page_id_));
      cursor_ = 0;
    }
    if (!IsEnd()) {
      data_ = MappingType(page_->KeyAt(cursor_), page_->ValueAt(cursor_));
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
