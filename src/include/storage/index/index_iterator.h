//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int cursor);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (page_id_ == itr.page_id_ && cursor_ == itr.cursor_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(itr == *this); }

 private:
  // add your own private member variables here
  MappingType data_;

  BufferPoolManager *bpm_;

  page_id_t page_id_;
  int cursor_;

  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  LeafPage *page_;
};

}  // namespace bustub
