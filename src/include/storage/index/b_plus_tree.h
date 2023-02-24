//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <variant>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

// Macros for reinterpret_cast between Page and BPlusTreePage
#define to_page_ptr(page) reinterpret_cast<Page *>(page)
#define to_tree_ptr(page) reinterpret_cast<BPlusTreePage *>(page)

// Macros for static_cast BPlusTreePage to LeafPage and InternalPage
#define to_leaf_ptr(page) static_cast<LeafPage *>(page)
#define to_inte_ptr(page) static_cast<InternalPage *>(page)

// Some latch wrappers
#define ROOT_PAGE_ID_LOCK nullptr
  void RLatchPage(BPlusTreePage *page);
  void RUnlatchPage(BPlusTreePage *page);
  void WLatchPage(BPlusTreePage *page);
  void WUnlatchPage(BPlusTreePage *page);

// Macros define PStackNode attributes
#define PStackNode_DIRTY (1 << 0)
#define PStackNode_TODEL (1 << 1)

  struct PStackNode {
    BPlusTreePage *page_;
    int route_idx_;
    int attribute_;
    explicit PStackNode(BPlusTreePage *page = nullptr, int route_idx = -1, int attribute = 0)
        : page_(page), route_idx_(route_idx), attribute_(attribute) {}
  };

  struct PStack {
    std::vector<PStackNode> nodes_;
    bool for_write;  // for_write is true while this stack handling wlatch,
                     // false while handling rlatch.
  };

  auto PStackNew(bool for_write, bool lock_root) -> PStack;
  auto PStackPointer(PStack &stack) -> int;
  void PStackSetRouteIdx(PStack &stack, int index, int route_idx);
  void PStackSetAttribute(PStack &stack, int index, int attri);
  auto PStackPushLockedPage(PStack &stack, PStackNode node) -> int;
  auto PStackPushUnlockedPage(PStack &stack, PStackNode node) -> int;
  void PStackPop(PStack &stack);
  auto PStackTop(PStack &stack) -> PStackNode;
  auto PStackEmpty(PStack &stack) -> bool;
  void PStackRelease(PStack &stack);

  using ValueUnion = std::variant<page_id_t, ValueType>;
  auto PageNewLeaf(page_id_t parent_page_id) -> LeafPage *;
  auto PageNewInternal(page_id_t parent_page_id) -> InternalPage *;
  auto PageGetKey(BPlusTreePage *page, int index) -> KeyType;
  auto PageGetValue(BPlusTreePage *page, int index) -> ValueUnion;
  void PageSetKeyValue(BPlusTreePage *page, int index, const KeyType &key, const ValueUnion &val);

  void SetRootPage(BPlusTreePage *page);
  auto RootPageExist() -> bool;
  void EnsureRootPageExist();

  using unlock_cond_fn = std::function<bool(BPlusTreePage *)>;
  auto BinarySearch(BPlusTreePage *page, const KeyType &key, int begin, int end) -> int;
  auto SearchPage(BPlusTreePage *page, const KeyType &key) -> int;
  void Search(PStack &stack, const KeyType &key, unlock_cond_fn &&safe_for_release);

  enum OpCode { OP_FINISH, OP_INSERT, OP_REMOVE, OP_REPLACE };
  struct Op {
    OpCode opcode_;
    int index_;
    KeyType key_;
    ValueUnion val_;

    Op(OpCode opcode = OP_FINISH, int index = -1, KeyType key = KeyType{}, ValueUnion val = ValueType{})
        : opcode_(opcode), index_(index), key_(key), val_(val) {}
  };

  auto DoOperation(PStack &stack, const Op &op) -> Op;
  void Run(PStack &stack, Op op);

  // member variable
  std::string index_name_;

  page_id_t root_page_id_;
  ReaderWriterLatch root_page_id_latch_;

  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
