#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  LOG_DEBUG("create b+ tree %d %d", leaf_max_size, internal_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  bool for_write = false;
  bool lock_root = true;
  auto stack = PStackNew(for_write, lock_root);

  // For reading a value, once we got rlatch for current page,
  // we can immediately release all previous rlatch, so the lock
  // policy can just return true;
  auto safe_for_release = [](BPlusTreePage *page) { return true; };

  bool ok = false;
  PStackNode leaf_node;
  KeyType found_key;
  ValueType found_val;

  if (!RootPageExist()) {
    goto end;
  }

  // Now the stack should only contain a leaf node
  Search(stack, key, std::move(safe_for_release));
  leaf_node = PStackTop(stack);

  if (leaf_node.route_idx_ >= leaf_node.page_->GetSize()) {
    goto end;
  }

  found_key = PageGetKey(leaf_node.page_, leaf_node.route_idx_);
  if (comparator_(key, found_key) != 0) {
    goto end;
  }

  ok = true;
  found_val = std::get<ValueType>(PageGetValue(leaf_node.page_, leaf_node.route_idx_));
  result->emplace_back(found_val);

end:
  PStackRelease(stack);
  return ok;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  PStack stack;
  PState state;

  bool for_write = true;
  bool lock_root = true;
  stack = PStackNew(for_write, lock_root);

  EnsureRootPageExist();

  auto safe_for_insert = [this](BPlusTreePage *page) { return this->PageSafeForInsert(page); };

  Search(stack, key, safe_for_insert);
  PStackNode node = PStackTop(stack);

  bool ok = false;
  KeyType found_key;

  if (node.route_idx_ < node.page_->GetSize()) {
    found_key = PageGetKey(node.page_, node.route_idx_);
    if (comparator_(key, found_key) == 0) {
      goto end;
    }
  }

  ok = true;

  // do insert
  state.opcode_ = OP_INSERT;
  state.index_ = node.route_idx_;
  state.k1_ = key;
  state.v1_ = value;

  Run(stack, state);

end:
  PStackRelease(stack);
  return ok;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  PStack stack;
  PState state;

  bool for_write = true;
  bool lock_root = true;
  stack = PStackNew(for_write, lock_root);

  if (!RootPageExist()) {
    PStackRelease(stack);
    return;
  }

  auto safe_for_remove = [this](BPlusTreePage *page) { return this->PageSafeForRemove(page); };

  Search(stack, key, safe_for_remove);
  PStackNode node = PStackTop(stack);

  if (node.route_idx_ >= node.page_->GetSize()) {
    PStackRelease(stack);
    return;
  }
  if (comparator_(key, PageGetKey(node.page_, node.route_idx_)) != 0) {
    PStackRelease(stack);
    return;
  }

  state.opcode_ = OP_REMOVE;
  state.index_ = node.route_idx_;

  Run(stack, state);
  PStackRelease(stack);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  LeafPage *leaf = FirstLeaf();
  auto result = INDEXITERATOR_TYPE(buffer_pool_manager_, leaf->GetPageId(), 0);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return result;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  PStack stack = PStackNew(false, true);
  auto release_fn = [](BPlusTreePage *_) { return true; };
  Search(stack, key, std::move(release_fn));
  PStackNode node = PStackTop(stack);
  auto result = INDEXITERATOR_TYPE(buffer_pool_manager_, node.page_->GetPageId(), node.route_idx_);
  PStackRelease(stack);
  return result;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RLatchPage(BPlusTreePage *page) {
  if (page == ROOT_PAGE_ID_LOCK) {
    // LOG_DEBUG("rlatch 0");
    root_page_id_latch_.RLock();
  } else {
    // LOG_DEBUG("rlatch %d", page->GetPageId());
    to_page_ptr(page)->RLatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RUnlatchPage(BPlusTreePage *page) {
  if (page == ROOT_PAGE_ID_LOCK) {
    // LOG_DEBUG("runlatch 0");
    root_page_id_latch_.RUnlock();
  } else {
    // LOG_DEBUG("runlatch %d", page->GetPageId());
    to_page_ptr(page)->RUnlatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::WLatchPage(BPlusTreePage *page) {
  if (page == ROOT_PAGE_ID_LOCK) {
    // LOG_DEBUG("wlatch 0");
    root_page_id_latch_.WLock();
  } else {
    // LOG_DEBUG("wlatch %d", page->GetPageId());
    to_page_ptr(page)->WLatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::WUnlatchPage(BPlusTreePage *page) {
  if (page == ROOT_PAGE_ID_LOCK) {
    // LOG_DEBUG("wunlatch 0");
    root_page_id_latch_.WUnlock();
  } else {
    // LOG_DEBUG("wunlatch %d", page->GetPageId());
    to_page_ptr(page)->WUnlatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackNew(bool for_write, bool lock_root) -> PStack {
  PStack stack;
  stack.for_write_ = for_write;
  if (lock_root) {
    if (for_write) {
      WLatchPage(ROOT_PAGE_ID_LOCK);
    } else {
      RLatchPage(ROOT_PAGE_ID_LOCK);
    }
    // use a PStackNode with nullptr indicating
    // root_page_id was locked
    stack.nodes_.emplace_back(PStackNode(nullptr));
  }
  return stack;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackPointer(PStack &stack) -> int { return stack.nodes_.size() - 1; }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PStackSetRouteIdx(PStack &stack, int index, int route_idx) {
  stack.nodes_[index].route_idx_ = route_idx;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PStackSetAttr(PStack &stack, int index, int attr) { stack.nodes_[index].attribute_ |= attr; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackPushLockedPage(PStack &stack, PStackNode node) -> int {
  stack.nodes_.emplace_back(node);
  return PStackPointer(stack);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackPushUnlockedPage(PStack &stack, PStackNode node) -> int {
  stack.nodes_.emplace_back(node);
  if (stack.for_write_) {
    WLatchPage(node.page_);
  } else {
    RLatchPage(node.page_);
  }
  return PStackPointer(stack);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PStackPop(PStack &stack) {
  BUSTUB_ASSERT(!stack.nodes_.empty(), "pop empty stack");

  PStackNode node = stack.nodes_.back();
  stack.nodes_.pop_back();

  // nullptr page_ means we are locking root_page_id_
  if (node.page_ == nullptr) {
    if (stack.for_write_) {
      WUnlatchPage(ROOT_PAGE_ID_LOCK);
    } else {
      RUnlatchPage(ROOT_PAGE_ID_LOCK);
    }
    return;
  }

  if (stack.for_write_) {
    WUnlatchPage(node.page_);
  } else {
    RUnlatchPage(node.page_);
  }

  bool is_dirty = node.attribute_ & PStackNode_DIRTY;
  bool is_todel = node.attribute_ & PStackNode_TODEL;

  page_id_t page_id = node.page_->GetPageId();

  buffer_pool_manager_->UnpinPage(page_id, is_dirty);
  if (is_todel) {
    buffer_pool_manager_->DeletePage(node.page_->GetPageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackTop(PStack &stack) -> PStackNode { return stack.nodes_.back(); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackGet(PStack &stack, int index) -> PStackNode { return stack.nodes_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackEmpty(PStack &stack) -> bool { return stack.nodes_.empty(); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PStackSwap(PStack &stack, int i1, int i2) {
  PStackNode temp = stack.nodes_[i2];
  stack.nodes_[i2] = stack.nodes_[i1];
  stack.nodes_[i1] = temp;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PStackRelease(PStack &stack) {
  while (!stack.nodes_.empty()) {
    PStackPop(stack);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageNewLeaf(page_id_t parent_page_id) -> LeafPage * {
  page_id_t page_id;
  auto *page = to_tree_ptr(buffer_pool_manager_->NewPage(&page_id));
  auto *leaf = to_leaf_ptr(page);
  leaf->Init(page_id, parent_page_id, leaf_max_size_);
  return leaf;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageNewInternal(page_id_t parent_page_id) -> InternalPage * {
  page_id_t page_id;
  auto *page = to_tree_ptr(buffer_pool_manager_->NewPage(&page_id));
  auto *inte = to_inte_ptr(page);
  inte->Init(page_id, parent_page_id, internal_max_size_);
  return inte;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageGetKey(BPlusTreePage *page, int index) -> KeyType {
  if (page->IsLeafPage()) {
    return to_leaf_ptr(page)->KeyAt(index);
  }
  return to_inte_ptr(page)->KeyAt(index);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageGetValue(BPlusTreePage *page, int index) -> ValueUnion {
  if (page->IsLeafPage()) {
    return to_leaf_ptr(page)->ValueAt(index);
  }
  return to_inte_ptr(page)->ValueAt(index);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PageSetKeyValue(BPlusTreePage *page, int index, const KeyType &key, const ValueUnion &val) {
  if (page->IsLeafPage()) {
    return to_leaf_ptr(page)->SetKeyValue(index, key, std::get<ValueType>(val));
  }
  return to_inte_ptr(page)->SetKeyValue(index, key, std::get<page_id_t>(val));
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PageInsertAt(BPlusTreePage *page, int index, const KeyType &key, const ValueUnion &val) {
  page->IncreaseSize(1);
  for (int i = page->GetSize() - 1; i > index; i--) {
    auto prev_k = PageGetKey(page, i - 1);
    auto prev_v = PageGetValue(page, i - 1);
    PageSetKeyValue(page, i, prev_k, prev_v);
  }
  PageSetKeyValue(page, index, key, val);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PageRemoveAt(BPlusTreePage *page, int index) {
  BUSTUB_ASSERT(index < page->GetSize(), "index >= page size");
  for (int i = index; i < page->GetSize() - 1; i++) {
    auto next_k = PageGetKey(page, i + 1);
    auto next_v = PageGetValue(page, i + 1);
    PageSetKeyValue(page, i, next_k, next_v);
  }
  page->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PageAppend(BPlusTreePage *page, const KeyType &key, const ValueUnion &val) {
  PageInsertAt(page, page->GetSize(), key, val);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageOverflow(BPlusTreePage *page) -> bool {
  if (page->IsLeafPage()) {
    return to_leaf_ptr(page)->Overflow();
  }
  return to_inte_ptr(page)->Overflow();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageUnderflow(BPlusTreePage *page) -> bool { return page->GetSize() < page->GetMinSize(); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PageMoveOne(BPlusTreePage *src, int src_idx, BPlusTreePage *dst, int dst_idx) {
  auto key = PageGetKey(src, src_idx);
  auto val = PageGetValue(src, src_idx);
  PageInsertAt(dst, dst_idx, key, val);
  PageRemoveAt(src, src_idx);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PageResetKey(BPlusTreePage *page, int index, const KeyType &key) {
  PageSetKeyValue(page, index, key, PageGetValue(page, index));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageSafeForInsert(BPlusTreePage *page) -> bool {
  if (page->IsLeafPage()) {
    return to_leaf_ptr(page)->GetSize() + 1 < to_leaf_ptr(page)->GetMaxSize();
  }
  return to_inte_ptr(page)->GetSize() + 1 <= to_inte_ptr(page)->GetMaxSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageSafeForRemove(BPlusTreePage *page) -> bool {
  if (page->IsLeafPage()) {
    return to_leaf_ptr(page)->GetSize() - 1 >= to_leaf_ptr(page)->GetMinSize();
  }
  return to_inte_ptr(page)->GetSize() - 1 >= to_inte_ptr(page)->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPage(BPlusTreePage *page) {
  root_page_id_ = page != nullptr ? page->GetPageId() : INVALID_PAGE_ID;
  UpdateRootPageId();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RootPageExist() -> bool { return root_page_id_ != INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::EnsureRootPageExist() {
  if (RootPageExist()) {
    return;
  }
  LeafPage *page = PageNewLeaf(INVALID_PAGE_ID);
  SetRootPage(page);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(BPlusTreePage *page, const KeyType &key, int begin, int end) -> int {
  if (begin > end) {
    return begin;
  }
  int mid = begin + (end - begin) / 2;
  auto mid_key = PageGetKey(page, mid);
  if (comparator_(key, mid_key) > 0) {
    return BinarySearch(page, key, mid + 1, end);
  }
  if (comparator_(key, mid_key) < 0) {
    return BinarySearch(page, key, begin, mid - 1);
  }
  return mid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchPage(BPlusTreePage *page, const KeyType &key) -> int {
  // We start from index 1 for internal page, since the
  // first key at internal page is invalid.
  // See comments in b_plus_tree_internal_page.h.
  int begin = (page->IsLeafPage()) ? 0 : 1;
  int end = page->GetSize() - 1;
  int result = BinarySearch(page, key, begin, end);
  // The result of BinarySearch stops at the first key >= search key.
  // For leaf page, that is enough. For internal page, however,
  // we still have no knowledge about going left or right.
  if (!page->IsLeafPage()) {
    if (result >= page->GetSize() || comparator_(key, PageGetKey(page, result)) < 0) {
      result -= 1;
    }
  }
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Search(PStack &stack, const KeyType &key, unlock_cond_fn &&safe_for_release) {
  BUSTUB_ASSERT(RootPageExist(), "search with invalid btree");

  Page *raw_page;
  BPlusTreePage *page;
  page_id_t next;

  next = root_page_id_;
  for (;;) {
    // fetch page and lock it
    raw_page = buffer_pool_manager_->FetchPage(next);
    page = to_tree_ptr(raw_page);
    BUSTUB_ASSERT(page != nullptr, "fail to fetch page");
    if (stack.for_write_) {
      WLatchPage(page);
    } else {
      RLatchPage(page);
    }

    // check if it's safe to release previous latches,
    // then add the page to stack
    if (safe_for_release(page)) {
      PStackRelease(stack);
    }
    int sp = PStackPushLockedPage(stack, PStackNode(page));

    int route_idx = SearchPage(page, key);
    PStackSetRouteIdx(stack, sp, route_idx);

    if (page->IsLeafPage()) {
      break;
    }

    next = std::get<page_id_t>(PageGetValue(page, route_idx));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitPage(PStack &stack, BPlusTreePage *lhs, KeyType *upkey, page_id_t *upval) {
  BPlusTreePage *rhs;
  if (lhs->IsLeafPage()) {
    rhs = PageNewLeaf(lhs->GetParentPageId());
  } else {
    rhs = PageNewInternal(lhs->GetParentPageId());
  }
  BUSTUB_ASSERT(rhs != nullptr, "fail to fetch page");
  PStackPushUnlockedPage(stack, PStackNode(rhs));
  PStackSetAttr(stack, PStackPointer(stack), PStackNode_DIRTY);

  for (int i = lhs->GetMinSize(); i < lhs->GetSize(); i++) {
    auto key = PageGetKey(lhs, i);
    auto val = PageGetValue(lhs, i);
    PageAppend(rhs, key, val);
  }
  lhs->SetSize(lhs->GetMinSize());

  page_id_t rhs_page_id = rhs->GetPageId();

  *upkey = PageGetKey(rhs, 0);
  *upval = rhs_page_id;

  if (lhs->IsLeafPage()) {
    to_leaf_ptr(rhs)->SetNextPageId(to_leaf_ptr(lhs)->GetNextPageId());
    to_leaf_ptr(lhs)->SetNextPageId(rhs_page_id);
  } else {
    // set first key of interal page to invalid
    PageSetKeyValue(rhs, 0, KeyType{}, PageGetValue(rhs, 0));

    // reset children's parent pointer
    PStack tmp_stack = PStackNew(true /* for_write */, false /* lock_root */);
    BPlusTreePage *child;
    page_id_t child_page_id;
    for (int i = 0; i < rhs->GetSize(); i++) {
      child_page_id = std::get<page_id_t>(PageGetValue(rhs, i));
      child = to_tree_ptr(buffer_pool_manager_->FetchPage(child_page_id));
      BUSTUB_ASSERT(child != nullptr, "fail to fetch page");
      PStackPushUnlockedPage(tmp_stack, PStackNode(child));
      PStackSetAttr(tmp_stack, PStackPointer(tmp_stack), PStackNode_DIRTY);
      child->SetParentPageId(rhs_page_id);
      PStackPop(tmp_stack);
    }
    PStackRelease(tmp_stack);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoInsert(PStack &stack, PState &state) {
  auto index = state.index_;
  auto key = state.k1_;
  auto val = state.v1_;

  int sp = PStackPointer(stack);
  PStackNode node = PStackTop(stack);
  PageInsertAt(node.page_, index, key, val);
  PStackSetAttr(stack, sp, PStackNode_DIRTY);

  if (!PageOverflow(node.page_)) {
    PStackPop(stack);
    state.opcode_ = OP_FINISH;
    return;
  }

  // LOG_DEBUG("page %d overflow", node.page_->GetPageId());

  KeyType upkey;
  page_id_t upval;
  SplitPage(stack, node.page_, &upkey, &upval);

  PStackNode parent = PStackGet(stack, sp - 1);
  if (parent.page_ != nullptr) {
    PStackPop(stack);
    PStackPop(stack);
    state.opcode_ = OP_INSERT;
    state.index_ = parent.route_idx_ + 1;
    state.k1_ = upkey;
    state.v1_ = upval;
    return;
  }

  // we are at root now
  state.opcode_ = OP_ADDROOT;
  state.k1_ = upkey;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoRemove(PStack &stack, PState &state) {
  int sp = PStackPointer(stack);
  PStackNode curr = PStackGet(stack, sp);

  PageRemoveAt(curr.page_, state.index_);
  PStackSetAttr(stack, sp, PStackNode_DIRTY);

  if (!PageUnderflow(curr.page_)) {
    state.opcode_ = OP_FINISH;
    return;
  }

  // page underflow now
  if (curr.page_->IsRootPage()) {
    if (curr.page_->IsLeafPage()) {
      if (curr.page_->GetSize() == 0) {
        // cleanup root_page_id
        PStackSetAttr(stack, sp, PStackNode_TODEL);
        SetRootPage(nullptr);
      }
    } else {
      if (curr.page_->GetSize() == 1) {
        page_id_t new_root_page_id;
        BPlusTreePage *new_root;
        new_root_page_id = std::get<page_id_t>(PageGetValue(curr.page_, 0));
        new_root = to_tree_ptr(buffer_pool_manager_->FetchPage(new_root_page_id));
        BUSTUB_ASSERT(new_root != nullptr, "fail to fetch page");
        sp = PStackPushUnlockedPage(stack, PStackNode(new_root));
        new_root->SetParentPageId(INVALID_PAGE_ID);
        SetRootPage(new_root);
        PStackSetAttr(stack, sp, PStackNode_DIRTY);
        PStackSetAttr(stack, sp - 1, PStackNode_TODEL);
      }
    }
    state.opcode_ = OP_FINISH;
    return;
  }

  PStackNode prev = PStackGet(stack, sp - 1);

  BPlusTreePage *nbhd;  // neighborhood
  int nbhd_route_idx;
  page_id_t nbhd_page_id;

  if (prev.route_idx_ == prev.page_->GetSize() - 1) {
    nbhd_route_idx = prev.route_idx_ - 1;
  } else {
    nbhd_route_idx = prev.route_idx_ + 1;
  }

  nbhd_page_id = std::get<page_id_t>(PageGetValue(prev.page_, nbhd_route_idx));
  nbhd = to_tree_ptr(buffer_pool_manager_->FetchPage(nbhd_page_id));
  BUSTUB_ASSERT(nbhd != nullptr, "fail to fetch page");

  sp = PStackPushUnlockedPage(stack, PStackNode(nbhd));

  if (nbhd->GetSize() - 1 >= nbhd->GetMinSize()) {
    state.opcode_ = OP_REBALANCE;
    state.index_ = nbhd_route_idx > prev.route_idx_;
    return;
  }

  int route_rhs = nbhd_route_idx;
  if (nbhd_route_idx < prev.route_idx_) {
    route_rhs = prev.route_idx_;
    PStackSwap(stack, sp, sp - 1);
  }
  state.opcode_ = OP_MERGE;
  state.index_ = route_rhs;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoAddRoot(PStack &stack, PState &state) {
  int sp = PStackPointer(stack);
  PStackNode rnode = PStackGet(stack, sp);
  PStackNode lnode = PStackGet(stack, sp - 1);

  page_id_t new_root_page_id;
  BPlusTreePage *new_root;

  new_root = PageNewInternal(INVALID_PAGE_ID);
  PageAppend(new_root, KeyType{}, lnode.page_->GetPageId());
  PageAppend(new_root, state.k1_, rnode.page_->GetPageId());

  new_root_page_id = new_root->GetPageId();
  lnode.page_->SetParentPageId(new_root_page_id);
  rnode.page_->SetParentPageId(new_root_page_id);

  SetRootPage(new_root);

  buffer_pool_manager_->UnpinPage(new_root_page_id, true /* dirty */);

  state.opcode_ = OP_FINISH;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoRebalance(PStack &stack, PState &state) {
  int sp = PStackPointer(stack);
  PStackNode src = PStackGet(stack, sp);
  PStackNode dst = PStackGet(stack, sp - 1);
  PStackNode parent = PStackGet(stack, sp - 2);

  int upidx;
  page_id_t child_page_id;  // record changed child page id for internal
  BPlusTreePage *child;
  PStack temp_stack = PStackNew(true, false);

  bool right_to_left = state.index_ > 0;

  if (dst.page_->IsLeafPage()) {
    if (right_to_left) {
      upidx = parent.route_idx_ + 1;
      PageMoveOne(src.page_, 0, dst.page_, dst.page_->GetSize());
      PageResetKey(parent.page_, upidx, PageGetKey(src.page_, 0));
    } else {
      upidx = parent.route_idx_;
      PageMoveOne(src.page_, src.page_->GetSize() - 1, dst.page_, 0);
      PageResetKey(parent.page_, upidx, PageGetKey(dst.page_, 0));
    }
    goto end;
  }

  // internal page
  if (right_to_left) {
    upidx = parent.route_idx_ + 1;
    child_page_id = std::get<page_id_t>(PageGetValue(src.page_, 0));
    PageMoveOne(src.page_, 0, dst.page_, dst.page_->GetSize());
    // demote key
    PageResetKey(dst.page_, dst.page_->GetSize() - 1, PageGetKey(parent.page_, upidx));
    // promote key
    PageResetKey(parent.page_, upidx, PageGetKey(src.page_, 0));
    PageResetKey(src.page_, 0, KeyType{});
  } else {
    upidx = parent.route_idx_;
    child_page_id = std::get<page_id_t>(PageGetValue(src.page_, src.page_->GetSize() - 1));
    // demote key
    PageResetKey(dst.page_, 0, PageGetKey(parent.page_, upidx));
    PageMoveOne(src.page_, src.page_->GetSize() - 1, dst.page_, 0);
    // promote key
    PageResetKey(parent.page_, upidx, PageGetKey(dst.page_, 0));
    PageResetKey(dst.page_, 0, KeyType{});
  }
  child = to_tree_ptr(buffer_pool_manager_->FetchPage(child_page_id));
  BUSTUB_ASSERT(child != nullptr, "fail to fetch page");
  PStackPushUnlockedPage(temp_stack, PStackNode(child));
  PStackSetAttr(temp_stack, PStackPointer(temp_stack), PStackNode_DIRTY);
  child->SetParentPageId(dst.page_->GetPageId());

end:
  PStackRelease(temp_stack);
  PStackSetAttr(stack, sp, PStackNode_DIRTY);
  PStackSetAttr(stack, sp - 1, PStackNode_DIRTY);
  PStackSetAttr(stack, sp - 2, PStackNode_DIRTY);
  PStackPop(stack);
  PStackPop(stack);
  PStackPop(stack);
  state.opcode_ = OP_FINISH;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoMerge(PStack &stack, PState &state) {
  int sp = PStackPointer(stack);
  PStackNode rhs = PStackGet(stack, sp);
  PStackNode lhs = PStackGet(stack, sp - 1);
  PStackNode parent = PStackGet(stack, sp - 2);

  PStack temp_stack = PStackNew(true, false);

  if (rhs.page_->IsLeafPage()) {
    to_leaf_ptr(lhs.page_)->SetNextPageId(to_leaf_ptr(rhs.page_)->GetNextPageId());
  } else {
    PageResetKey(rhs.page_, 0, PageGetKey(parent.page_, state.index_));
  }

  while (rhs.page_->GetSize() > 0) {
    PageMoveOne(rhs.page_, 0, lhs.page_, lhs.page_->GetSize());
    if (!rhs.page_->IsLeafPage()) {
      page_id_t child_page_id = std::get<page_id_t>(PageGetValue(lhs.page_, lhs.page_->GetSize() - 1));
      auto *child = to_tree_ptr(buffer_pool_manager_->FetchPage(child_page_id));
      BUSTUB_ASSERT(child != nullptr, "fail to fetch page");
      PStackPushUnlockedPage(temp_stack, PStackNode(child));
      child->SetParentPageId(lhs.page_->GetPageId());
      PStackSetAttr(temp_stack, PStackPointer(temp_stack), PStackNode_DIRTY);
      PStackPop(temp_stack);
    }
  }

  PStackRelease(temp_stack);

  PStackSetAttr(stack, sp, PStackNode_TODEL);
  PStackSetAttr(stack, sp - 1, PStackNode_DIRTY);
  PStackPop(stack);
  PStackPop(stack);

  state.opcode_ = OP_REMOVE;
  // state.index_ = state.index_; not need to change
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoOperation(PStack &stack, PState &state) {
  switch (state.opcode_) {
    case OP_INSERT:
      return DoInsert(stack, state);

    case OP_ADDROOT:
      return DoAddRoot(stack, state);

    case OP_REMOVE:
      return DoRemove(stack, state);

    case OP_REBALANCE:
      return DoRebalance(stack, state);

    case OP_MERGE:
      return DoMerge(stack, state);

    case OP_FINISH:
    default:
      BUSTUB_ASSERT(false, "invalid opcode");
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Run(PStack &stack, PState &state) {
  while (state.opcode_ != OP_FINISH) {
    DoOperation(stack, state);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FirstLeaf() -> LeafPage * {
  page_id_t target = root_page_id_;
  BPlusTreePage *page;
  for (;;) {
    page = to_tree_ptr(buffer_pool_manager_->FetchPage(target));
    if (page->IsLeafPage()) {
      break;
    }
    target = to_inte_ptr(page)->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  return to_leaf_ptr(page);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
