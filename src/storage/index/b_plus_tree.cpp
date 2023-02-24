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
      internal_max_size_(internal_max_size) {}

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
  bool res;

  LeafPage *leaf;
  int index;
  Search(key, &leaf, &index);

  if (index == -1 || comparator_(key, leaf->KeyAt(index))) {
    res = false;
    goto ret;
  }

  res = true;
  result->emplace_back(leaf->ValueAt(index));

ret:
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return res;
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
  if (root_page_id_ == INVALID_PAGE_ID) {
    page_id_t pid;
    LeafPage *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&pid));
    leaf->Init(pid, INVALID_PAGE_ID);
    root_page_id_ = pid;
    leaf->SetValue(0, key, value);
    leaf->IncreaseSize(1);
    ToString(leaf, buffer_pool_manager_);
    return true;
  }
  return false;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

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
auto BPLUSTREE_TYPE::PStackNew(bool for_write, bool lock_root) -> PStack {
  PStack stack;
  stack.for_write = for_write;
  if (lock_root) {
    if (for_write) {
      root_page_id_latch_.WLock();
    } else {
      root_page_id_latch_.RLock();
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
void BPLUSTREE_TYPE::PStackSetAttribute(PStack &stack, int index, int attri) {
  stack.nodes_[index].attribute_ |= attri;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PStackPush(PStack &stack, PStackNode node) -> int {
  stack.nodes_.emplace_back(node);
  if (stack.for_write) {
    to_page_ptr(node.page_)->WLatch();
  } else {
    to_page_ptr(node.page_)->RLatch();
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
    if (stack.for_write) {
      root_page_id_latch_.WUnlock();
    } else {
      root_page_id_latch_.RUnlock();
    }
    return;
  }

  if (stack.for_write) {
    to_page_ptr(node.page_)->WUnlatch();
  } else {
    to_page_ptr(node.page_)->RUnlatch();
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
void BPLUSTREE_TYPE::PStackRelease(PStack &stack) {
  while (!stack.nodes_.empty()) {
    PStackPop(stack);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PageGetKey(BPlusTreePage *page, int index) -> KeyType {
  if (page->IsLeafPage()) {
    return static_cast<LeafPage *>(page)->KeyAt(index);
  } else {
    return static_cast<InternalPage *>(page)->KeyAt(index);
  }
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
  } else if (comparator_(key, mid_key) < 0) {
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
  int end = page->GetSize();
  int result = BinarySearch(page, key, begin, end);
  // The result of BinarySearch stops at the first key >= search key.
  // For leaf page, that is enough. For internal page, however,
  // we still have no knowledge about going left or right.
  if (!page->IsLeafPage()) {
    auto found_key = PageGetKey(page, result);
    if (result >= page->GetSize() || comparator_(key, found_key) < 0) {
      result -= 1;
    }
  }
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Search(const KeyType &key, LeafPage **leaf, int *index) {
  page_id_t target = root_page_id_;
  BPlusTreePage *page;
  for (;;) {
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(target));
    if (page->IsLeafPage()) {
      break;
    }
    // search an internal page
    InternalPage *inter = static_cast<InternalPage *>(page);
    page_id_t next = SearchInternal(inter, key);
    buffer_pool_manager_->UnpinPage(target, false);
    target = next;
  }
  // reach a leaf page
  *leaf = static_cast<LeafPage *>(page);
  SearchLeaf(*leaf, key, index);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInternal(InternalPage *page, const KeyType &key) -> page_id_t {
  // We should start at index 1 since the first key at internal page
  // is invalid. See comments at b_plus_tree_internal_page.h.
  for (int i = 1; i < page->GetSize(); i++) {
    if (comparator_(key, page->KeyAt(i))) {
      return page->ValueAt(i - 1);
    }
  }
  return page->ValueAt(page->GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SearchLeaf(LeafPage *page, const KeyType &key, int *index) {
  for (int i = 0; i < page->GetSize(); i++) {
    if (comparator_(page->KeyAt(i), key)) {
      continue;
    }
    *index = i;
    return;
  }
  // not found
  *index = -1;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
