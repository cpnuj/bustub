//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::ForEach(std::function<void(K, V)> const &fn) {
  std::scoped_lock<std::mutex> lock(latch_);
  return ForEachInternal(fn);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::ForEachInternal(std::function<void(K, V)> const &fn) {
  for (auto const &bucket : dir_) {
    for (auto const &kv : bucket->GetItems()) {
      fn(kv.first, kv.second);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return FindInternal(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return RemoveInternal(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  return InsertInternal(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::FindInternal(const K &key, V &value) -> bool {
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RemoveInternal(const K &key) -> bool {
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  auto bucket = dir_[IndexOf(key)];
  while (!bucket->Insert(key, value)) {
    if (global_depth_ == bucket->GetDepth()) {
      Extend();
    }
    Split(bucket);
    bucket = dir_[IndexOf(key)];
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto &items = bucket->GetItems();
  for (auto it = items.begin(); it != items.end();) {
    auto &new_bucket = dir_[IndexOf(it->first)];
    if (new_bucket != bucket) {
      new_bucket->Insert(it->first, it->second);
      it = items.erase(it);
    } else {
      it++;
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Split(std::shared_ptr<Bucket> bucket) -> void {
  bucket->IncrementDepth();
  std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  num_buckets_++;

  for (uint64_t i = 0; i < dir_.size(); ++i) {
    if (dir_[i] == bucket && (i & (1 << (bucket->GetDepth() - 1)))) {
      dir_[i] = new_bucket;
    }
  }

  RedistributeBucket(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Extend() -> void {
  global_depth_++;

  dir_.resize(std::pow(2, global_depth_));
  size_t old_mask = (1 << (global_depth_ - 1)) - 1;

  for (auto i = dir_.size() / 2; i < dir_.size(); i++) {
    dir_[i] = dir_[i & old_mask];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto const &kv : list_) {
    if (kv.first == key) {
      value = kv.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (list_.size() >= size_) {
    return false;
  }

  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }

  list_.emplace_back(std::pair(key, value));

  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
