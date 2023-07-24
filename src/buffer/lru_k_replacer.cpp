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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  node_store_.reserve(num_frames);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = cold_list_.begin(); it != cold_list_.end(); ++it) {
    if (node_store_[*it].is_evictable_) {
      *frame_id = *it;
      node_store_.erase(*it);
      cold_list_.erase(it);
      --curr_size_;
      return true;
    }
  }
  for (auto it = hot_list_.begin(); it != hot_list_.end(); ++it) {
    if (node_store_[*it].is_evictable_) {
      *frame_id = *it;
      node_store_.erase(*it);
      hot_list_.erase(it);
      --curr_size_;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> latch(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    std::list<frame_id_t>::iterator list_it;
    list_it = cold_list_.insert(cold_list_.end(), frame_id);
    node_store_.emplace(frame_id, LRUKNode{list_it, 1, false});
    return;
  }
  LRUKNode &node = node_it->second;
  if (node.access_count_ >= k_) {
    hot_list_.erase(node.list_it_);
    hot_list_.emplace_back(frame_id);
    node.list_it_ = --hot_list_.end();
    return;
  }
  ++node.access_count_;
  if (node.access_count_ >= k_) {
    cold_list_.erase(node.list_it_);
    hot_list_.emplace_back(frame_id);
    node.list_it_ = --hot_list_.end();
    return;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> latch(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it != node_store_.end()) {
    curr_size_ += static_cast<int>(set_evictable) - static_cast<int>(node_it->second.is_evictable_);
    node_it->second.is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    return;
  }
  LRUKNode &node = node_it->second;
  if (!node.is_evictable_) {
    throw bustub::Exception("Remove non-evictable node frame!");
  }
  --curr_size_;
  if (node.access_count_ >= k_) {
    hot_list_.erase(node.list_it_);
  } else {
    cold_list_.erase(node.list_it_);
  }
  node_store_.erase(node_it);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> latch(latch_);
  return curr_size_;
}

}  // namespace bustub
