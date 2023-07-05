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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = cold_list_.begin(); it != cold_list_.end(); ++it) {
    const LRUKNode &node = **it;
    if (node.is_evictable_) {
      *frame_id = node.fid_;
      node_store_.erase(node.fid_);
      cold_list_.erase(it);
      --curr_size_;
      return true;
    }
  }
  for (auto it = warm_list_.begin(); it != warm_list_.end(); ++it) {
    const LRUKNode &node = **it;
    if (node.is_evictable_) {
      *frame_id = node.fid_;
      node_store_.erase(node.fid_);
      warm_list_.erase(it);
      --curr_size_;
      return true;
    }
  }
  for (auto it = hot_list_.begin(); it != hot_list_.end(); ++it) {
    const LRUKNode &node = **it;
    if (node.is_evictable_) {
      *frame_id = node.fid_;
      node_store_.erase(node.fid_);
      hot_list_.erase(it);
      --curr_size_;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> latch(latch_);
  ++current_timestamp_;
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    auto [store_it, _] =
        node_store_.emplace(frame_id, LRUKNode{{current_timestamp_}, {}, {}, frame_id, access_type, false});
    LRUKNode &node = store_it->second;
    if (access_type == AccessType::Get) {
      auto warm_it = warm_list_.insert(cold_list_.end(), &node);
      node.cold_it_ = warm_it;
      return;
    }
    auto cold_it = cold_list_.insert(cold_list_.end(), &node);
    node.cold_it_ = cold_it;
    return;
  }
  LRUKNode &node = node_it->second;
  if (node.accessed_time_.size() >= k_) {
    hot_list_.erase(node.hot_it_);
    node.accessed_time_.pop_front();
    node.accessed_time_.emplace_back(current_timestamp_);
    auto [hot_it, _] = hot_list_.emplace(&node);
    node.hot_it_ = hot_it;
    return;
  }
  node.accessed_time_.emplace_back(current_timestamp_);
  if (node.accessed_time_.size() >= k_) {
    if (node.access_type_ == AccessType::Get) {
      warm_list_.erase(node.cold_it_);
    } else {
      cold_list_.erase(node.cold_it_);
    }
    auto [hot_it, _] = hot_list_.emplace(&node);
    node.hot_it_ = hot_it;
    return;
  }
  if (node.access_type_ != access_type && access_type == AccessType::Get) {
    cold_list_.erase(node.cold_it_);
    auto warm_it = warm_list_.insert(warm_list_.end(), &node);
    node.cold_it_ = warm_it;
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
  if (node.accessed_time_.size() >= k_) {
    hot_list_.erase(node.hot_it_);
  } else if (node.access_type_ == AccessType::Get) {
    warm_list_.erase(node.cold_it_);
  } else {
    cold_list_.erase(node.cold_it_);
  }
  node_store_.erase(node_it);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> latch(latch_);
  return curr_size_;
}

}  // namespace bustub
