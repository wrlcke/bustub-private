//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> latch(mutex_);
  if (unpinned_list_.empty()) {
    frame_id = nullptr;
    return false;
  }
  *frame_id = unpinned_list_.front();
  iterator_map_.erase(*frame_id);
  unpinned_list_.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(mutex_);
  if (iterator_map_.count(frame_id) != 0) {
    unpinned_list_.erase(iterator_map_[frame_id]);
    iterator_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(mutex_);
  if (iterator_map_.count(frame_id) == 0) {
    unpinned_list_.emplace_back(frame_id);
    iterator_map_[frame_id] = --unpinned_list_.end();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> latch(mutex_);
  return unpinned_list_.size();
}

}  // namespace bustub
