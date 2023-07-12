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
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return curr_page_id_ == itr.curr_page_id_ && curr_index_ == itr.curr_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return curr_page_id_ != itr.curr_page_id_ || curr_index_ != itr.curr_index_;
  }

  auto Start(BufferPoolManager *bpm, page_id_t start_page_id, int start_index = 0) -> INDEXITERATOR_TYPE;

  auto Start(BufferPoolManager *bpm, ReadPageGuard start_page_guard, int start_index = 0) -> INDEXITERATOR_TYPE;

  auto Terminate() -> INDEXITERATOR_TYPE;

 private:
  // add your own private member variables here

  MappingType key_value_;
  int curr_index_;
  page_id_t curr_page_id_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
