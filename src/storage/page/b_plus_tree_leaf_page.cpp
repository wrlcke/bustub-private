//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::HasValue(const KeyType &key, const KeyComparator &comp) const -> bool {
  int index = LowerBound(key, comp);
  return index != GetSize() && comp(key, KeyAt(index)) == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LowerBound(const KeyType &key, const KeyComparator &comp) const -> int {
  int left = 0;
  int right = GetSize();
  int mid;
  while (left < right) {
    mid = (left + right) / 2;
    if (comp(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveRange(B_PLUS_TREE_LEAF_PAGE_TYPE *other, int start_index, int end_index,
                                           int other_start_index) -> void {
  if (other_start_index < other->GetSize()) {
    // for (int i = other->GetSize() - 1; i >= other_start_index; --i) {
    //   other->array_[i + end_index - start_index] = other->array_[i];
    // }
    std::move_backward(other->array_ + other_start_index, other->array_ + other->GetSize(),
                       other->array_ + other->GetSize() + end_index - start_index);
  }
  // for (int i = start_index; i < end_index; ++i) {
  //   other->array_[i + other_start_index - start_index] = array_[i];
  // }
  std::move(array_ + start_index, array_ + end_index, other->array_ + other_start_index);
  if (end_index < GetSize()) {
    // for (int i = end_index; i < GetSize(); ++i) {
    //   array_[i - end_index + start_index] = array_[i];
    // }
    std::move(array_ + end_index, array_ + GetSize(), array_ + start_index);
  }
  SetSize(GetSize() - end_index + start_index);
  other->SetSize(other->GetSize() + end_index - start_index);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comp) -> void {
  int index = LowerBound(key, comp);
  if (index < GetSize()) {
    // for (int i = GetSize(); i > index; --i) {
    //   array_[i] = array_[i - 1];
    // }
    std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  }
  array_[index] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comp) -> void {
  int index = LowerBound(key, comp);
  if (index + 1 < GetSize()) {
    // for (int i = index + 1; i < GetSize(); ++i) {
    //   array_[i - 1] = array_[i];
    // }
    std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &comp) const -> bool {
  int index = LowerBound(key, comp);
  if (index == GetSize() || comp(key, KeyAt(index)) != 0) {
    return false;
  }
  result->emplace_back(ValueAt(index));
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
