//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // replace with your own code
  array_[index].first = key;
  SetSize(std::max(GetSize(), index + 1));
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // replace with your own code
  array_[index].second = value;
  SetSize(std::max(GetSize(), index + 1));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  // replace with your own code
  array_[index].first = key;
  array_[index].second = value;
  SetSize(std::max(GetSize(), index + 1));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
  if (index + 1 < GetSize()) {
    // for (int i = index; i < GetSize() - 1; i++) {
    //   array_[i] = array_[i + 1];
    // }
    std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &comp) const -> int {
  int left = 1;
  int right = GetSize();
  int mid;
  while (left < right) {
    mid = (left + right) / 2;
    if (comp(array_[mid].first, key) <= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveRange(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other, int start_index, int end_index,
                                               int other_start_index) -> void {
  std::move(array_ + start_index, array_ + end_index, other->array_ + other_start_index);
  SetSize(start_index);
  other->SetSize(other_start_index + end_index - start_index);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comp)
    -> void {
  int index = UpperBound(key, comp);
  if (index < GetSize()) {
    // for (int i = GetSize(); i > index; i--) {
    //   array_[i] = array_[i - 1];
    // }
    std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  }
  array_[index] = {key, value};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comp) -> void {
  int index = UpperBound(key, comp) - 1;
  if (index + 1 < GetSize()) {
    // for (int i = index; i < GetSize() - 1; i++) {
    //   array_[i] = array_[i + 1];
    // }
    std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comp) const -> ValueType {
  return array_[UpperBound(key, comp) - 1].second;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
