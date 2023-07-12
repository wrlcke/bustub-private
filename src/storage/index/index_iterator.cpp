/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return curr_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return key_value_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ReadPageGuard guard = bpm_->FetchPageRead(curr_page_id_);
  const auto *curr_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (curr_index_ < curr_page->GetSize()) {
    key_value_ = {curr_page->KeyAt(curr_index_), curr_page->ValueAt(curr_index_)};
    ++curr_index_;
    return *this;
  }
  curr_page_id_ = curr_page->GetNextPageId();
  curr_index_ = 0;
  if (curr_page_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  // TODO(unknown):  As the hint says, "We do not test your iterator for thread-safe leaf scans.
  // A correct implementation, however, would require the Leaf Page to throw a std::exception
  // when it cannot acquire a latch on its sibling to avoid potential dead-locks."
  // We should use try-lock here to avoid dead-locks.
  // But the page latch does not support try-lock, so I just use lock here.
  // A correct implementation should use try-lock, and if failed to acquire the latch, throw an exception.
  guard = bpm_->FetchPageRead(curr_page_id_);
  curr_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  key_value_ = {curr_page->KeyAt(curr_index_), curr_page->ValueAt(curr_index_)};
  ++curr_index_;
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::Start(BufferPoolManager *bpm, page_id_t start_page_id, int start_index) -> INDEXITERATOR_TYPE {
  bpm_ = bpm;
  curr_page_id_ = start_page_id;
  curr_index_ = start_index;
  ReadPageGuard guard = bpm_->FetchPageRead(curr_page_id_);
  const auto *curr_page = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  key_value_ = {curr_page->KeyAt(curr_index_), curr_page->ValueAt(curr_index_)};
  ++curr_index_;
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::Start(BufferPoolManager *bpm, ReadPageGuard start_page_guard, int start_index)
    -> INDEXITERATOR_TYPE {
  bpm_ = bpm;
  curr_page_id_ = start_page_guard.PageId();
  curr_index_ = start_index;
  const auto *curr_page = start_page_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  key_value_ = {curr_page->KeyAt(curr_index_), curr_page->ValueAt(curr_index_)};
  ++curr_index_;
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::Terminate() -> INDEXITERATOR_TYPE {
  curr_page_id_ = INVALID_PAGE_ID;
  curr_index_ = 0;
  bpm_ = nullptr;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
