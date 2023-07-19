#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  // WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  // auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  // root_page->root_page_id_ = INVALID_PAGE_ID;
  // header_page->tree_depth_ = 0;
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  BasicPageGuard leaf_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
  header_page->tree_depth_ = 1;
  auto *leaf_page = leaf_guard.AsMut<LeafPage>();
  leaf_page->Init(leaf_max_size_);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // return GetRootPageId() == INVALID_PAGE_ID;
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  guard = bpm_->FetchPageBasic(header_page->root_page_id_);
  const auto *root_page = guard.As<BPlusTreePage>();
  return root_page->GetSize() == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  page_id_t next_page_id = header_page->root_page_id_;
  int distance_to_leaf = header_page->tree_depth_;
  while (--distance_to_leaf > 0) {
    guard = bpm_->FetchPageRead(next_page_id);
    next_page_id = guard.As<InternalPage>()->GetValue(key, comparator_);
  }
  guard = bpm_->FetchPageRead(next_page_id);
  const auto *leaf_page = guard.As<LeafPage>();
  return leaf_page->GetValue(key, result, comparator_);
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  page_id_t next_page_id = header_page->root_page_id_;
  // while (next_page_id == INVALID_PAGE_ID) {
  //   guard.Drop();
  //   NewLeafAsRoot();
  //   guard = bpm_->FetchPageRead(header_page_id_);
  //   header_page = guard.As<BPlusTreeHeaderPage>();
  //   next_page_id = header_page->root_page_id_;
  // }
  int distance_to_leaf = header_page->tree_depth_;
  while (--distance_to_leaf > 0) {
    guard = bpm_->FetchPageRead(next_page_id);
    next_page_id = guard.As<InternalPage>()->GetValue(key, comparator_);
  }
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(next_page_id);
  guard.Drop();
  const auto *const_leaf_page = leaf_guard.As<LeafPage>();
  if (const_leaf_page->HasValue(key, comparator_)) {
    return false;
  }
  if (const_leaf_page->GetSize() + 1 < const_leaf_page->GetMaxSize()) {
    auto *leaf_page = leaf_guard.AsMut<LeafPage>();
    leaf_page->Insert(key, value, comparator_);
    return true;
  }
  // Split the pages.
  leaf_guard.Drop();
  return SplitInsert(key, value, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  std::deque<WritePageGuard> write_set{};
  write_set.emplace_front(bpm_->FetchPageWrite(header_page_id_));
  const auto root_page_id = write_set.front().As<BPlusTreeHeaderPage>()->root_page_id_;
  write_set.emplace_front(bpm_->FetchPageWrite(root_page_id));
  const auto *const_internal_page = write_set.front().As<InternalPage>();
  while (!const_internal_page->IsLeafPage()) {
    if (!const_internal_page->IsFull()) {
      write_set.resize(1);
    }
    const page_id_t next_page_id = const_internal_page->GetValue(key, comparator_);
    write_set.emplace_front(bpm_->FetchPageWrite(next_page_id));
    const_internal_page = write_set.front().As<InternalPage>();
  }
  const auto *const_leaf_page = write_set.front().As<LeafPage>();
  if (const_leaf_page->HasValue(key, comparator_)) {
    return false;
  }
  auto *leaf_page = write_set.front().AsMut<LeafPage>();
  leaf_page->Insert(key, value, comparator_);
  if (!leaf_page->IsFull()) {
    return true;
  }
  SplitContext ctx{{}, {}, root_page_id, &write_set};
  while (!write_set.empty()) {
    WritePageGuard &write_guard = write_set.front();
    if (write_guard.PageId() == header_page_id_) {
      SplitHeader(write_guard.AsMut<BPlusTreeHeaderPage>(), &ctx);
    } else if (write_guard.As<BPlusTreePage>()->IsLeafPage()) {
      SplitLeaf(write_guard.AsMut<LeafPage>(), &ctx);
    } else {
      SplitInternal(write_guard.AsMut<InternalPage>(), &ctx);
    }
    if (!write_set.empty()) {
      write_set.pop_front();
    }
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitHeader(BPlusTree::BPlusTreeHeaderPage *header_page, SplitContext *ctx) -> void {
  const page_id_t left_child = header_page->root_page_id_;
  const page_id_t right_child = ctx->new_page_id_;
  const KeyType &key = ctx->new_key_;
  page_id_t new_root_page_id;

  BasicPageGuard internal_guard = bpm_->NewPageGuarded(&new_root_page_id);
  auto *internal_page = internal_guard.AsMut<InternalPage>();
  internal_page->Init(internal_max_size_);
  internal_page->SetValueAt(0, left_child);
  internal_page->SetKeyValueAt(1, key, right_child);
  header_page->root_page_id_ = new_root_page_id;
  ++header_page->tree_depth_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal_page, SplitContext *ctx) -> void {
  const KeyType &key = ctx->new_key_;
  const page_id_t child = ctx->new_page_id_;
  if (!internal_page->IsFull()) {
    internal_page->Insert(key, child, comparator_);
    return;
  }
  if (!ctx->IsFrontPageRoot()) {
    auto *parent_page = ctx->GetFrontPageParent().template AsMut<InternalPage>();
    int index = parent_page->UpperBound(internal_page->KeyAt(0), comparator_) - 1;
    WritePageGuard left_sib_guard;
    WritePageGuard right_sib_guard;
    const InternalPage *const_left_page = nullptr;  // using const_left_sib_page can't pass clang-tidy, ****
    const InternalPage *const_right_sib_page = nullptr;
    if (index < parent_page->GetSize() - 1) {
      right_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
      const_right_sib_page = right_sib_guard.As<InternalPage>();
    }
    if (index > 0) {
      left_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
      const_left_page = left_sib_guard.As<InternalPage>();
    }
    if (CanRedistribute(const_left_page, internal_page)) {
      ShiftRightToLeft(left_sib_guard.AsMut<InternalPage>(), internal_page);
      if (comparator_(key, internal_page->KeyAt(0)) < 0) {
        left_sib_guard.AsMut<InternalPage>()->Insert(key, child, comparator_);
      } else {
        internal_page->InsertFromZero(key, child, comparator_);
      }
      parent_page->SetKeyAt(index, internal_page->KeyAt(0));
      ctx->Finish();
      return;
    }
    if (CanRedistribute(internal_page, const_right_sib_page)) {
      ShiftLeftToRight(internal_page, right_sib_guard.AsMut<InternalPage>());
      if (comparator_(key, right_sib_guard.AsMut<InternalPage>()->KeyAt(0)) < 0) {
        internal_page->Insert(key, child, comparator_);
      } else {
        right_sib_guard.AsMut<InternalPage>()->InsertFromZero(key, child, comparator_);
      }
      parent_page->SetKeyAt(index + 1, const_right_sib_page->KeyAt(0));
      ctx->Finish();
      return;
    }
  }
  page_id_t new_page_id;
  BasicPageGuard new_internal_guard = bpm_->NewPageGuarded(&new_page_id);
  auto *new_internal_page = new_internal_guard.AsMut<InternalPage>();
  new_internal_page->Init(internal_max_size_);

  int mid_index = internal_page->GetMinSize();
  int insert_pos = internal_page->UpperBound(key, comparator_);

  if (insert_pos < mid_index) {
    internal_page->Range(mid_index - 1, -1) >> new_internal_page;
    internal_page->Insert(key, child, comparator_);
  } else {
    internal_page->Range(mid_index, -1) >> new_internal_page;
    new_internal_page->InsertFromZero(key, child, comparator_);
  }
  ctx->new_key_ = new_internal_page->KeyAt(0);
  ctx->new_page_id_ = new_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_page, SplitContext *ctx) -> void {
  if (!ctx->IsFrontPageRoot()) {
    auto *parent_page = ctx->GetFrontPageParent().template AsMut<InternalPage>();
    int index = parent_page->UpperBound(leaf_page->KeyAt(0), comparator_) - 1;
    WritePageGuard left_sib_guard;
    WritePageGuard right_sib_guard;
    const LeafPage *const_left_page = nullptr;  // using const_left_sib_page can't pass clang-tidy, ****
    const LeafPage *const_right_sib_page = nullptr;
    if (index < parent_page->GetSize() - 1) {
      right_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
      const_right_sib_page = right_sib_guard.As<LeafPage>();
    }
    if (index > 0) {
      left_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
      const_left_page = left_sib_guard.As<LeafPage>();
    }
    if (CanRedistribute(const_left_page, leaf_page)) {
      ShiftRightToLeft(left_sib_guard.AsMut<LeafPage>(), leaf_page);
      parent_page->SetKeyAt(index, leaf_page->KeyAt(0));
      ctx->Finish();
      return;
    }
    if (CanRedistribute(leaf_page, const_right_sib_page)) {
      ShiftLeftToRight(leaf_page, right_sib_guard.AsMut<LeafPage>());
      parent_page->SetKeyAt(index + 1, const_right_sib_page->KeyAt(0));
      ctx->Finish();
      return;
    }
  }
  page_id_t new_page_id;
  BasicPageGuard new_leaf_guard = bpm_->NewPageGuarded(&new_page_id);
  auto *new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  leaf_page->Range(leaf_page->GetMinSize(), -1) >> new_leaf_page;
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  ctx->new_key_ = new_leaf_page->KeyAt(0);
  ctx->new_page_id_ = new_page_id;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  page_id_t next_page_id = header_page->root_page_id_;
  int distance_to_leaf = header_page->tree_depth_;
  while (--distance_to_leaf > 0) {
    guard = bpm_->FetchPageRead(next_page_id);
    next_page_id = guard.As<InternalPage>()->GetValue(key, comparator_);
  }
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(next_page_id);
  guard.Drop();
  const auto *const_leaf_page = leaf_guard.As<LeafPage>();
  if (!const_leaf_page->HasValue(key, comparator_)) {
    return;
  }
  if (const_leaf_page->OverHalfFull() || leaf_guard.PageId() == root_page_id) {
    auto *leaf_page = leaf_guard.AsMut<LeafPage>();
    leaf_page->Remove(key, comparator_);
    return;
  }
  // Merge or redistribute.
  leaf_guard.Drop();
  MergeRemove(key, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeRemove(const KeyType &key, Transaction *txn) -> void {
  std::deque<WritePageGuard> write_set{};
  write_set.emplace_front(bpm_->FetchPageWrite(header_page_id_));
  const auto root_page_id = write_set.front().As<BPlusTreeHeaderPage>()->root_page_id_;
  write_set.emplace_front(bpm_->FetchPageWrite(root_page_id));
  const auto *const_internal_page = write_set.front().As<InternalPage>();
  while (!const_internal_page->IsLeafPage()) {
    if (const_internal_page->OverHalfFull()) {
      write_set.resize(1);
    }
    auto next_page_id = const_internal_page->GetValue(key, comparator_);
    write_set.emplace_front(bpm_->FetchPageWrite(next_page_id));
    const_internal_page = write_set.front().As<InternalPage>();
  }
  const auto *const_leaf_page = write_set.front().As<LeafPage>();
  if (!const_leaf_page->HasValue(key, comparator_)) {
    return;
  }
  auto *leaf_page = write_set.front().AsMut<LeafPage>();
  leaf_page->Remove(key, comparator_);
  if (!const_leaf_page->UnderHalfFull() || write_set.front().PageId() == root_page_id) {
    return;
  }

  MergeContext ctx{{}, root_page_id, &write_set};
  while (!write_set.empty()) {
    WritePageGuard &write_guard = write_set.front();
    if (write_guard.As<BPlusTreePage>()->IsLeafPage()) {
      MergeLeaf(write_guard.AsMut<LeafPage>(), &ctx);
    } else {
      MergeInternal(write_guard.AsMut<InternalPage>(), &ctx);
    }
    if (!write_set.empty()) {
      write_set.pop_front();
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternal(InternalPage *internal_page, MergeContext *ctx) -> void {
  internal_page->Remove(ctx->delete_key_, comparator_);
  if (!internal_page->UnderHalfFull()) {
    return;
  }
  if (ctx->IsFrontPageRoot()) {
    if (internal_page->GetSize() > 1) {
      ctx->Finish();
      return;
    }
    auto *header_page = ctx->GetFrontPageParent().template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = internal_page->ValueAt(0);
    --header_page->tree_depth_;
    ctx->Finish();
    return;
  }
  auto *parent_page = ctx->GetFrontPageParent().template AsMut<InternalPage>();
  int index = parent_page->UpperBound(internal_page->KeyAt(0), comparator_) - 1;
  WritePageGuard left_sib_guard;
  WritePageGuard right_sib_guard;
  const InternalPage *const_left_page = nullptr;  // using const_left_sib_page can't pass clang-tidy, ****
  const InternalPage *const_right_sib_page = nullptr;
  if (index < parent_page->GetSize() - 1) {
    right_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
    const_right_sib_page = right_sib_guard.As<InternalPage>();
  }
  if (index > 0) {
    left_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
    const_left_page = left_sib_guard.As<InternalPage>();
  }
  if (CanRedistribute(internal_page, const_right_sib_page)) {
    ShiftRightToLeft(internal_page, right_sib_guard.AsMut<InternalPage>());
    parent_page->SetKeyAt(index + 1, const_right_sib_page->KeyAt(0));
    ctx->Finish();
    return;
  }
  if (CanRedistribute(const_left_page, internal_page)) {
    ShiftLeftToRight(left_sib_guard.AsMut<InternalPage>(), internal_page);
    parent_page->SetKeyAt(index, internal_page->KeyAt(0));
    ctx->Finish();
    return;
  }
  InternalPage *left_page;
  InternalPage *right_page;
  if (const_right_sib_page != nullptr) {
    left_sib_guard.Drop();
    right_sib_guard.SetDead();
    left_page = internal_page;
    right_page = right_sib_guard.AsMut<InternalPage>();
  } else {
    right_sib_guard.Drop();
    ctx->GetFrontPage().SetDead();  //!!!
    left_page = left_sib_guard.AsMut<InternalPage>();
    right_page = internal_page;
  }
  ctx->delete_key_ = right_page->KeyAt(0);
  left_page << right_page->Range(0, -1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaf(LeafPage *leaf_page, MergeContext *ctx) -> void {
  auto *parent_page = ctx->GetFrontPageParent().template AsMut<InternalPage>();
  int index = parent_page->UpperBound(leaf_page->KeyAt(0), comparator_) - 1;
  WritePageGuard left_sib_guard;
  WritePageGuard right_sib_guard;
  const LeafPage *const_left_page = nullptr;  // using const_left_sib_page can't pass clang-tidy, ****
  const LeafPage *const_right_sib_page = nullptr;
  if (index < parent_page->GetSize() - 1) {
    right_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
    const_right_sib_page = right_sib_guard.As<LeafPage>();
  }
  if (index > 0) {
    left_sib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
    const_left_page = left_sib_guard.As<LeafPage>();
  }
  if (CanRedistribute(leaf_page, const_right_sib_page)) {
    ShiftRightToLeft(leaf_page, right_sib_guard.AsMut<LeafPage>());
    parent_page->SetKeyAt(index + 1, const_right_sib_page->KeyAt(0));
    ctx->Finish();
    return;
  }
  if (CanRedistribute(const_left_page, leaf_page)) {
    ShiftLeftToRight(left_sib_guard.AsMut<LeafPage>(), leaf_page);
    parent_page->SetKeyAt(index, leaf_page->KeyAt(0));
    ctx->Finish();
    return;
  }
  LeafPage *left_page;
  LeafPage *right_page;
  if (const_right_sib_page != nullptr) {
    left_sib_guard.Drop();
    right_sib_guard.SetDead();
    left_page = leaf_page;
    right_page = right_sib_guard.AsMut<LeafPage>();
  } else {
    right_sib_guard.Drop();
    ctx->GetFrontPage().SetDead();  //!!!
    left_page = left_sib_guard.AsMut<LeafPage>();
    right_page = leaf_page;
  }
  ctx->delete_key_ = right_page->KeyAt(0);
  left_page << right_page->Range(0, -1);
  left_page->SetNextPageId(right_page->GetNextPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CanRedistribute(const BPlusTreePage *left_page, const BPlusTreePage *right_page) -> bool {
  return left_page != nullptr && right_page != nullptr &&
         left_page->GetSize() + right_page->GetSize() >= left_page->GetMinSize() * 2 &&
         left_page->GetSize() + right_page->GetSize() < left_page->GetMaxSize() * 2 * 95 / 100;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftLeftToRight(InternalPage *left_page, InternalPage *right_page) -> void {
  int left_size = left_page->GetSize();
  int right_size = right_page->GetSize();
  int shift_size = (left_size + right_size) / 2 - right_size;
  left_page->Range(left_size - shift_size, -1) >> right_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftRightToLeft(InternalPage *left_page, InternalPage *right_page) -> void {
  int left_size = left_page->GetSize();
  int right_size = right_page->GetSize();
  int shift_size = (left_size + right_size) / 2 - left_size;
  left_page << right_page->Range(0, shift_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftLeftToRight(LeafPage *left_page, LeafPage *right_page) -> void {
  int left_size = left_page->GetSize();
  int right_size = right_page->GetSize();
  int shift_size = (left_size + right_size) / 2 - right_size;
  left_page->Range(left_size - shift_size, -1) >> right_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftRightToLeft(LeafPage *left_page, LeafPage *right_page) -> void {
  int left_size = left_page->GetSize();
  int right_size = right_page->GetSize();
  int shift_size = (left_size + right_size) / 2 - left_size;
  left_page << right_page->Range(0, shift_size);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  // if (header_page->root_page_id_ == INVALID_PAGE_ID) {
  //   return INDEXITERATOR_TYPE().Terminate();
  // }

  guard = bpm_->FetchPageRead(header_page->root_page_id_);
  const auto *internal_page = guard.As<InternalPage>();
  while (!internal_page->IsLeafPage()) {
    page_id_t child_page_id = internal_page->ValueAt(0);
    guard = bpm_->FetchPageRead(child_page_id);
    internal_page = guard.As<InternalPage>();
  }
  return INDEXITERATOR_TYPE().Start(bpm_, std::move(guard));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  // if (header_page->root_page_id_ == INVALID_PAGE_ID) {
  //   return INDEXITERATOR_TYPE().Terminate();
  // }

  guard = bpm_->FetchPageRead(header_page->root_page_id_);
  const auto *internal_page = guard.As<InternalPage>();
  while (!internal_page->IsLeafPage()) {
    page_id_t child_page_id = internal_page->GetValue(key, comparator_);
    guard = bpm_->FetchPageRead(child_page_id);
    internal_page = guard.As<InternalPage>();
  }
  int index = guard.As<LeafPage>()->LowerBound(key, comparator_);
  return INDEXITERATOR_TYPE().Start(bpm_, std::move(guard), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE().Terminate(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  const auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::NewLeafAsRoot() -> void {
//   WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
//   auto *header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
//   if (header_page->root_page_id_ != INVALID_PAGE_ID) {
//     return;
//   }
//   BasicPageGuard leaf_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
//   auto *leaf_page = leaf_guard.AsMut<LeafPage>();
//   leaf_page->Init(leaf_max_size_);
// }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
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
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "}\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
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
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
