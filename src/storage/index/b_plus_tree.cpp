#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
#define MYDEBUG1
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
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  BasicPageGuard leaf_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
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
  // Declaration of context instance.
  // Context ctx;
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  // if (header_page->root_page_id_ == INVALID_PAGE_ID) {
  //   return false;
  // }
  guard = bpm_->FetchPageRead(header_page->root_page_id_);
  const auto *internal_page = guard.As<InternalPage>();
  while (!internal_page->IsLeafPage()) {
    page_id_t child_page_id = internal_page->GetValue(key, comparator_);
    guard = bpm_->FetchPageRead(child_page_id);
    internal_page = guard.As<InternalPage>();
  }
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
  // Declaration of context instance.
  // Context ctx;
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
  ReadPageGuard next_guard = bpm_->FetchPageRead(next_page_id);
  const auto *internal_page = next_guard.As<InternalPage>();
#ifdef MYDEBUG
  std::stringstream ss;
  ss << std::this_thread::get_id();
  LOG_DEBUG("thread id: %s inserting Get root page id: %d", ss.str().c_str(), next_page_id);
  // PrintTree(next_page_id, internal_page);
#endif

  while (!internal_page->IsLeafPage()) {
    guard = std::move(next_guard);  // Release parent page guard immediately.
    next_page_id = internal_page->GetValue(key, comparator_);
    next_guard = bpm_->FetchPageRead(next_page_id);
    internal_page = next_guard.As<InternalPage>();
#ifdef MYDEBUG
    if (!internal_page->IsLeafPage()) {
      LOG_DEBUG("thread id: %s inserting Get next internal page id: %d", ss.str().c_str(), next_page_id);
    } else {
      LOG_DEBUG("thread id: %s inserting Get next leaf page id: %d", ss.str().c_str(), next_page_id);
    }
    if (next_page_id == 0) {
      exit(-1);
    }
// PrintTree(next_page_id, internal_page);
#endif
  }
  next_guard.Drop();  // Release the read latch to get the write latch, during which the parent latch can't be released.
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(next_page_id);
  guard.Drop();  // After getting the write latch, release the parent latch.
  auto *leaf_page = leaf_guard.AsMut<LeafPage>();
  if (leaf_page->HasValue(key, comparator_)) {
    return false;
  }
#ifdef MYDEBUG
  LOG_DEBUG("thread id: %s Before Leaf Insert", ss.str().c_str());
#endif
  if (leaf_page->IsFull()) {
    // Another Thread splitting, we should retry
    leaf_guard.Drop();
#ifdef MYDEBUG
    LOG_DEBUG("thread id: %s Insert Retry", ss.str().c_str());
#endif
    return Insert(key, value, txn);
  }
  leaf_page->Insert(key, value, comparator_);
#ifdef MYDEBUG
  LOG_DEBUG("thread id: %s Insert Over", ss.str().c_str());
#endif
  if (leaf_page->IsFull()) {
    // Split the pages.
    leaf_guard.Drop();
#ifdef MYDEBUG
    LOG_DEBUG("thread id: %s Split Start", ss.str().c_str());
#endif
    Split(key, value, txn);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(const KeyType &key, const ValueType &value, Transaction *txn) -> void {
  std::deque<WritePageGuard> write_set{};
  write_set.emplace_back(bpm_->FetchPageWrite(header_page_id_));
  const auto *const_header_page = write_set.back().As<BPlusTreeHeaderPage>();
  write_set.emplace_back(bpm_->FetchPageWrite(const_header_page->root_page_id_));
  const auto *const_internal_page = write_set.back().As<InternalPage>();
#ifdef MYDEBUG
  std::stringstream ss;
  ss << std::this_thread::get_id();
  LOG_DEBUG("thread id: %s Split Down A page id %d", ss.str().c_str(), const_header_page->root_page_id_);
#endif
  while (!const_internal_page->IsLeafPage()) {
    if (!const_internal_page->IsFull()) {
      while (write_set.size() > 1) {
        write_set.pop_front();
      }
    }
    page_id_t child_page_id = const_internal_page->GetValue(key, comparator_);
    write_set.emplace_back(bpm_->FetchPageWrite(child_page_id));
#ifdef MYDEBUG
    LOG_DEBUG("thread id: %s Split Down B page id: %d", ss.str().c_str(), child_page_id);
    if (child_page_id == 0) {
      exit(-1);
    }
#endif
    const_internal_page = write_set.back().As<InternalPage>();
  }
  const auto *const_leaf_page = write_set.back().As<LeafPage>();
  if (!const_leaf_page->IsFull()) {
    write_set.clear();
    return;
  }

  SplitContext ctx{};
  while (!write_set.empty()) {
    WritePageGuard &write_guard = write_set.back();
    if (write_guard.PageId() == header_page_id_) {
#ifdef MYDEBUG
      LOG_DEBUG("thread id: %s Split Get header page id: %d", ss.str().c_str(), write_guard.PageId());
#endif
      SplitHeader(write_guard.AsMut<BPlusTreeHeaderPage>(), &ctx);
    } else if (write_guard.As<BPlusTreePage>()->IsLeafPage()) {
#ifdef MYDEBUG
      LOG_DEBUG("thread id: %s Split Get leaf page id: %d", ss.str().c_str(), write_guard.PageId());
#endif
      SplitLeaf(write_guard.AsMut<LeafPage>(), &ctx);
    } else {
#ifdef MYDEBUG
      LOG_DEBUG("thread id: %s Split Get internal page id: %d", ss.str().c_str(), write_guard.PageId());
#endif
      SplitInternal(write_guard.AsMut<InternalPage>(), &ctx);
// PrintTree(write_guard.PageId(), write_guard.As<InternalPage>());
// PrintTree(ctx.new_page_id_, bpm_->FetchPageRead(ctx.new_page_id_).template As<InternalPage>());
#ifdef MYDEBUG
      auto left_page = write_guard.As<InternalPage>();
      auto lid = write_guard.PageId();
      auto gg = bpm_->FetchPageRead(ctx.new_page_id_);
      auto right_page = gg.template As<InternalPage>();
      auto rid = ctx.new_page_id_;
      void(lid + rid);
      if (!right_page->IsLeafPage()) {
        bool is0 = false;
        for (int i = 0; i < left_page->GetSize(); i++) {
          if (left_page->ValueAt(i) == 0) {
            is0 = true;
            break;
          }
        }
        if (is0) {
          std::string kstr = "[";
          bool first = true;

          // first key of internal page is always invalid
          for (int i = 0; i < left_page->GetSize(); i++) {
            KeyType key = left_page->KeyAt(i);
            if (first) {
              first = false;
            } else {
              kstr.append(",");
            }

            kstr.append("(" + std::to_string(key.ToString()) + " " + std::to_string(left_page->ValueAt(i)) + ")");
          }
          kstr.append("]");
          LOG_DEBUG("thread id: %s invalid left page: %s", ss.str().c_str(), kstr.c_str());
          exit(-1);
        }
        is0 = false;
        for (int i = 0; i < right_page->GetSize(); i++) {
          if (right_page->ValueAt(i) == 0) {
            is0 = true;
            break;
          }
        }
        if (is0) {
          std::string kstr = "[";
          bool first = true;

          // first key of internal page is always invalid
          for (int i = 0; i < right_page->GetSize(); i++) {
            KeyType key = right_page->KeyAt(i);
            if (first) {
              first = false;
            } else {
              kstr.append(",");
            }

            kstr.append("(" + std::to_string(key.ToString()) + " " + std::to_string(left_page->ValueAt(i)) + ")");
          }
          kstr.append("]");
          LOG_DEBUG("thread id: %s invalid right page: %s", ss.str().c_str(), kstr.c_str());
          exit(-1);
        }
      }
#endif
    }
#ifdef MYDEBUG
    LOG_DEBUG("thread id: %s Current Split Turn Over", ss.str().c_str());
#endif
    write_set.pop_back();
  }
#ifdef MYDEBUG
  LOG_DEBUG("thread id: %s Split Over", ss.str().c_str());
#endif
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitHeader(BPlusTreeHeaderPage *header_page, SplitContext *ctx) -> void {
  page_id_t old_child_page_id = header_page->root_page_id_;
  page_id_t new_root_page_id;
  const auto &[new_key, new_child_page_id] = *ctx;
  BasicPageGuard internal_guard = bpm_->NewPageGuarded(&new_root_page_id);
  auto *internal_page = internal_guard.AsMut<InternalPage>();
  internal_page->Init(internal_max_size_);
  internal_page->SetValueAt(0, old_child_page_id);
  internal_page->SetKeyValueAt(1, new_key, new_child_page_id);
  header_page->root_page_id_ = new_root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal_page, SplitContext *ctx) -> void {
  const auto [new_child_key, new_child_page_id] = *ctx;
  if (!internal_page->IsFull()) {
    internal_page->Insert(new_child_key, new_child_page_id, comparator_);
    return;
  }
  auto &[new_key, new_page_id] = *ctx;
  BasicPageGuard new_internal_guard = bpm_->NewPageGuarded(&new_page_id);
  auto *new_internal_page = new_internal_guard.AsMut<InternalPage>();
  new_internal_page->Init(internal_max_size_);

  int mid_index = internal_page->GetMinSize();
  int insert_pos = internal_page->UpperBound(new_child_key, comparator_);

  if (insert_pos < mid_index) {
    internal_page->MoveRange(new_internal_page, mid_index - 1, internal_page->GetSize(), 0);
    internal_page->Insert(new_child_key, new_child_page_id, comparator_);
  } else if (insert_pos == mid_index) {
    internal_page->MoveRange(new_internal_page, mid_index, internal_page->GetSize(), 1);
    new_internal_page->SetKeyValueAt(0, new_child_key, new_child_page_id);
  } else {
    internal_page->MoveRange(new_internal_page, mid_index, internal_page->GetSize(), 0);
    new_internal_page->Insert(new_child_key, new_child_page_id, comparator_);
  }
  new_key = new_internal_page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_page, SplitContext *ctx) -> void {
  auto &[new_key, new_page_id] = *ctx;

  if (!leaf_page->IsFull()) {
    return;
  }
  BasicPageGuard new_leaf_guard = bpm_->NewPageGuarded(&new_page_id);
  auto *new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  leaf_page->MoveRange(new_leaf_page, leaf_page->GetMinSize(), leaf_page->GetMaxSize(), 0);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  new_key = new_leaf_page->KeyAt(0);
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
  // Declaration of context instance.
  // Context ctx;
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  page_id_t next_page_id = root_page_id;
  ReadPageGuard next_guard = bpm_->FetchPageRead(next_page_id);
  const auto *internal_page = next_guard.As<InternalPage>();
  if (internal_page->GetSize() == 0) {
    return;
  }
  while (!internal_page->IsLeafPage()) {
    guard = std::move(next_guard);
    next_page_id = internal_page->GetValue(key, comparator_);
    next_guard = bpm_->FetchPageRead(next_page_id);
    internal_page = next_guard.As<InternalPage>();
  }
  next_guard.Drop();
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(next_page_id);
  guard.Drop();
  auto *leaf_page = leaf_guard.AsMut<LeafPage>();
  if (!leaf_page->HasValue(key, comparator_)) {
    return;
  }
  if (leaf_guard.PageId() != root_page_id && leaf_page->UnderHalfFull()) {
    // Another Thread Merging or Redistributing
    leaf_guard.Drop();
    return Remove(key, txn);
  }
  leaf_page->Remove(key, comparator_);
  if (leaf_guard.PageId() != root_page_id && leaf_page->UnderHalfFull()) {
    // Merge or redistribute.
    leaf_guard.Drop();
    Merge(key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Merge(const KeyType &key, Transaction *txn) -> void {
  std::deque<WritePageGuard> write_set{};
  write_set.emplace_back(bpm_->FetchPageWrite(header_page_id_));
  const auto *const_header_page = write_set.back().As<BPlusTreeHeaderPage>();
  write_set.emplace_back(bpm_->FetchPageWrite(const_header_page->root_page_id_));
  const auto *const_internal_page = write_set.back().As<InternalPage>();
  while (!const_internal_page->IsLeafPage()) {
    if (const_internal_page->OverHalfFull()) {
      while (write_set.size() > 1) {
        write_set.pop_front();
      }
    }
    page_id_t child_page_id = const_internal_page->GetValue(key, comparator_);
    write_set.emplace_back(bpm_->FetchPageWrite(child_page_id));
    const_internal_page = write_set.back().As<InternalPage>();
  }
  const auto *const_leaf_page = write_set.back().As<LeafPage>();
  if (!const_leaf_page->UnderHalfFull()) {
    write_set.clear();
    return;
  }

  MergeContext ctx{{}, {}, {}, MergeContext::Remove};
  while (!write_set.empty() && ctx.op_type_ != MergeContext::Finish) {
    WritePageGuard &write_guard = write_set.back();
    page_id_t deleted_page_id = INVALID_PAGE_ID;
    if (write_guard.PageId() == header_page_id_) {
      write_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = ctx.page_id_;
    } else if (ctx.op_type_ == MergeContext::UpdateKey || write_set.size() <= 1 ||
               (write_set.size() == 2 && write_set.front().PageId() == header_page_id_)) {
      MergeInternal(write_guard.AsMut<InternalPage>(), nullptr, &ctx);
    } else {
      bool is_left_sib;
      WritePageGuard &parent_guard = *-- --write_set.end();
      const auto *parent_page = parent_guard.As<InternalPage>();
      WritePageGuard sib_guard = GetSibling(key, parent_page, &is_left_sib);
      WritePageGuard &left_guard = is_left_sib ? sib_guard : write_guard;
      WritePageGuard &right_guard = is_left_sib ? write_guard : sib_guard;
      ctx.parent_key_ = is_left_sib ? parent_page->KeyAt(parent_page->UpperBound(key, comparator_) - 1)
                                    : parent_page->KeyAt(parent_page->UpperBound(key, comparator_));
      if (write_guard.As<BPlusTreePage>()->IsLeafPage()) {
        MergeLeaf(left_guard.AsMut<LeafPage>(), right_guard.AsMut<LeafPage>(), &ctx);
      } else {
        MergeInternal(left_guard.AsMut<InternalPage>(), right_guard.AsMut<InternalPage>(), &ctx);
      }
      if (ctx.op_type_ == MergeContext::Remove) {
        deleted_page_id = is_left_sib ? write_guard.PageId() : sib_guard.PageId();
      }
    }
    write_set.pop_back();
    if (deleted_page_id != INVALID_PAGE_ID) {
      bpm_->DeletePage(deleted_page_id);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternal(InternalPage *left_page, InternalPage *right_page, MergeContext *ctx) -> void {
  if (ctx->op_type_ == MergeContext::UpdateKey) {
    left_page->SetKeyAt(left_page->UpperBound(ctx->key_, comparator_) - 1, ctx->key_);
    ctx->op_type_ = MergeContext::Finish;
    return;
  }
  if (right_page == nullptr) {
    left_page->Remove(ctx->key_, comparator_);
    if (left_page->GetSize() == 1) {
      ctx->page_id_ = left_page->ValueAt(0);
      ctx->op_type_ = MergeContext::UpdateRoot;
    }
    return;
  }
  if (left_page->OverHalfFull()) {
    right_page->Insert(left_page->LastKey(), left_page->LastValue(), comparator_);
    left_page->RemoveLast();
    ctx->op_type_ = MergeContext::UpdateKey;
    ctx->key_ = right_page->KeyAt(0);
    return;
  }
  if (right_page->OverHalfFull()) {
    left_page->SetKeyValueAt(left_page->GetSize(), ctx->parent_key_, right_page->ValueAt(0));
    right_page->RemoveAt(0);
    ctx->op_type_ = MergeContext::UpdateKey;
    ctx->key_ = right_page->KeyAt(0);
    return;
  }
  right_page->SetKeyAt(0, ctx->parent_key_);
  right_page->MoveRange(left_page, 0, right_page->GetSize(), left_page->GetSize());
  ctx->op_type_ = MergeContext::Remove;
  ctx->key_ = ctx->parent_key_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaf(LeafPage *left_page, LeafPage *right_page, MergeContext *ctx) -> void {
  if (left_page->OverHalfFull()) {
    right_page->Insert(left_page->LastKey(), left_page->LastValue(), comparator_);
    left_page->RemoveLast();
    ctx->op_type_ = MergeContext::UpdateKey;
    ctx->key_ = right_page->KeyAt(0);
    return;
  }
  if (right_page->OverHalfFull()) {
    left_page->SetKeyValueAt(left_page->GetSize(), right_page->KeyAt(0), right_page->ValueAt(0));
    right_page->RemoveAt(0);
    ctx->op_type_ = MergeContext::UpdateKey;
    ctx->key_ = right_page->KeyAt(0);
    return;
  }
  right_page->MoveRange(left_page, 0, right_page->GetSize(), left_page->GetSize());
  left_page->SetNextPageId(right_page->GetNextPageId());
  ctx->op_type_ = MergeContext::Remove;
  ctx->key_ = right_page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSibling(const KeyType &key, const InternalPage *parent_page, bool *is_left_sib)
    -> WritePageGuard {
  int index = parent_page->UpperBound(key, comparator_) - 1;
  if (index == parent_page->GetSize() - 1) {
    *is_left_sib = true;
    return bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
  }
  if (index == 0) {
    *is_left_sib = false;
    return bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
  }
  WritePageGuard left_sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
  WritePageGuard right_sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
  if (left_sibling_guard.As<BPlusTreePage>()->GetSize() > right_sibling_guard.As<BPlusTreePage>()->GetSize()) {
    *is_left_sib = true;
    return left_sibling_guard;
  }
  *is_left_sib = false;
  return right_sibling_guard;
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
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
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
