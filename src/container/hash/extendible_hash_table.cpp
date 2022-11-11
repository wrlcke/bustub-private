//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  HashTableDirectoryPage *dir =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&directory_page_id_, nullptr)->GetData());
  page_id_t first_bucket_page_id;
  buffer_pool_manager->NewPage(&first_bucket_page_id, nullptr);
  dir->SetPageId(directory_page_id_);
  dir->SetBucketPageId(0, first_bucket_page_id);
  dir->SetLocalDepth(0, 0);
  buffer_pool_manager->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager->UnpinPage(first_bucket_page_id, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(
      buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();

  bool succeeded = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  return succeeded;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();

  if (!bucket_page->IsFull()) {
    bool succeeded = bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    return succeeded;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir);
  bucket_idx &= dir->GetLocalDepthMask(bucket_idx);
  page_id_t origin_bucket_page_id = dir->GetBucketPageId(bucket_idx);
  page_id_t split_bucket_page_id;

  Page *origin_page = buffer_pool_manager_->FetchPage(origin_bucket_page_id);
  origin_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *origin_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(origin_page->GetData());

  if (!origin_bucket->IsFull()) {
    origin_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(origin_bucket_page_id, true, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return Insert(transaction, key, value);
  }

  Page *split_page = buffer_pool_manager_->NewPage(&split_bucket_page_id, nullptr);
  split_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_page->GetData());

  if (dir->GetLocalDepth(bucket_idx) == dir->GetGlobalDepth()) {
    dir->IncrGlobalDepth();
  }

  uint32_t size = dir->Size();
  uint32_t step = 0x1 << dir->GetLocalDepth(bucket_idx);
  uint32_t idx_to_have_origin = bucket_idx;
  uint32_t idx_to_have_split = bucket_idx + step;
  uint32_t local_high_bit = dir->GetLocalHighBit(bucket_idx);
  while (idx_to_have_split < size) {
    dir->SetBucketPageId(idx_to_have_split, split_bucket_page_id);
    dir->SetBucketPageId(idx_to_have_origin, origin_bucket_page_id);
    dir->IncrLocalDepth(idx_to_have_split);
    dir->IncrLocalDepth(idx_to_have_origin);
    idx_to_have_origin += step;
    idx_to_have_split += step;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  table_latch_.WUnlock();

  uint32_t origin_kv_idx = 0;
  uint32_t split_kv_idx = 0;
  for (; origin_kv_idx < BUCKET_ARRAY_SIZE; origin_kv_idx++) {
    if (!origin_bucket->IsOccupied(origin_kv_idx)) {
      break;
    }
    if (origin_bucket->IsReadable(origin_kv_idx)) {
      KeyType key_at_idx = std::move(origin_bucket->KeyAt(origin_kv_idx));
      ValueType value_at_idx = std::move(origin_bucket->ValueAt(origin_kv_idx));
      if ((Hash(key_at_idx) & local_high_bit)) {
        origin_bucket->RemoveAt(origin_kv_idx);
        for (; split_kv_idx < BUCKET_ARRAY_SIZE; split_kv_idx++) {
          if (!split_bucket->IsReadable(split_kv_idx)) {
            split_bucket->SetReadable(split_kv_idx, true);
            split_bucket->SetOccupied(split_kv_idx);
            split_bucket->SetKeyValue(split_kv_idx, key_at_idx, value_at_idx);
            break;
          }
        }
      }
    }
  }
  split_page->WUnlatch();
  origin_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(origin_bucket_page_id, true, nullptr);
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir);
  page_id_t bucket_page_id = dir->GetBucketPageId(bucket_idx);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();

  bool succeeded = bucket_page->Remove(key, value, comparator_);
  bool is_empty = bucket_page->IsEmpty();
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, succeeded, nullptr);
  if (is_empty) {
    Merge(transaction, key, value);
  }
  return succeeded;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir);
  bucket_idx &= dir->GetLocalDepthMask(bucket_idx);
  uint32_t local_depth = dir->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return;
  }
  uint32_t merge_bucket_idx = dir->GetPairIndex(bucket_idx);
  if (local_depth != dir->GetLocalDepth(merge_bucket_idx)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return;
  }
  page_id_t empty_bucket_page_id = dir->GetBucketPageId(bucket_idx);
  page_id_t merge_bucket_page_id = dir->GetBucketPageId(merge_bucket_idx);

  Page *empty_bucket_page = buffer_pool_manager_->FetchPage(empty_bucket_page_id, nullptr);
  empty_bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *empty_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(empty_bucket_page->GetData());
  if (!empty_bucket->IsEmpty()) {
    empty_bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(empty_bucket_page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return;
  }
  empty_bucket_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(empty_bucket_page_id, false, nullptr);
  for (uint32_t idx = bucket_idx; idx < dir->Size(); idx += (1 << local_depth)) {
    dir->SetBucketPageId(idx, merge_bucket_page_id);
    dir->DecrLocalDepth(idx);
  }
  for (uint32_t idx = merge_bucket_idx; idx < dir->Size(); idx += (1 << local_depth)) {
    dir->DecrLocalDepth(idx);
  }
  while (dir->CanShrink()) {
    dir->DecrGlobalDepth();
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  table_latch_.WUnlock();
  buffer_pool_manager_->DeletePage(empty_bucket_page_id, nullptr);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
