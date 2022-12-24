//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsOccupied(idx)) {
      break;
    }
    if (IsReadable(idx) && cmp(array_[idx].first, key) == 0) {
      result->emplace_back(array_[idx].second);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsOccupied(idx)) {
      break;
    }
    if (IsReadable(idx) && cmp(array_[idx].first, key) == 0 && value == array_[idx].second) {
      return false;
    }
  }
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsReadable(idx)) {
      SetReadable(idx, true);
      SetOccupied(idx);
      array_[idx] = {key, value};
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsOccupied(idx)) {
      break;
    }
    if (IsReadable(idx) && cmp(array_[idx].first, key) == 0 && array_[idx].second == value) {
      SetReadable(idx, false);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetReadable(bucket_idx, false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return occupied_[bucket_idx / 8] & (0x1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] |= (0x1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return readable_[bucket_idx / 8] & (0x1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx, bool readable) {
  if (readable) {
    readable_[bucket_idx / 8] |= (0x1 << (bucket_idx % 8));
  } else {
    readable_[bucket_idx / 8] &= ~(0x1 << (bucket_idx % 8));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  int char_array_size = BUCKET_ARRAY_SIZE / 8;
  for (int i = 0; i < char_array_size; i++) {
    if (readable_[i] != '\xff') {
      return false;
    }
  }
  int rest_size = BUCKET_ARRAY_SIZE % 8;
  return rest_size == 0 || readable_[char_array_size] == (1 << rest_size) - 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t num = 0;
  int char_array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  uint8_t temp;
  for (int i = 0; i < char_array_size; i++) {
    if (occupied_[i] == 0) {
      break;
    }
    temp = readable_[i];
    if (temp == 0xff) {
      num += 8;
      continue;
    }
    while (temp != 0) {
      temp &= (temp - 1);
      ++num;
    }
  }
  return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  int char_array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for (int i = 0; i < char_array_size; i++) {
    if (occupied_[i] == 0) {
      return true;
    }
    if (readable_[i] != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::SetKeyValue(uint32_t bucket_idx, KeyType key,
                                                                         ValueType value) {
  array_[bucket_idx] = {key, value};
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
