//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  void SetKeyValueAt(int index, const KeyType &key, const ValueType &value);
  void RemoveAt(int index);
  auto HasValue(const KeyType &key, const KeyComparator &copm) const -> bool;
  inline auto LastKey() const -> KeyType { return KeyAt(GetSize() - 1); }
  inline auto LastValue() const -> ValueType { return ValueAt(GetSize() - 1); }
  inline auto RemoveLast() -> void { IncreaseSize(-1); }

  /**
   * @param key the key to search for
   * @param comp the comparator to use
   * @return the first index in the leaf page whose key is not less than the given key
   */
  auto LowerBound(const KeyType &key, const KeyComparator &comp) const -> int;

  /**
   * @param other the other page
   * @param start_index the start index
   * @param end_index the end index
   * @param other_start_index the other start index
   */
  auto MoveRange(B_PLUS_TREE_LEAF_PAGE_TYPE *other, int start_index, int end_index, int other_start_index) -> void;

  /**
   * @param key the key to insert
   * @param value the value to insert
   * @param comp the comparator to use
   */
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comp) -> void;

  /**
   * @param key the key to remove
   * @param comp the comparator to use
   */
  auto Remove(const KeyType &key, const KeyComparator &comp) -> void;

  /**
   * @param key the key to find
   * @param[out] result the value to find
   * @param comp the comparator to use
   * @return true if key exists in the leaf page, false otherwise
   */
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, const KeyComparator &comp) const -> bool;

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
