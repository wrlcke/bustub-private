/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

template <typename KeyType>
class BPlusTreeSplitContext {
 public:
  KeyType new_key_;
  page_id_t new_page_id_;
  const page_id_t root_page_id_;
  std::deque<WritePageGuard> *write_set_;

  inline auto IsFrontPageRoot() -> bool { return write_set_->front().PageId() == root_page_id_; }
  inline auto GetFrontPageParent() -> WritePageGuard & { return *++write_set_->begin(); }
  inline auto Finish() -> void { write_set_->clear(); }
};

template <typename KeyType>
class BPlusTreeMergeContext {
 public:
  KeyType delete_key_;
  const page_id_t root_page_id_;
  std::deque<WritePageGuard> *write_set_;

  inline auto IsFrontPageRoot() -> bool { return write_set_->front().PageId() == root_page_id_; }
  inline auto GetFrontPageParent() -> WritePageGuard & { return *++write_set_->begin(); }
  inline auto GetFrontPage() -> WritePageGuard & { return write_set_->front(); }
  inline auto Finish() -> void { write_set_->clear(); }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using SplitContext = BPlusTreeSplitContext<KeyType>;
  using MergeContext = BPlusTreeMergeContext<KeyType>;
  class BPlusTreeHeaderPage;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  ~BPlusTree() { PrintNumMetric(); }

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *txn);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() const -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // auto NewLeafAsRoot() -> void;

  /**
   * Split the leaf page which contains the key, and restructur the tree.
   * 1. Traverse down the tree to find the target leaf page,
   *  and store the pages that you need to modify (full) on the path in a write set.
   * 2. Travese reversely along the write set, split each page,
   *  storing the new key and page ID in the split context to pass it to parent.
   * 2.1 During the reverse traversal, all pages except the front one in the set should be split
   *  because the initial traversal ensured that all pages on the path are full except the first one.
   *  Traverse the set and perform different splitting based on page types, and naturally exit the loop.
   * 2.2 For the header page, which can only be at the front of the write set (the end of the reverse traversal),
   *  create a new internal page. Initialize it with the old root page ID and the new page ID
   *  with the new key, and set the new root page ID to be the ID of the created internal page.
   * 2.3 For internal pages, receive the key and page ID in the split context from the child page.
   *  If the internal page is not full (which is the front of the write set), simply insert the key
   *  and page ID. Otherwise, split the page and set the new key and page ID in the split context.
   * 2.4 For leaf pages, they will always be at the back of the write set (the beginning of the reverse traversal).
   *  Create a new leaf page, split it, set the new page, and fill the context.
   */
  auto SplitInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool;
  auto SplitHeader(BPlusTreeHeaderPage *page, SplitContext *ctx) -> void;
  auto SplitInternal(InternalPage *page, SplitContext *ctx) -> void;
  auto SplitLeaf(LeafPage *page, SplitContext *ctx) -> void;

  /** Merge or redistribute the leaf page containg the key, and restructur the tree.
   * 1. Traverse down the tree to find the target leaf page,
   *  and store the pages that you need to modify (less than or equals to half full) on the path in a write set.
   * 2. Travese reversely along the write set, merge or redistribute each page,
   *  storing the new key or new root page id in the merge context to pass it to parent.
   * 2.1 Starting from the back of the write set and moving backwards,
   *  each page should choose either to merge with its sibling (removing one key from the parent and continue iteration)
   *  or to redistribute with its sibling (updating one key from the parent and exit the loop).
   *  There are four types of cases: leaf page, internal page, root page, and header page.
   * 2.2 For leaf and internal pages that are not at the beginning (end of the reverse traversal) of the write set,
   *  compare the sizes of their siblings and choose the larger one for merging or redistribution.
   *  Pass the key that needs to be deleted or updated to the parent.
   * 2.3 For the leaf pages at the beginning of the write set, which means it is a root leaf page
   *  that is impossible, because after deleting and before merging we have checked the leaf page is
   *  not the root page and is less than half full, so its parent internal page must be in the write set before it.
   * 2.4 For the internal pages at the beginning of the write set, which means it is over half full.
   *  Simply delete or update the key from the context.
   * 2.5 For the root page, which can only be an internal page, it can occupy either the first position
   *  or the second position in the write set.
   *  If it is in the first position, it indicates that the root page is over half full, and the handling is
   *  the same as described in 2.4.
   *  If it is in the second position, it can only be right after the header page and be less than half full.
   *  However, it cannot be merged or redistributed with its sibling. Delete or update the key from the context.
   *  If the size of the root page is more than one, we exit the process.
   *  Otherwise, we pass the only element, which is a page ID, as the new root page ID to the context.
   * 2.6 For header page which can only be in the front of the write set, update the root page id from the context.
   */
  auto MergeRemove(const KeyType &key, Transaction *txn) -> void;
  auto MergeInternal(InternalPage *internal_page, MergeContext *ctx) -> void;
  auto MergeLeaf(LeafPage *leaf_page, MergeContext *ctx) -> void;
  auto CanRedistribute(const BPlusTreePage *left_page, const BPlusTreePage *right_page) -> bool;
  auto ShiftLeftToRight(InternalPage *left_page, InternalPage *right_page) -> void;
  auto ShiftRightToLeft(InternalPage *left_page, InternalPage *right_page) -> void;
  auto ShiftLeftToRight(LeafPage *left_page, LeafPage *right_page) -> void;
  auto ShiftRightToLeft(LeafPage *left_page, LeafPage *right_page) -> void;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;

  class BPlusTreeHeaderPage {
   public:
    // Delete all constructor / destructor to ensure memory safety
    BPlusTreeHeaderPage() = delete;
    BPlusTreeHeaderPage(const BPlusTreeHeaderPage &other) = delete;

    page_id_t root_page_id_;
    int32_t tree_depth_;
  };

 public:
  std::atomic_bool restart_metric_{true};
  std::atomic_int read_num_{0};
  std::atomic_int insert_num_{0};
  std::atomic_int insert_duplicate_num_{0};
  std::atomic_int insert_redistribute_num_{0};
  std::atomic_int split_num_{0};
  std::atomic_int remove_num_{0};
  std::atomic_int remove_notfound_num_{0};
  std::atomic_int remove_redistribute_num_{0};
  std::atomic_int merge_num_{0};
  void PrintNumMetric() {
    // std::cout << "read_num: " << read_num_ << std::endl;
    // std::cout << "insert_num: " << insert_num_ << std::endl;
    // std::cout << "insert_duplicate_num: " << insert_duplicate_num_ << std::endl;
    // std::cout << "insert_redistribute_num: " << insert_redistribute_num_ << std::endl;
    // std::cout << "split_num: " << split_num_ << std::endl;
    // std::cout << "remove_num: " << remove_num_ << std::endl;
    // std::cout << "remove_notfound_num: " << remove_notfound_num_ << std::endl;
    // std::cout << "remove_redistribute_num: " << remove_redistribute_num_ << std::endl;
    // std::cout << "merge_num: " << merge_num_ << std::endl;
    read_num_ = 0;
    insert_num_ = 0;
    insert_duplicate_num_ = 0;
    insert_redistribute_num_ = 0;
    split_num_ = 0;
    remove_num_ = 0;
    remove_notfound_num_ = 0;
    remove_redistribute_num_ = 0;
    merge_num_ = 0;
  }
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
