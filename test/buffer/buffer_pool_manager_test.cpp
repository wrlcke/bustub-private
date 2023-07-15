//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, NewPageTest1) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 20;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);
  int test_size = 1000;
  auto gen_str = [](int i) {
    std::string s[5] = {"Hello", "No", "Yes", "Wrong", "Book"};
    std::string str = s[i % 5] + std::to_string(i);
    return str;
  };
  for (int i = 0; i < test_size; ++i) {
    page_id_t page_id_temp;
    auto *page = bpm->NewPage(&page_id_temp);
    EXPECT_NE(nullptr, page);
    EXPECT_EQ(i, page_id_temp);
    snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "%s", gen_str(i).c_str());
    bpm->UnpinPage(i, true);
  }

  for (int i = 0; i < test_size; ++i) {
    auto *page = bpm->FetchPage(i);
    EXPECT_NE(nullptr, page);
    EXPECT_EQ(0, strcmp(page->GetData(), gen_str(i).c_str()));
    bpm->UnpinPage(i, false);
  }
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// using bustub::DiskManagerUnlimitedMemory;

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// // helper function to insert
void NewPageHelper(BufferPoolManager *bpm, int num_pages, __attribute__((unused)) uint64_t thread_itr = 0) {
  auto gen_str = [](int i) {
    std::string s[5] = {"Hello", "No", "Yes", "Wrong", "Book"};
    std::string str = s[i % 5] + std::to_string(i);
    return str;
  };
  for (int i = 0; i < num_pages; ++i) {
    page_id_t page_id_temp;
    auto *page = bpm->NewPage(&page_id_temp);
    EXPECT_NE(nullptr, page);
    snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "%s", gen_str(page_id_temp).c_str());
    bpm->UnpinPage(page_id_temp, true);
  }
}
TEST(BufferPoolManagerTest, NewPageTest2) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 20;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);
  int test_size = 1000;
  int num_per_thread = 100;
  LaunchParallelTest(10, NewPageHelper, bpm, num_per_thread);
  auto gen_str = [](int i) {
    std::string s[5] = {"Hello", "No", "Yes", "Wrong", "Book"};
    std::string str = s[i % 5] + std::to_string(i);
    return str;
  };
  for (int i = 0; i < test_size; ++i) {
    auto *page = bpm->FetchPage(i);
    EXPECT_NE(nullptr, page);
    EXPECT_EQ(0, strcmp(page->GetData(), gen_str(i).c_str()));
    bpm->UnpinPage(i, false);
  }
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
// // helper function to seperate insert
// void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
//                        int total_threads, __attribute__((unused)) uint64_t thread_itr) {
//   GenericKey<8> index_key;
//   RID rid;
//   // create transaction
//   auto *transaction = new Transaction(0);
//   for (auto key : keys) {
//     if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
//       int64_t value = key & 0xFFFFFFFF;
//       rid.Set(static_cast<int32_t>(key >> 32), value);
//       index_key.SetFromInteger(key);
//       tree->Insert(index_key, rid, transaction);
//     }
//   }
//   delete transaction;
// }

// // helper function to delete
// void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
//                   __attribute__((unused)) uint64_t thread_itr = 0) {
//   GenericKey<8> index_key;
//   // create transaction
//   auto *transaction = new Transaction(0);
//   for (auto key : remove_keys) {
//     index_key.SetFromInteger(key);
//     tree->Remove(index_key, transaction);
//   }
//   delete transaction;
// }

// // helper function to seperate delete
// void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
//                        const std::vector<int64_t> &remove_keys, int total_threads,
//                        __attribute__((unused)) uint64_t thread_itr) {
//   GenericKey<8> index_key;
//   // create transaction
//   auto *transaction = new Transaction(0);
//   for (auto key : remove_keys) {
//     if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
//       index_key.SetFromInteger(key);
//       tree->Remove(index_key, transaction);
//     }
//   }
//   delete transaction;
// }

// void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
//                   uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
//   auto *transaction = new Transaction(static_cast<txn_id_t>(tid));
//   GenericKey<8> index_key;
//   RID rid;
//   for (auto key : keys) {
//     int64_t value = key & 0xFFFFFFFF;
//     rid.Set(static_cast<int32_t>(key >> 32), value);
//     index_key.SetFromInteger(key);
//     std::vector<RID> result;
//     bool res = tree->GetValue(index_key, &result, transaction);
//     ASSERT_EQ(res, true);
//     ASSERT_EQ(result.size(), 1);
//     ASSERT_EQ(result[0], rid);
//   }
//   delete transaction;
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest1) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
//   // keys to Insert
//   std::vector<int64_t> keys;
//   int64_t scale_factor = 100;
//   for (int64_t key = 1; key < scale_factor; key++) {
//     keys.push_back(key);
//   }
//   LaunchParallelTest(2, InsertHelper, &tree, keys);

//   std::vector<RID> rids;
//   GenericKey<8> index_key;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(rids[0].GetSlotNum(), value);
//   }

//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//   }

//   EXPECT_EQ(current_key, keys.size() + 1);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());
//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
//   // keys to Insert
//   std::vector<int64_t> keys;
//   int64_t scale_factor = 100;
//   for (int64_t key = 1; key < scale_factor; key++) {
//     keys.push_back(key);
//   }
//   LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

//   std::vector<RID> rids;
//   GenericKey<8> index_key;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     tree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 1);

//     int64_t value = key & 0xFFFFFFFF;
//     EXPECT_EQ(rids[0].GetSlotNum(), value);
//   }

//   int64_t start_key = 1;
//   int64_t current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//   }

//   EXPECT_EQ(current_key, keys.size() + 1);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());

//   GenericKey<8> index_key;
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
//   // sequential insert
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   InsertHelper(&tree, keys);

//   std::vector<int64_t> remove_keys = {1, 5, 3, 4};
//   LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

//   int64_t start_key = 2;
//   int64_t current_key = start_key;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 1);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());
//   GenericKey<8> index_key;
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);

//   // sequential insert
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//   InsertHelper(&tree, keys);

//   std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
//   LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

//   int64_t start_key = 7;
//   int64_t current_key = start_key;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 4);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_MixTest1) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());

//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bpm->NewPage(&page_id);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
//   GenericKey<8> index_key;
//   // first, populate index
//   std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//   InsertHelper(&tree, keys);

//   // concurrent insert
//   keys.clear();
//   for (int i = 6; i <= 10; i++) {
//     keys.push_back(i);
//   }
//   LaunchParallelTest(1, InsertHelper, &tree, keys);
//   // concurrent delete
//   std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
//   LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

//   int64_t start_key = 2;
//   int64_t size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 5);

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_MixTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
//   auto *bpm = new BufferPoolManager(50, disk_manager.get());

//   // create and fetch header_page
//   page_id_t page_id;
//   auto *header_page = bpm->NewPage(&page_id);
//   (void)header_page;

//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

//   // Add perserved_keys
//   std::vector<int64_t> perserved_keys;
//   std::vector<int64_t> dynamic_keys;
//   int64_t total_keys = 50;
//   int64_t sieve = 5;
//   for (int64_t i = 1; i <= total_keys; i++) {
//     if (i % sieve == 0) {
//       perserved_keys.push_back(i);
//     } else {
//       dynamic_keys.push_back(i);
//     }
//   }
//   InsertHelper(&tree, perserved_keys, 1);
//   // Check there are 1000 keys in there
//   size_t size;

//   auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
//   auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
//   auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

//   std::vector<std::thread> threads;
//   std::vector<std::function<void(int)>> tasks;
//   tasks.emplace_back(insert_task);
//   tasks.emplace_back(delete_task);
//   tasks.emplace_back(lookup_task);

//   size_t num_threads = 6;
//   for (size_t i = 0; i < num_threads; i++) {
//     threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
//   }
//   for (size_t i = 0; i < num_threads; i++) {
//     threads[i].join();
//   }

//   // Check all reserved keys exist
//   size = 0;

//   for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
//     const auto &pair = *iter;
//     if ((pair.first).ToString() % sieve == 0) {
//       size++;
//     }
//   }

//   ASSERT_EQ(size, perserved_keys.size());

//   bpm->UnpinPage(HEADER_PAGE_ID, true);
//   delete bpm;
// }

}  // namespace bustub
