//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  frame_meta_latches_ = new std::mutex[pool_size_];
  frame_data_latches_ = new std::mutex[pool_size_];
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] frame_meta_latches_;
  delete[] frame_data_latches_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> latch(latch_);
  frame_id_t frame_id;
  Page *page;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page = pages_ + frame_id;

  std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
  page_table_.erase(page->page_id_);
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  latch.unlock();
  page_id_t old_page_id = page->page_id_;
  bool old_is_dirty = page->is_dirty_;
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  std::unique_lock<std::mutex> frame_data_latch(frame_data_latches_[frame_id]);
  frame_latch.unlock();
  if (old_is_dirty) {
    disk_manager_->WritePage(old_page_id, page->data_);
  }
  page->ResetMemory();
  disk_manager_->WritePage(*page_id, page->data_);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> latch(latch_);
  frame_id_t frame_id;
  Page *page;

  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    page = pages_ + frame_id;
    replacer_->RecordAccess(frame_id, access_type);
    std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    ++page->pin_count_;
    std::unique_lock<std::mutex> frame_data_latch(frame_data_latches_[frame_id]);
    return page;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  page = pages_ + frame_id;

  std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
  page_table_.erase(page->page_id_);
  page_table_[page_id] = frame_id;

  latch.unlock();
  page_id_t old_page_id = page->page_id_;
  bool old_is_dirty = page->is_dirty_;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  std::unique_lock<std::mutex> frame_data_latch(frame_data_latches_[frame_id]);
  frame_latch.unlock();
  if (old_is_dirty) {
    disk_manager_->WritePage(old_page_id, page->data_);
  }
  disk_manager_->ReadPage(page_id, page->data_);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
  if (page->pin_count_ <= 0) {
    return false;
  }
  if (page->pin_count_ == 1) {
    replacer_->SetEvictable(frame_id, true);
  }
  latch.unlock();

  --page->pin_count_;
  page->is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
  std::unique_lock<std::mutex> frame_data_latch(frame_data_latches_[frame_id]);
  latch.unlock();
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> latch(latch_);
  for (auto [page_id, frame_id] : page_table_) {
    std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
    std::unique_lock<std::mutex> frame_data_latch(frame_data_latches_[frame_id]);
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  std::unique_lock<std::mutex> frame_latch(frame_meta_latches_[frame_id]);
  if (page->pin_count_ > 0) {
    return false;
  }
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  latch.unlock();

  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  // page->ResetMemory();
  // disk_manager_->WritePage(page->page_id_, page->data_);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
