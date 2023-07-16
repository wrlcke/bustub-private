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
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager), disk_scheduler_(disk_manager) {
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
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

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

  page_table_.erase(page->page_id_);
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  page_id_t old_page_id = page->page_id_;
  bool old_is_dirty = page->is_dirty_;
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  if (old_is_dirty) {
    disk_scheduler_.SubmitWrite(old_page_id, page->data_);
  }
  page->ResetMemory();
  disk_scheduler_.SubmitWrite(*page_id, page->data_);
  latch.unlock();

  disk_scheduler_.ExecuteWriteAsync(*page_id);
  if (old_is_dirty) {
    disk_scheduler_.ExecuteWriteAsync(old_page_id);
  }
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
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    ++page->pin_count_;
    latch.unlock();
    disk_scheduler_.CheckPageLoaded(page_id);
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

  page_table_.erase(page->page_id_);
  page_table_[page_id] = frame_id;

  page_id_t old_page_id = page->page_id_;
  bool old_is_dirty = page->is_dirty_;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  if (old_is_dirty) {
    disk_scheduler_.SubmitWrite(old_page_id, page->data_);
  }
  disk_scheduler_.SubmitRead(page_id, page->data_);
  latch.unlock();

  disk_scheduler_.ExecuteRead(page_id);
  if (old_is_dirty) {
    disk_scheduler_.ExecuteWriteAsync(old_page_id);
  }
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  if (page->pin_count_ <= 0) {
    return false;
  }
  if (page->pin_count_ == 1) {
    replacer_->SetEvictable(frame_id, true);
  }

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

  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> latch(latch_);
  for (auto [page_id, frame_id] : page_table_) {
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

  if (page->pin_count_ > 0) {
    return false;
  }
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);

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

void DiskScheduler::SubmitRead(page_id_t page_id, char *page_data) {
  std::unique_lock<std::shared_mutex> latch(schedule_latch_);
  DiskRequest &request = disk_requests_[page_id];
  latch.unlock();
  std::unique_lock<std::mutex> request_latch(request.request_latch_);
  request.page_id_ = page_id;
  request.need_read_ = true;
  request.read_page_data_ = page_data;
}

void DiskScheduler::SubmitWrite(page_id_t page_id, const char *page_data) {
  std::unique_lock<std::shared_mutex> latch(schedule_latch_);
  DiskRequest &request = disk_requests_[page_id];
  latch.unlock();
  std::unique_lock<std::mutex> request_latch(request.request_latch_);
  delete[] request.write_page_data_;
  char *copy_data = new char[BUSTUB_PAGE_SIZE];
  memcpy(copy_data, page_data, BUSTUB_PAGE_SIZE);
  request.page_id_ = page_id;
  request.need_write_ = true;
  request.write_page_data_ = copy_data;
}

void DiskScheduler::ExecuteRead(page_id_t page_id) {
  std::shared_lock<std::shared_mutex> latch(schedule_latch_);
  DiskRequest &request = disk_requests_[page_id];
  latch.unlock();
  ExecuteRead(request);
}

void DiskScheduler::ExecuteWrite(page_id_t page_id) {
  std::shared_lock<std::shared_mutex> latch(schedule_latch_);
  DiskRequest &request = disk_requests_[page_id];
  latch.unlock();
  ExecuteWrite(request);
}

void DiskScheduler::ExecuteReadAsync(page_id_t page_id) {
  std::shared_lock<std::shared_mutex> latch(schedule_latch_);
  DiskRequest &request = disk_requests_[page_id];
  latch.unlock();
  ThreadPoolSubmit(&request, ExecuteType::Read);
}

void DiskScheduler::ExecuteWriteAsync(page_id_t page_id) {
  std::shared_lock<std::shared_mutex> latch(schedule_latch_);
  DiskRequest &request = disk_requests_[page_id];
  latch.unlock();
  ThreadPoolSubmit(&request, ExecuteType::Write);
}

void DiskScheduler::CheckPageLoaded(page_id_t page_id) { ExecuteRead(page_id); }

void DiskScheduler::ExecuteRead(DiskRequest &request) {
  std::unique_lock<std::mutex> request_latch(request.request_latch_);
  if (request.need_read_) {
    if (request.need_write_) {
      memcpy(request.read_page_data_, request.write_page_data_, BUSTUB_PAGE_SIZE);
    } else {
      disk_manager_->ReadPage(request.page_id_, request.read_page_data_);
    }
    request.need_read_ = false;
  }
}

void DiskScheduler::ExecuteWrite(DiskRequest &request) {
  std::unique_lock<std::mutex> request_latch(request.request_latch_);
  if (request.need_write_) {
    if (request.need_read_) {
      memcpy(request.read_page_data_, request.write_page_data_, BUSTUB_PAGE_SIZE);
      request.need_read_ = false;
    }
    disk_manager_->WritePage(request.page_id_, request.write_page_data_);
    delete[] request.write_page_data_;
    request.write_page_data_ = nullptr;
    request.need_write_ = false;
  }
}

void DiskScheduler::ThreadPoolWorker() {
  while (true) {
    std::unique_lock<std::mutex> thread_pool_latch(thread_pool_latch_);
    while (!thread_pool_shutdown_ && task_queue_.empty()) {
      thread_pool_cv_.wait(thread_pool_latch);
    }
    if (thread_pool_shutdown_ && task_queue_.empty()) {
      return;
    }
    auto [request, type] = task_queue_.front();
    task_queue_.pop_front();
    thread_pool_latch.unlock();
    type == ExecuteType::Read ? ExecuteRead(*request) : ExecuteWrite(*request);
  }
}

void DiskScheduler::ThreadPoolInit(size_t pool_size) {
  thread_pool_.reserve(pool_size);
  thread_pool_shutdown_ = false;
  for (size_t i = 0; i < pool_size; ++i) {
    thread_pool_.emplace_back(&DiskScheduler::ThreadPoolWorker, this);
  }
}

void DiskScheduler::ThreadPoolSubmit(DiskRequest *request, ExecuteType type) {
  std::unique_lock<std::mutex> thread_pool_latch(thread_pool_latch_);
  task_queue_.emplace_back(ExecutionTask{request, type});
  thread_pool_latch.unlock();
  thread_pool_cv_.notify_one();
}

void DiskScheduler::ThreadPoolDestroy() {
  thread_pool_shutdown_ = true;
  thread_pool_cv_.notify_all();
  for (auto &t : thread_pool_) {
    t.join();
  }
}

}  // namespace bustub
