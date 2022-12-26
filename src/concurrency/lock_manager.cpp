//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  std::unique_lock<std::mutex> latch(latch_);
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  LockRequestQueue &lock_request_queue = lock_table_[rid];
  auto request = lock_request_queue.Push(txn->GetTransactionId(), LockMode::SHARED);
  AbortYoung(*request, &lock_request_queue);

  while (NeedWait(*request, lock_request_queue)) {
    lock_request_queue.cv_.wait(latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      // return false;
    }
  }
  request->granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  std::unique_lock<std::mutex> latch(latch_);
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    latch_.unlock();
    return LockUpgrade(txn, rid);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  LockRequestQueue &lock_request_queue = lock_table_[rid];
  auto request = lock_request_queue.Push(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  AbortYoung(*request, &lock_request_queue);

  while (NeedWait(*request, lock_request_queue)) {
    lock_request_queue.cv_.wait(latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      // return false;
    }
  }
  request->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  std::unique_lock<std::mutex> latch(latch_);
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  lock_request_queue.upgrading_ = txn->GetTransactionId();
  auto old_position = lock_request_queue.Find(txn->GetTransactionId());
  auto request = lock_request_queue.Move(old_position, lock_request_queue.FirstWaiting());
  txn->GetSharedLockSet()->erase(rid);
  request->lock_mode_ = LockMode::EXCLUSIVE;
  request->granted_ = false;
  AbortYoung(*request, &lock_request_queue);

  while (NeedWait(*request, lock_request_queue)) {
    lock_request_queue.cv_.wait(latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      // return false;
    }
  }
  request->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_request_queue.upgrading_ = INVALID_TXN_ID;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  std::unique_lock<std::mutex> latch(latch_);
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  auto request = lock_request_queue.Find(txn->GetTransactionId());
  if (request != lock_request_queue.End()) {
    lock_request_queue.Erase(request);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  lock_request_queue.cv_.notify_all();
  return true;
}

bool LockManager::NeedWait(const LockRequest &request, const LockRequestQueue &lock_request_queue) {
  if (request.lock_mode_ == LockMode::SHARED) {
    for (const auto &lock_request : lock_request_queue.request_queue_) {
      if (lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
        return true;
      }
      if (lock_request.txn_id_ == request.txn_id_) {
        return false;
      }
    }
    BUSTUB_ASSERT(false, "Lock request not found in lock request queue.");
  } else {
    if (!lock_request_queue.request_queue_.empty()) {
      return lock_request_queue.request_queue_.front().txn_id_ != request.txn_id_;
    }
    BUSTUB_ASSERT(false, "Lock request queue should not be empty");
  }
}

void LockManager::AbortYoung(const LockRequest &request, LockRequestQueue *lock_request_queue) {
  bool any_killed = false;
  LockRequestQueue::Iterator other = lock_request_queue->Begin();
  while (other->txn_id_ != request.txn_id_) {
    if (request.txn_id_ < other->txn_id_ && HasConflict(request, *other)) {
      any_killed = true;
      Transaction *txn = TransactionManager::GetTransaction(other->txn_id_);
      txn->SetState(TransactionState::ABORTED);
      other = lock_request_queue->Erase(other);
    } else {
      ++other;
    }
  }
  if (any_killed) {
    lock_request_queue->cv_.notify_all();
  }
}

bool LockManager::HasConflict(const LockRequest &request, const LockRequest &other) {
  return request.lock_mode_ == LockMode::EXCLUSIVE || other.lock_mode_ == LockMode::EXCLUSIVE;
}

}  // namespace bustub
