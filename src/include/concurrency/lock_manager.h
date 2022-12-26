//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
   public:
    using Iterator = std::list<LockRequest>::iterator;
    std::list<LockRequest> request_queue_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;

    Iterator Find(txn_id_t txn_id) {
      return std::find_if(request_queue_.begin(), request_queue_.end(),
                          [txn_id](const LockRequest &request) { return request.txn_id_ == txn_id; });
    }

    Iterator Push(txn_id_t txn_id, LockMode lock_mode) {
      return request_queue_.emplace(request_queue_.end(), txn_id, lock_mode);
    }

    Iterator Erase(Iterator it) { return request_queue_.erase(it); }

    Iterator FirstWaiting() {
      return std::find_if(request_queue_.begin(), request_queue_.end(),
                          [](const LockRequest &request) { return !request.granted_; });
    }

    Iterator Move(Iterator src, Iterator dest) {
      auto request = *src;
      request_queue_.erase(src);
      return request_queue_.emplace(dest, request);
    }

    Iterator Begin() { return request_queue_.begin(); }

    Iterator End() { return request_queue_.end(); }
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  bool LockSharedIfNeeded(Transaction *txn, const RID &rid) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      return LockShared(txn, rid);
    }
    return true;
  }

  bool LockExclusiveIfNeeded(Transaction *txn, const RID &rid) {
    if (txn->IsSharedLocked(rid)) {
      return LockUpgrade(txn, rid);
    }
    return LockExclusive(txn, rid);
  }

  bool UnlockSharedIfNeeded(Transaction *txn, const RID &rid) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (txn->IsSharedLocked(rid)) {
        return Unlock(txn, rid);
      }
    }
    return true;
  }

 private:
  /** The latch to protect the lock table. */
  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;

  bool NeedWait(const LockRequest &request, const LockRequestQueue &lock_request_queue);

  void AbortYoung(const LockRequest &request, LockRequestQueue *lock_request_queue);

  bool HasConflict(const LockRequest &request, const LockRequest &other);
};

}  // namespace bustub
