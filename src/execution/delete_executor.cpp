//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  transaction_ = exec_ctx_->GetTransaction();
  lock_manager_ = exec_ctx->GetLockManager();
  assert(transaction_ != nullptr);
  assert(lock_manager_ != nullptr);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    lock_manager_->LockExclusiveIfNeeded(transaction_, *rid);
    if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      transaction_->SetState(TransactionState::ABORTED);
      return false;
    }
    for (const IndexInfo *index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      const Tuple &key = tuple->KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      transaction_->GetIndexWriteSet()->emplace_back(*rid, table_info_->oid_, WType::DELETE, *tuple,
                                                     index_info->index_oid_, exec_ctx_->GetCatalog());
    }
  }
  return false;
}

}  // namespace bustub
