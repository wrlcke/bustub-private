//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  transaction_ = exec_ctx_->GetTransaction();
  lock_manager_ = exec_ctx->GetLockManager();

  assert(transaction_ != nullptr);
  assert(lock_manager_ != nullptr);
}

void SeqScanExecutor::Init() { table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  for (; table_iterator_ != table_info_->table_->End(); ++table_iterator_) {
    if (!lock_manager_->LockSharedIfNeeded(transaction_, table_iterator_->GetRid())) {
      return false;
    }
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(&*table_iterator_, &table_info_->schema_).GetAs<bool>()) {
      *tuple = MakeOutputTuple(*table_iterator_);
      *rid = table_iterator_->GetRid();
      lock_manager_->UnlockSharedIfNeeded(transaction_, table_iterator_->GetRid());
      ++table_iterator_;
      return true;
    }
    lock_manager_->UnlockSharedIfNeeded(transaction_, table_iterator_->GetRid());
  }
  return false;
}

Tuple SeqScanExecutor::MakeOutputTuple(const Tuple &tuple) {
  std::vector<Value> values;
  values.reserve(GetOutputSchema()->GetColumnCount());
  for (const Column &column : GetOutputSchema()->GetColumns()) {
    BUSTUB_ASSERT(column.GetExpr() != nullptr, "Column expression cannot be null.");
    values.emplace_back(column.GetExpr()->Evaluate(&tuple, &table_info_->schema_));
  }
  return Tuple(values, GetOutputSchema());
}

}  // namespace bustub
