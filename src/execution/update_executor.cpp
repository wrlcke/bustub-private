//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan->TableOid());
  transaction_ = exec_ctx_->GetTransaction();
  lock_manager_ = exec_ctx->GetLockManager();
}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    lock_manager_->LockExclusiveIfNeeded(transaction_, *rid);
    Tuple &&updated_tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction())) {
      transaction_->SetState(TransactionState::ABORTED);
      return false;
    }
    for (const IndexInfo *index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      const Schema &key_schema = *index_info->index_->GetKeySchema();
      const std::vector<uint32_t> &key_attrs = index_info->index_->GetKeyAttrs();
      const Tuple &old_key = tuple->KeyFromTuple(table_info_->schema_, key_schema, key_attrs);
      const Tuple &new_key = updated_tuple.KeyFromTuple(table_info_->schema_, key_schema, key_attrs);
      if (old_key.GetLength() != new_key.GetLength() ||
          std::strncmp(old_key.GetData(), new_key.GetData(), old_key.GetLength()) != 0) {
        index_info->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
        index_info->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
        IndexWriteRecord record(*rid, table_info_->oid_, WType::UPDATE, updated_tuple, index_info->index_oid_,
                                exec_ctx_->GetCatalog());
        record.old_tuple_ = *tuple;
        transaction_->GetIndexWriteSet()->emplace_back(record);
      }
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
