//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    // Insert the tuple directly into the table
    for (const auto &raw_value : plan_->RawValues()) {
      *tuple = Tuple(raw_value, &table_info_->schema_);
      InsertEntry(tuple, rid);
    }
  } else {
    // Insert the tuple from the child executor
    while (child_executor_->Next(tuple, rid)) {
      InsertEntry(tuple, rid);
    }
  }
  return false;
}

}  // namespace bustub
