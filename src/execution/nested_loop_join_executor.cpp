//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  left_schema_ = left_executor_->GetOutputSchema();
  right_schema_ = right_executor_->GetOutputSchema();
  output_schema_ = plan_->OutputSchema();
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  outer_tuple_valid_ = false;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple inner_tuple;
  RID outer_rid;
  RID inner_rid;

  while (outer_tuple_valid_ || left_executor_->Next(&outer_tuple_, &outer_rid)) {
    if (!outer_tuple_valid_) {
      outer_tuple_valid_ = true;
      right_executor_->Init();
    }
    while (right_executor_->Next(&inner_tuple, &inner_rid)) {
      if (plan_->Predicate()->EvaluateJoin(&outer_tuple_, left_schema_, &inner_tuple, right_schema_).GetAs<bool>()) {
        *tuple = MakeJoinTuple(outer_tuple_, inner_tuple);
        return true;
      }
    }
    outer_tuple_valid_ = false;
  }
  return false;
}

}  // namespace bustub
