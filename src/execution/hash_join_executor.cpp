//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  Tuple left_tuple;
  RID left_rid;
  hht_.Clear();
  while (left_child_->Next(&left_tuple, &left_rid)) {
    hht_.InsertEntry(MakeLeftJoinKey(&left_tuple), left_tuple);
  }
  right_child_->Init();
  right_tuple_valid_ = false;
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID right_rid;
  while (right_tuple_valid_ || right_child_->Next(&right_tuple_, &right_rid)) {
    if (!right_tuple_valid_) {
      right_key_ = MakeRightJoinKey(&right_tuple_);
      if (!hht_.HasKey(right_key_)) {
        continue;
      }
      hht_iter_ = hht_.BeginOfKey(right_key_);
      right_tuple_valid_ = true;
    }
    while (hht_iter_ != hht_.EndOfKey(right_key_)) {
      const Tuple &left_tuple = *hht_iter_;
      ++hht_iter_;
      *tuple = MakeJoinTuple(&left_tuple, &right_tuple_);
      return true;
    }
    right_tuple_valid_ = false;
  }
  return false;
}

}  // namespace bustub
