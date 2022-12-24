//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the insert */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  /** The child executor that produces tuple for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The child executor that produces tuple for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_executor_;
  const Schema *output_schema_;
  const Schema *left_schema_;
  const Schema *right_schema_;
  Tuple outer_tuple_;
  bool outer_tuple_valid_;

  inline Tuple MakeJoinTuple(const Tuple &outer_tuple, const Tuple &inner_tuple) {
    std::vector<Value> values;
    values.reserve(output_schema_->GetColumnCount());
    for (const auto &col : output_schema_->GetColumns()) {
      BUSTUB_ASSERT(col.GetExpr() != nullptr, "Column expression cannot be null.");
      values.emplace_back(col.GetExpr()->EvaluateJoin(&outer_tuple_, left_schema_, &inner_tuple, right_schema_));
    }
    return Tuple(values, output_schema_);
  }
};

}  // namespace bustub
