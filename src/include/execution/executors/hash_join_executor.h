//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

// struct HashJoinKey {
//   Value key_;
//   bool operator==(const HashJoinKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
//   struct Hasher {
//     std::size_t operator()(const HashJoinKey &key) const { return HashUtil::HashValue(&key.key_); }
//   };
// };
using HashJoinKey = Value;
using HashJoinValue = std::vector<Tuple>;
struct HashJoinKeyEqualFn {
  bool operator()(const HashJoinKey &lhs, const HashJoinKey &rhs) const {
    return lhs.CompareEquals(rhs) == CmpBool::CmpTrue;
  }
};
struct HashJoinKeyHashFn {
  std::size_t operator()(const HashJoinKey &key) const { return HashUtil::HashValue(&key); }
};
class SimpleHashJoinHashTable {
 public:
  inline void InsertEntry(const HashJoinKey &key, const Tuple &tuple) {
    if (ht_.count(key) == 0) {
      ht_.insert({key, HashJoinValue()});
    }
    ht_[key].emplace_back(tuple);
  }

  using ValueIterator = HashJoinValue::const_iterator;
  //  using KeyIterator =
  //      std::unordered_map<HashJoinKey, ValueIterator, HashJoinKeyHashFn, HashJoinKeyEqualFn>::const_iterator;
  inline bool HasKey(const HashJoinKey &key) const { return ht_.count(key) > 0; }
  inline ValueIterator BeginOfKey(const HashJoinKey &key) const { return ht_.at(key).cbegin(); }
  inline ValueIterator EndOfKey(const HashJoinKey &key) const { return ht_.at(key).cend(); }
  inline void Clear() { ht_.clear(); }

 private:
  std::unordered_map<HashJoinKey, HashJoinValue, HashJoinKeyHashFn, HashJoinKeyEqualFn> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  SimpleHashJoinHashTable hht_{};
  SimpleHashJoinHashTable::ValueIterator hht_iter_{};
  Tuple right_tuple_{};
  Value right_key_{};
  bool right_tuple_valid_{};

 private:
  inline HashJoinKey MakeLeftJoinKey(const Tuple *tuple) const {
    return plan_->LeftJoinKeyExpression()->Evaluate(tuple, left_child_->GetOutputSchema());
  }

  inline HashJoinKey MakeRightJoinKey(const Tuple *tuple) const {
    return plan_->RightJoinKeyExpression()->Evaluate(tuple, right_child_->GetOutputSchema());
  }

  inline Tuple MakeJoinTuple(const Tuple *left_tuple, const Tuple *right_tuple) {
    std::vector<Value> values;
    values.reserve(left_child_->GetOutputSchema()->GetColumnCount() +
                   right_child_->GetOutputSchema()->GetColumnCount());
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      BUSTUB_ASSERT(col.GetExpr() != nullptr, "Column expression should not be null.");
      values.emplace_back(col.GetExpr()->EvaluateJoin(left_tuple, left_child_->GetOutputSchema(), right_tuple,
                                                      right_child_->GetOutputSchema()));
    }
    return Tuple(values, plan_->OutputSchema());
  }
};

}  // namespace bustub
