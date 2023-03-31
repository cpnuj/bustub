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
#include "execution/executors/nested_loop_join_executor.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  // build up a in-memory hash table
  Tuple left_tuple;
  RID left_rid;
  const auto &left_expr = plan_->LeftJoinKeyExpression();
  const auto &left_schema = plan_->GetLeftPlan()->OutputSchema();

  while (left_child_->Next(&left_tuple, &left_rid)) {
    auto value = left_expr.Evaluate(&left_tuple, left_schema);
    auto [iter, succ] = table_.insert({value, std::vector<Tuple>{}});
    left_tuples_.emplace_back(std::make_pair(left_tuple, &iter->second));
  }

  // hash right child to table
  Tuple right_tuple;
  RID right_rid;
  const auto &right_expr = plan_->RightJoinKeyExpression();
  const auto &right_schema = plan_->GetRightPlan()->OutputSchema();

  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto value = right_expr.Evaluate(&right_tuple, right_schema);
    auto iter = table_.find(value);
    if (iter != table_.end()) {
      iter->second.push_back(right_tuple);
    }
  }

  left_ptr_ = 0;
  right_ptr_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &left_schema = plan_->GetLeftPlan()->OutputSchema();
  const auto &right_schema = plan_->GetRightPlan()->OutputSchema();
  for (;;) {
    if (left_ptr_ >= left_tuples_.size()) {
      return false;
    }
    const auto &[left_tuple, to_join] = left_tuples_[left_ptr_];
    if (right_ptr_ >= to_join->size()) {
      left_ptr_++;
      right_ptr_ = 0;
      if (to_join->empty() && plan_->GetJoinType() == JoinType::LEFT) {
        *tuple =
            ConcatTuples(left_tuple, left_schema, NullTupleFromSchema(right_schema), right_schema, GetOutputSchema());
        return true;
      }
      continue;
    }
    *tuple = ConcatTuples(left_tuple, left_schema, to_join->at(right_ptr_), right_schema, GetOutputSchema());
    right_ptr_++;
    return true;
  }
}

}  // namespace bustub
