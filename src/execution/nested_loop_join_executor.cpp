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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), lexecutor_(std::move(left_executor)), rexecutor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  lexecutor_->Init();
  rexecutor_->Init();
  // get ready for right tuples
  Tuple tuple;
  RID rid;
  for (;;) {
    const auto status = rexecutor_->Next(&tuple, &rid);
    if (!status) {
      break;
    }
    right_tuples_.emplace_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_expr = plan_->Predicate();

  Tuple left_tuple;
  RID left_rid;

  for (;;) {
    const auto status = lexecutor_->Next(&left_tuple, &left_rid);

    if (!status) {
      return false;
    }

    for (const auto &right_tuple : right_tuples_) {

    }
  }
}

}  // namespace bustub
