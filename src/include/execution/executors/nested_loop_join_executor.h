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
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  /** The left and right executor. */
  std::unique_ptr<AbstractExecutor> lexecutor_;
  std::unique_ptr<AbstractExecutor> rexecutor_;
  /** The running left tuple. */
  Tuple left_tuple_;
  /** The place stores right tuples. */
  std::vector<Tuple> right_tuples_;
  /** The pointer to next right tuple that should be joined */
  uint32_t right_tuples_next_{0};
  /** How many tuples join successfully, used for left join */
  uint32_t curr_run_succ_{0};
};

auto NullValuesFromSchema(const Schema &schema) -> std::vector<Value>;
auto ValuesFromTuple(const Tuple &tuple, const Schema &schema) -> std::vector<Value>;

auto NullTupleFromSchema(const Schema &schema) -> Tuple;

void CopyTupleValues(std::vector<Value> &dst, const Tuple &tuple, const Schema &schema);

auto ConcatTuples(const Tuple &tuple1, const Schema &schema1, const Tuple &tuple2, const Schema &schema2,
                  const Schema &schema_out) -> Tuple;

}  // namespace bustub
