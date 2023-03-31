//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The sorted tuples */
  std::vector<Tuple> tuples_;
  /** The sorted tuples iter */
  std::vector<Tuple>::iterator iter_;
};

struct CompFn {
  const Schema &schema_;
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys_;

  auto operator()(Tuple &t1, Tuple &t2) const -> bool {
    for (const auto &order_by : order_bys_) {
      const auto &[t, expr] = order_by;
      auto v1 = expr->Evaluate(&t1, schema_);
      auto v2 = expr->Evaluate(&t2, schema_);

      auto result = v1.CompareEquals(v2);
      if (result == CmpBool::CmpTrue) {
        continue;
      }

      if (t == OrderByType::DESC) {
        result = v1.CompareGreaterThan(v2);
      } else {
        result = v1.CompareLessThan(v2);
      }

      return result == CmpBool::CmpTrue;
    }
    // all same got here
    return false;
  }

  explicit CompFn(const Schema &schema, const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys)
      : schema_(schema), order_bys_(order_bys) {}
};

}  // namespace bustub
