#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::TrySplitPredicates(AbstractExpressionRef predicates) -> std::vector<AbstractExpressionRef> {
  LogicExpression *logic_expr = dynamic_cast<LogicExpression *>(predicates.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    std::vector<AbstractExpressionRef> left_ret, right_ret, ret;
    left_ret = TrySplitPredicates(predicates->GetChildAt(0));
    right_ret = TrySplitPredicates(predicates->GetChildAt(1));
    ret.insert(ret.end(), left_ret.begin(), left_ret.end());
    ret.insert(ret.end(), right_ret.begin(), right_ret.end());
    return ret;
  }
  return std::vector<AbstractExpressionRef>{predicates};
}

auto Optimizer::OptimizeSplitPredicates(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSplitPredicates(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);

    auto split_res = TrySplitPredicates(filter_plan.GetPredicate());
    if (split_res.size() > 1) {
      AbstractPlanNodeRef split_plan = std::move(filter_plan.GetChildPlan());
      for (int i = split_res.size() - 1; i >= 0; i--) {
        split_plan = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(split_plan->OutputSchema()),
                                                      std::move(split_res[i]), std::move(split_plan));
      }
      return split_plan;
    }

    // no split
    return optimized_plan;
  }

  return optimized_plan;
}

auto Optimizer::TryRewriteExprForLeft(const AbstractExpressionRef &expr, size_t left_col_cnt, size_t right_col_cnt,
                                      bool *succ) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(TryRewriteExprForLeft(child, left_col_cnt, right_col_cnt, succ));
  }
  const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
  if (column_value_expr != nullptr) {
    BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 0, "tuple_idx cannot be value other than 0 before this stage.")
    auto col_idx = column_value_expr->GetColIdx();
    if (col_idx < left_col_cnt) {
      return std::make_shared<ColumnValueExpression>(0, col_idx, column_value_expr->GetReturnType());
    }
    // col_idx in right
    *succ = false;
  }
  return expr->CloneWithChildren(children);
}

auto Optimizer::TryRewriteExprForRight(const AbstractExpressionRef &expr, size_t left_col_cnt, size_t right_col_cnt,
                                       bool *succ) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(TryRewriteExprForRight(child, left_col_cnt, right_col_cnt, succ));
  }
  const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
  if (column_value_expr != nullptr) {
    BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 0, "tuple_idx cannot be value other than 0 before this stage.")
    auto col_idx = column_value_expr->GetColIdx();
    if (col_idx >= left_col_cnt && col_idx < left_col_cnt + right_col_cnt) {
      return std::make_shared<ColumnValueExpression>(0, col_idx - left_col_cnt, column_value_expr->GetReturnType());
    }
    // col_idx in left
    *succ = false;
  }
  return expr->CloneWithChildren(children);
}

auto Optimizer::TryPushdownPredicates(const AbstractPlanNodeRef &plan, AbstractExpressionRef predicate)
    -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;

  // only one child, simply push down
  if (plan->children_.size() == 1) {
    children.emplace_back(TryPushdownPredicates(plan->GetChildAt(0), std::move(predicate)));
    return plan->CloneWithChildren(std::move(children));
  }

  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    size_t left_col_cnt = nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
    size_t right_col_cnt = nlj_plan.GetRightPlan()->OutputSchema().GetColumnCount();

    AbstractExpressionRef new_predicate{};
    bool can_rewrite;

    // try left
    can_rewrite = true;
    new_predicate = TryRewriteExprForLeft(predicate, left_col_cnt, right_col_cnt, &can_rewrite);
    if (can_rewrite) {
      children.emplace_back(TryPushdownPredicates(nlj_plan.GetChildAt(0), std::move(new_predicate)));
      children.emplace_back(nlj_plan.GetChildAt(1));
      return plan->CloneWithChildren(std::move(children));
    }

    // try right
    can_rewrite = true;
    new_predicate = TryRewriteExprForRight(predicate, left_col_cnt, right_col_cnt, &can_rewrite);
    if (can_rewrite) {
      children.emplace_back(nlj_plan.GetChildAt(0));
      children.emplace_back(TryPushdownPredicates(nlj_plan.GetChildAt(1), std::move(new_predicate)));
      return plan->CloneWithChildren(std::move(children));
    }
  }

  // cannot pushdown, stop here
  return std::make_shared<FilterPlanNode>(std::make_shared<Schema>(plan->OutputSchema()), std::move(predicate),
                                          std::move(plan));
}

auto Optimizer::OptimizePushdownPredicates(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePushdownPredicates(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    return TryPushdownPredicates(filter_plan.GetChildAt(0), filter_plan.GetPredicate());
  }

  return optimized_plan;
}

void Optimizer::ComputeRequiredIdx(const AbstractExpressionRef &expr, std::set<size_t> &indexes) {
  for (const auto &child : expr->GetChildren()) {
    ComputeRequiredIdx(child, indexes);
  }
  const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
  if (column_value_expr != nullptr) {
    indexes.insert(column_value_expr->GetColIdx());
  }
}

auto Optimizer::TryPushdownProjection(const AbstractPlanNodeRef &plan, const AbstractPlanNodeRef &parent)
    -> AbstractPlanNodeRef {
  BUSTUB_ASSERT(parent->GetType() == PlanType::Projection, "parent plan must be projection");

  const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*parent);
  const auto &exprs = proj_plan.GetExpressions();

  //
  // For join node, the father expressions may retrive data from both of our children.
  //
  // First, we should compute the needed projection index for our children according to
  // the father expressions and the join condition. We should get the needed projection
  // index for left and right child.
  //
  // Second, we construct the pushdown projection plan node for left child and right
  // child, according to their need projection index, and then pushdown the new node.
  //
  // Last, we should rewrite our join condition to fix the new scheme of our children.
  //
  if (plan->GetType() == PlanType::HashJoin) {
    const auto &hjoin_plan = dynamic_cast<const HashJoinPlanNode &>(*plan);

    // step 1. compute the required projection indexes from projection expressions
    std::set<size_t> proj_indexes;
    std::for_each(exprs.begin(), exprs.end(), [&](auto &expr) { ComputeRequiredIdx(expr, proj_indexes); });

    // step 2. add required join indexes, the indexes from right child need to be re-computed
    std::set<size_t> join_indexes;
    std::copy(proj_indexes.begin(), proj_indexes.end(), std::inserter(join_indexes, join_indexes.begin()));
    ComputeRequiredIdx(hjoin_plan.left_key_expression_, join_indexes);

    // right join indexes need re-compute
    std::set<size_t> right_indexes;
    ComputeRequiredIdx(hjoin_plan.right_key_expression_, right_indexes);
    size_t left_col_cnt = hjoin_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
    std::for_each(right_indexes.begin(), right_indexes.end(),
                  [&join_indexes, left_col_cnt](size_t idx) { join_indexes.insert(idx + left_col_cnt); });
  }
  if (plan->GetType() == PlanType::Aggregation) {
    // const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
  }
  // else stop trying
  return parent;
}

auto Optimizer::OptimizePushdownProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePushdownPredicates(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Projection) {
    // const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Filter with multiple children?? Impossible!");
    const auto &child_plan = optimized_plan->children_[0];
    return TryPushdownProjection(child_plan, std::move(optimized_plan));
  }

  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeSplitPredicates(p);
  p = OptimizePushdownPredicates(p);
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizePushdownProjection(p);
  return p;
}

}  // namespace bustub
