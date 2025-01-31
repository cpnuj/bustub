#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);

    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "limit multi children?");
    const auto &child_plan = optimized_plan->children_[0];

    if (child_plan->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      BUSTUB_ENSURE(child_plan->children_.size() == 1, "sort multi children?");
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, child_plan->children_[0],
                                            sort_plan.GetOrderBy(), limit_plan.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
