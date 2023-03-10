//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->aggregates_, plan->agg_types_)),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() { child_->Init(); }

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!ready_) {
    // get ready
    Tuple child_tuple{};
    RID child_rid{};
    for (;;) {
      auto status = child_->Next(&child_tuple, &child_rid);
      if (!status) {
        break;
      }
      auto key = MakeAggregateKey(&child_tuple);
      auto val = MakeAggregateValue(&child_tuple);
      aht_.InsertCombine(key, val);
    }
    ready_ = true;
    aht_iterator_ = aht_.Begin();

    // special case: empty input and no group bys
    if (aht_iterator_ == aht_.End()) {
      if (plan_->group_bys_.empty()) {
        *tuple = Tuple{aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
        return true;
      }
    }
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  auto values = aht_iterator_.Key().group_bys_;
  auto aggres = aht_iterator_.Val().aggregates_;

  values.insert(values.end(), aggres.begin(), aggres.end());

  *tuple = Tuple{values, &GetOutputSchema()};

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
