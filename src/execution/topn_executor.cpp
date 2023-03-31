#include <algorithm>

#include "execution/executors/sort_executor.h"
#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  // compare function to build reverse top N tuples
  auto comp_fn = CompFn(GetOutputSchema(), plan_->GetOrderBy());
  // auto reverse_comp_fn = [&](Tuple &t1, Tuple &t2) { return !comp_fn(t1, t2); };

  // get next from child, and build a reverse top-n heap
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_.push_back(child_tuple);
    std::push_heap(tuples_.begin(), tuples_.end(), comp_fn);
    while (tuples_.size() > plan_->GetN()) {
      std::pop_heap(tuples_.begin(), tuples_.end(), comp_fn);
      tuples_.pop_back();
    }
  }

  // sort heap in correct compare order
  std::sort(tuples_.begin(), tuples_.end(), comp_fn);
  iter_ = tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *iter_;
  iter_++;
  return true;
}

}  // namespace bustub
