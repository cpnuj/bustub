//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  TableInfo *tinfo = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  Transaction *txn = GetExecutorContext()->GetTransaction();

  int32_t i = 0;
  Tuple child_tuple{};

  for (;;) {
    auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      break;
    }
    assert(tinfo->table_->InsertTuple(child_tuple, rid, txn));
    i++;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(Value(TypeId::INTEGER, i));
  *tuple = Tuple{values, &GetOutputSchema()};
  done_ = true;

  return true;
}

}  // namespace bustub
