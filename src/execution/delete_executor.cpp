//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  TableInfo *tinfo = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  Transaction *txn = GetExecutorContext()->GetTransaction();

  int32_t cnt = 0;
  Tuple child_tuple{};
  RID child_rid{};

  for (;;) {
    auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }
    assert(tinfo->table_->MarkDelete(child_rid, txn));
    cnt++;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(Value(TypeId::INTEGER, cnt));
  *tuple = Tuple{values, &GetOutputSchema()};

  done_ = true;
  return true;
}

}  // namespace bustub
