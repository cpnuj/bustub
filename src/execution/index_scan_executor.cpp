//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  IndexInfo *iinfo = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  // Transaction *txn = GetExecutorContext()->GetTransaction();
  auto *btidx = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(iinfo->index_.get());
  iter_ = btidx->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }

  Transaction *txn = GetExecutorContext()->GetTransaction();
  IndexInfo *iinfo = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);
  TableInfo *tinfo = GetExecutorContext()->GetCatalog()->GetTable(iinfo->table_name_);

  auto [_, val] = *iter_;
  *rid = val;
  tinfo->table_->GetTuple(val, tuple, txn);
  ++iter_;

  return true;
}

}  // namespace bustub
