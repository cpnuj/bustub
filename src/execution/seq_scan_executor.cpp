//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(TableIterator(nullptr, RID{}, nullptr)),
      iter_end_(TableIterator(nullptr, RID{}, nullptr)) {}

void SeqScanExecutor::Init() {
  TableInfo *tinfo = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  Transaction *txn = GetExecutorContext()->GetTransaction();
  iter_ = tinfo->table_->Begin(txn);
  iter_end_ = tinfo->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == iter_end_) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

}  // namespace bustub
