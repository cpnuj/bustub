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

  std::vector<IndexInfo *> indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(tinfo->name_);

  int32_t cnt = 0;
  Tuple child_tuple{};

  for (;;) {
    auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      break;
    }
    assert(tinfo->table_->InsertTuple(child_tuple, rid, txn));

    // insert index
    {
      for (IndexInfo *index_info : indexes) {
        // generate key fixes index key schema
        Schema schema = tinfo->schema_;
        Schema key_schema = index_info->key_schema_;
        std::vector<uint32_t> key_attrs;

        for (const Column &col : key_schema.GetColumns()) {
          key_attrs.emplace_back(schema.GetColIdx(col.GetName()));
        }

        // insert it
        Tuple index_tuple = child_tuple.KeyFromTuple(schema, key_schema, key_attrs);
        index_info->index_->InsertEntry(index_tuple, *rid, txn);
      }
    }

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
