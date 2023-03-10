//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  // build key column
  key_column_ = {Column("unnamed", plan_->KeyPredicate()->GetReturnType())};
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto *txn = GetExecutorContext()->GetTransaction();
  auto *iinfo = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);
  auto *tinfo = GetExecutorContext()->GetCatalog()->GetTable(iinfo->table_name_);
  auto *btidx = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(iinfo->index_.get());

  for (;;) {
    Tuple outer_tuple;

    if (!child_executor_->Next(&outer_tuple, rid)) {
      return false;
    }

    auto key = plan_->KeyPredicate()->Evaluate(&outer_tuple, child_executor_->GetOutputSchema());
    auto key_schema = Schema{key_column_};
    auto key_tuple = Tuple{std::vector<Value>{key}, &key_schema};

    std::vector<RID> result;
    btidx->ScanKey(key_tuple, &result, txn);

    assert(result.empty() || result.size() == 1);

    if (result.size() == 1) {
      Tuple inner_tuple;
      assert(tinfo->table_->GetTuple(result[0], &inner_tuple, txn /* check acquire read lock */));
      *tuple =
          ConcatTuples(outer_tuple, child_executor_->GetOutputSchema(), inner_tuple, tinfo->schema_, GetOutputSchema());
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = ConcatTuples(outer_tuple, child_executor_->GetOutputSchema(), NullTupleFromSchema(tinfo->schema_),
                            tinfo->schema_, GetOutputSchema());
      return true;
    }
  }
}

}  // namespace bustub
