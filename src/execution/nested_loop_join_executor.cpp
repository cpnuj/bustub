//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      lexecutor_(std::move(left_executor)),
      rexecutor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  lexecutor_->Init();
  rexecutor_->Init();
  // get ready for right tuples
  Tuple tuple;
  RID rid;
  for (;;) {
    const auto status = rexecutor_->Next(&tuple, &rid);
    if (!status) {
      break;
    }
    right_tuples_.emplace_back(tuple);
  }
  // right_tuples_next_ = right_tuples_.size();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = lexecutor_->GetOutputSchema();
  auto right_schema = rexecutor_->GetOutputSchema();

  // first run
  if (!left_tuple_.IsAllocated()) {
    if (!lexecutor_->Next(&left_tuple_, rid)) {
      return false;
    }
  }

  for (;;) {
    // We are at the end of a run, get next left tuple
    if (right_tuples_next_ >= right_tuples_.size()) {
      // if it requires a left join, and there is no success in this run,
      // emits a new tuple with left value and null right value.
      if (plan_->GetJoinType() == JoinType::LEFT && curr_run_succ_ == 0) {
        *tuple =
            ConcatTuples(left_tuple_, left_schema, NullTupleFromSchema(right_schema), right_schema, GetOutputSchema());
        curr_run_succ_++;
        return true;
      }

      // get next tuple
      if (!lexecutor_->Next(&left_tuple_, rid)) {
        return false;
      }

      // reset state for next run
      right_tuples_next_ = 0;
      curr_run_succ_ = 0;

      continue;
    }

    auto right_tuple_ = right_tuples_[right_tuples_next_++];

    auto value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema);
    if (!value.IsNull() && value.GetAs<bool>()) {
      *tuple = ConcatTuples(left_tuple_, left_schema, right_tuple_, right_schema, GetOutputSchema());
      curr_run_succ_++;
      return true;
    }
  }
}

auto NullValuesFromSchema(const Schema &schema) -> std::vector<Value> {
  std::vector<Value> values{};
  values.reserve(schema.GetColumnCount());
  for (const auto &col : schema.GetColumns()) {
    values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
  }
  return values;
}

auto NullTupleFromSchema(const Schema &schema) -> Tuple {
  std::vector<Value> values{};
  values.reserve(schema.GetColumnCount());
  for (const auto &col : schema.GetColumns()) {
    values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
  }
  return Tuple{values, &schema};
}

auto ValuesFromTuple(const Tuple &tuple, const Schema &schema) -> std::vector<Value> {
  std::vector<Value> values{};
  values.reserve(schema.GetColumnCount());
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    values.push_back(tuple.GetValue(&schema, i));
  }
  return values;
}

void CopyTupleValues(std::vector<Value> &dst, const Tuple &tuple, const Schema &schema) {
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    if (tuple.IsNull(&schema, i)) {
      auto col_type = schema.GetColumn(i).GetType();
      dst.push_back(ValueFactory::GetNullValueByType(col_type));
    } else {
      dst.push_back(tuple.GetValue(&schema, i));
    }
  }
}

auto ConcatTuples(const Tuple &tuple1, const Schema &schema1, const Tuple &tuple2, const Schema &schema2,
                  const Schema &schema_out) -> Tuple {
  std::vector<Value> values{};
  values.reserve(schema_out.GetColumnCount());
  CopyTupleValues(values, tuple1, schema1);
  CopyTupleValues(values, tuple2, schema2);
  return Tuple{values, &schema_out};
}

}  // namespace bustub
