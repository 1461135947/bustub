//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),plan_(plan),child_(std::move(child)),hash_table{plan_->GetAggregates(),plan->GetAggregateTypes()},hasht_table_iter(hash_table.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
    Tuple temp_tuple;
    RID temp_rid;

    child_->Init();
    // 构建哈希表
    while (child_->Next(&temp_tuple, &temp_rid)){
        hash_table.InsertCombine(MakeKey(&temp_tuple), MakeVal(&temp_tuple));
    }
    hasht_table_iter = hash_table.Begin();
}
Tuple AggregationExecutor::TransformOutput(){

    std::vector<Value> values;
    // 按照输出格式进行组装
    for (const auto &col: GetOutputSchema()->GetColumns()){
        values.push_back(col.GetExpr()->EvaluateAggregate(hasht_table_iter.Key().group_bys_, hasht_table_iter.Val().aggregates_));
    }
    return Tuple(values, GetOutputSchema());
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { 
    auto having=plan_->GetHaving();
    while (hasht_table_iter != hash_table.End()){
    
    //进行结果过滤
    if ( having== nullptr ||having->EvaluateAggregate(hasht_table_iter.Key().group_bys_, hasht_table_iter.Val().aggregates_).GetAs<bool>()){
      *tuple = TransformOutput();
      ++hasht_table_iter;
      return true;
    }

        ++hasht_table_iter;
    }
    return false;
 }

}  // namespace bustub
