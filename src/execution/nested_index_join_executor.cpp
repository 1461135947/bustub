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

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)),index_ptr(nullptr),heap(nullptr){

    }

void NestIndexJoinExecutor::Init() {
    child_executor_->Init();
    auto catalog=exec_ctx_->GetCatalog();
    auto table_oid=plan_->GetInnerTableOid();
    TableMetadata *metadata=catalog->GetTable(table_oid);
    auto table_name=metadata->name_;
    heap=metadata->table_.get();
    IndexInfo *index_info=catalog->GetIndex(plan_->GetIndexName(),table_name);
    index_ptr=reinterpret_cast<B_PLUS_TREE_INDEX_TYPE *>(index_info->index_.get());

}
Tuple NestIndexJoinExecutor::Join(Tuple *left_tuple, Tuple *right_tuple){
    std::vector<Value> vals;
    // 将左右两个tuple组合成一个新的tuple
    for (auto const &col:GetOutputSchema()->GetColumns()){
        vals.push_back(col.GetExpr()->EvaluateJoin(left_tuple, plan_->OuterTableSchema(),right_tuple,plan_->InnerTableSchema()));
    }

  return Tuple{vals, GetOutputSchema()};
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) { 
     while (true){
    //通过右边的集合与左边的进行匹配  
    if (!results.empty()){
      RID right_rid = results.back();
      results.pop_back();

      Tuple right_tuple;
      heap->GetTuple(right_rid, &right_tuple, GetExecutorContext()->GetTransaction());
      *tuple = Join(&left_tuple_, &right_tuple);

      return true;
    }
    // 递增左边集合的迭代器
    if (!child_executor_->Next(&left_tuple_, rid)){
      return false;
    }
    // 在索引中与左边的值进行匹配
    index_ptr->ScanKey(left_tuple_,&results,GetExecutorContext()->GetTransaction());
  }
 }

}  // namespace bustub
