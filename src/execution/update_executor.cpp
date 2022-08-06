//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
    // 初始化成员对象
    auto catalog=exec_ctx_->GetCatalog();
    auto table_oid=plan_->TableOid();
    table_info_=catalog->GetTable(table_oid);
    auto table_name=table_info_->name_;
    index_infos=catalog->GetTableIndexes(table_name);
    child_executor_->Init();
}
void UpdateExecutor::Update(Tuple &tuple, RID &rid){
    // 先构造要更新的tuple
    Tuple update_tupe=GenerateUpdatedTuple(tuple);
    auto table_heap=table_info_->table_.get();
    // 在table中更新数据
    table_heap->UpdateTuple(update_tupe,rid,exec_ctx_->GetTransaction());
    // 在索引中更新数据
    for(auto &index_info:index_infos){
        B_PLUS_TREE_INDEX_TYPE *index=reinterpret_cast<B_PLUS_TREE_INDEX_TYPE *>(index_info->index_.get());
        // 先删除再增加
        index->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index_info->index_->GetMetadata()->GetKeyAttrs()),rid,exec_ctx_->GetTransaction());
        index->InsertEntry(tuple.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index_info->index_->GetMetadata()->GetKeyAttrs()),rid,exec_ctx_->GetTransaction());

    }
}
bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    // 没有更新的值
    if(!child_executor_->Next(tuple,rid)){
        return false;
    }
    Update(*tuple,*rid);
    return true;
 }
}  // namespace bustub
