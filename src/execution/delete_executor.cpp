//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,plan_(plan),child_executor_(std::move(child_executor)){

    }

void DeleteExecutor::Init() {
    auto *catalog=exec_ctx_->GetCatalog();
    auto table_oid=plan_->TableOid();
    table_metadata=catalog->GetTable(table_oid);
    auto table_name=table_metadata->name_;
    index_infos=catalog->GetTableIndexes(table_name);
    child_executor_->Init();
}
 void DeleteExecutor::Delete(Tuple &tuple,RID &rid){
    // 从table中删除
     auto heap=table_metadata->table_.get();
     heap->MarkDelete(rid,exec_ctx_->GetTransaction());
    // 从索引中删除
    for(auto &index_info:index_infos){
        B_PLUS_TREE_INDEX_TYPE *index=reinterpret_cast<B_PLUS_TREE_INDEX_TYPE *>(index_info->index_.get());
        index->DeleteEntry(tuple.KeyFromTuple(table_metadata->schema_,index_info->key_schema_,index_info->index_->GetMetadata()->GetKeyAttrs()),rid,exec_ctx_->GetTransaction());
    }
 }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    if (!child_executor_->Next(tuple, rid)){
        return false;
    }

    Delete(*tuple, *rid);
    return true;
 }
    
}  // namespace bustub
