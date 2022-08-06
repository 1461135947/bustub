//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),child_executor_(std::move(child_executor)),plan_(plan) {}

void InsertExecutor::Init() {
    // std::cout<<"Init\n";
    auto catalog=exec_ctx_->GetCatalog();
    auto table_oid=plan_->TableOid();
    table_metadata_=catalog->GetTable(table_oid);
    // std::cout<<static_cast<void *>(table_metadata_)<<std::endl;
    index_infos=catalog->GetTableIndexes(table_metadata_->name_);
    // 检测是否有子操作
    // std::cout<<plan_->IsRawInsert()<<std::endl;
    if(plan_->IsRawInsert()){
        iter_=plan_->RawValues().begin();
    }else{
        child_executor_->Init();
    }
    // std::cout<<"初始化结束"<<std::endl;
}
void InsertExecutor::Insert(Tuple &insert_tuple, RID *rid){
    // 将数据插入到表中
    TableHeap *table_heap_ptr = table_metadata_->table_.get();
    table_heap_ptr->InsertTuple(insert_tuple, rid, GetExecutorContext()->GetTransaction());
    // 在索引中插入数据
    for (auto &index_info:index_infos){
        B_PLUS_TREE_INDEX_TYPE *b_plus_tree_index= reinterpret_cast<B_PLUS_TREE_INDEX_TYPE*>(index_info->index_.get());
        b_plus_tree_index->InsertEntry(insert_tuple.KeyFromTuple(table_metadata_->schema_,index_info->key_schema_,index_info->index_->GetMetadata()->GetKeyAttrs()),*rid,GetExecutorContext()->GetTransaction());
  }
}
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    // 没有子操作
    if(plan_->IsRawInsert()){
        if(iter_!=plan_->RawValues().end()){
            // 构建需要插入的tuple
            Tuple insert_tuple(*iter_++, &table_metadata_->schema_);
            Insert(insert_tuple,rid);
            return true;
        }
        return false;
    }
    // 有子操作
    if (child_executor_->Next(tuple, rid)){
      Insert(*tuple, rid);
      return true;
    }
    return false;
 }

}  // namespace bustub
