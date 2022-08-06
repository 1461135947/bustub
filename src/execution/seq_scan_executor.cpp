//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan){}

void SeqScanExecutor::Init() {
    // // 初始化类成员变量
    // table_oid_t table_oid=plan_->GetTableOid();
    // auto catalog=exec_ctx_->GetCatalog();
    // // BufferPoolManager *buffer_pool_manager=exec_ctx_->GetBufferPoolManager();
    // table_heap_ptr_=catalog->GetTable(table_oid)->table_.get();
    // table_metadata_ptr_=catalog->GetTable(table_oid);
    // auto txn=exec_ctx_->GetTransaction();
    // table_iter_=table_heap_ptr_->Begin(txn);
    BufferPoolManager *bpm_ptr = exec_ctx_->GetBufferPoolManager();
    table_oid_t table_id = plan_->GetTableOid();
    table_metadata_ptr_ = GetExecutorContext()->GetCatalog()->GetTable(table_id);
    table_heap_ptr_ = table_metadata_ptr_->table_.get();
    page_id_t first_page_id = table_heap_ptr_->GetFirstPageId();


    Page *page_ptr = bpm_ptr->FetchPage(first_page_id);

    RID rid{};
    TablePage *table_page_ptr = reinterpret_cast<TablePage *>(page_ptr);
    assert(table_page_ptr->GetFirstTupleRid(&rid));

    bpm_ptr->UnpinPage(first_page_id, false);


    // Tuple tuple;
    // table_heap_ptr_->GetTuple(rid, &tuple, exec_ctx_->GetTransaction());
    table_iter_ = TableIterator(table_heap_ptr_, rid, exec_ctx_->GetTransaction());


}
// 将tuple的格式按照输出格式进行调整
Tuple SeqScanExecutor::TransformOutPutSchema(Tuple &tuple){
    std::vector<Value> vals;
    
    for(auto &col:GetOutputSchema()->GetColumns()){
        auto val=col.GetExpr()->Evaluate(&tuple,&(table_metadata_ptr_->schema_));
        vals.push_back(val);
    }
    return Tuple(vals,GetOutputSchema());
}
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    // 进行迭代器访问
    Tuple temp;
    auto predicate=plan_->GetPredicate();
    while(table_iter_!=table_heap_ptr_->End()){
        temp=*(table_iter_++);
        *rid=temp.GetRid();
        // 使用predicate跳过不符条件的输出
        if(predicate==nullptr||predicate->Evaluate(&temp,plan_->OutputSchema()).GetAs<bool>()){
            *tuple=TransformOutPutSchema(temp);
            
            
            return true;
        }
        
    }
    return false;
}


}  // namespace bustub
