//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan){}

void IndexScanExecutor::Init() {
    auto catalog=exec_ctx_->GetCatalog();
    auto index_oid=plan_->GetIndexOid();
    auto index_info_ptr = catalog->GetIndex(index_oid);
    B_PLUS_TREE_INDEX_TYPE *b_plus_tree_index_ptr=reinterpret_cast<B_PLUS_TREE_INDEX_TYPE *>(index_info_ptr->index_.get());
    // 初始化迭代器
    iter_=b_plus_tree_index_ptr->GetBeginIterator();
    end_iter_=b_plus_tree_index_ptr->GetEndIterator();
    // 初始化table的相关信息
    table_metadata_ptr_=catalog->GetTable(index_info_ptr->table_name_);
    table_heap_ptr_=table_metadata_ptr_->table_.get();
}

// 将tuple的格式按照输出格式进行调整
Tuple IndexScanExecutor::TransformOutPutSchema(Tuple &tuple){
    std::vector<Value> vals;
    
    for(auto &col:GetOutputSchema()->GetColumns()){
        auto val=col.GetExpr()->Evaluate(&tuple,&(table_metadata_ptr_->schema_));
        vals.push_back(val);
    }
    return Tuple(vals,GetOutputSchema());
}
bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
    
   // 进行迭代器访问
    Tuple temp;
    auto predicate=plan_->GetPredicate();
    while(iter_!=end_iter_){
        *rid=(*iter_).second;
        ++iter_;
        // 使用predicate跳过不符条件的输出
        if(predicate==nullptr||predicate->Evaluate(tuple,plan_->OutputSchema()).GetAs<bool>()){
            *tuple=TransformOutPutSchema(temp);
            return true;
        }
        
    }
    return false;
}
    
}  // namespace bustub
