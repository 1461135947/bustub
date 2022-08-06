//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),left_executor_(std::move(left_executor)),right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    RID temp;
    left_res_=left_executor_->Next(&left_tuple_,&temp);
}
Tuple NestedLoopJoinExecutor::Join(Tuple *left,Tuple *right){
    std::vector<Value> res_values;
    // 将左右两个tuple组合成一个新的tuple
    for (auto const &col:GetOutputSchema()->GetColumns()){
       
        res_values.push_back(col.GetExpr()->EvaluateJoin(left, left_executor_->GetOutputSchema(),right, right_executor_->GetOutputSchema()));
    }

  return Tuple{res_values, GetOutputSchema()};
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { 
    Tuple right_tuple;
    RID temp_rid;
    auto predicate=plan_->Predicate();
    while (true){
        // 左边的表已经遍历完毕
        if(!left_res_){
            return false;
        }
        //右边的表遍历完毕需要移动左边的迭代器
        if(!right_executor_->Next(&right_tuple,&temp_rid)){
            right_executor_->Init();
            left_res_=left_executor_->Next(&left_tuple_,&temp_rid);
            continue;
        }
        // 进行谓词判断
        if(predicate==nullptr||predicate->EvaluateJoin(&left_tuple_,left_executor_->GetOutputSchema(),&right_tuple,right_executor_->GetOutputSchema()).GetAs<bool>()){
            *tuple=Join(&left_tuple_,&right_tuple);
            return true;
        }
    }
    return false;
}
    
}  // namespace bustub
