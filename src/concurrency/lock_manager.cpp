//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include <utility>
#include <vector>
#include<algorithm>
#include<stack>

namespace bustub {
  // 终止事务
void LockManager::AbortTransaction(Transaction *txn, AbortReason abort_reason) {
  // 设置事务状态并抛出异常交给上层处理
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(),abort_reason);
} 

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  auto isolationLevel=txn->GetIsolationLevel();
  // 读未提交不需要共享锁
  if(isolationLevel==IsolationLevel::READ_UNCOMMITTED){
    AbortTransaction(txn,AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 当前隔离级别属于可重复读并且处于锁的收缩阶段则不能获取锁
  if(isolationLevel==IsolationLevel::REPEATABLE_READ&&txn->GetState()==TransactionState::SHRINKING){
    AbortTransaction(txn,AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 当前事务已经获取锁
  if(txn->IsSharedLocked(rid)||txn->IsExclusiveLocked(rid)){
    return true;
  }
  // 开始加锁操作
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue=lock_table_[rid];
  l.unlock();
  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  // 将事务放入到请求队列中
  auto &lock_request=lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(),LockManager::LockMode::SHARED);
  // 等待前面请求的锁被满足
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });
  // 进行死锁检测后中断了当前事务
  if(txn->GetState()==TransactionState::ABORTED){
    AbortTransaction(txn,AbortReason::DEADLOCK);
    return false;
  }
  // 授权锁
  lock_request.granted=true;
  if(!txn->IsExclusiveLocked(rid)){
    txn->GetSharedLockSet()->emplace(rid);
  }
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 处于收缩阶段不能获取排它锁
  if(txn->GetState()==TransactionState::SHRINKING){
    AbortTransaction(txn,AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if(txn->IsExclusiveLocked(rid)){
    return true;
  }
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();
  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::EXCLUSIVE);
  // 等待所有的锁被释放
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortTransaction(txn, AbortReason::DEADLOCK);
    return false;
  }

  // 授权锁
  lock_request.granted_ = true;

  txn->GetExclusiveLockSet()->emplace(rid);

  return true;    
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 在锁的收缩阶段不能升级锁
  if(txn->GetState()==TransactionState::SHRINKING){
    AbortTransaction(txn,AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  if (lock_request_queue.upgrading_) {
    AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 开始进行锁的升级
  lock_request_queue.upgrading_ = true;
  // 进行一系列相关检查
  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  BUSTUB_ASSERT(it != lock_request_queue.request_queue_.end(), "Cannot find lock request when upgrade lock");
  BUSTUB_ASSERT(it->granted_, "Lock request has not be granted");
  BUSTUB_ASSERT(it->lock_mode_ == LockManager::LockMode::SHARED, "Lock request is not locked in SHARED mode");

  BUSTUB_ASSERT(txn->IsSharedLocked(rid), "Rid is not shared locked by transaction when upgrade");
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Rid is currently exclusive locked by transaction when upgrade");

  it->lock_mode_ = LockManager::LockMode::EXCLUSIVE;
  it->granted_ = false;
  // wait
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request = *it, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortTransaction(txn, AbortReason::DEADLOCK);
    return false;
  }

  // 将共享锁替换成排他锁
  it->granted_ = true;
  lock_request_queue.upgrading_ = false;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;

}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  // 如果是可重复读的隔离级别在锁释放时需要进入收缩状态
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 查找对应的锁请求
  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  BUSTUB_ASSERT(it != lock_request_queue.request_queue_.end(), "Cannot find lock request when unlock");
  // 删除锁请求
  auto following_it = lock_request_queue.request_queue_.erase(it);
  // 通知其他阻塞请求
  if (following_it != lock_request_queue.request_queue_.end() && !following_it->granted_ &&
      LockManager::IsLockCompatible(lock_request_queue, *following_it)) {
    lock_request_queue.cv_.notify_all();
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &v = waits_for_[t1];
  auto it = std::lower_bound(v.begin(), v.end(), t2);

  // 已经存在相应的边了;不需要添加
  if (it != v.end() && *it == t2) {
    return;
  }

  v.insert(it, t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &v = waits_for_[t1];
  // 存在边就删除
  auto it = std::find(v.begin(), v.end(), t2);

  if (it != v.end()) {
    v.erase(it);
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) { 
  std::vector<txn_id_t> vertices;
  std::transform(waits_for_.begin(), waits_for_.end(), std::back_inserter(vertices),
                 [](const auto &pair) { return pair.first; });
  std::sort(vertices.begin(), vertices.end());

  std::unordered_map<txn_id_t, LockManager::VisitedType> visited;

  for (auto &&v : vertices) {
    // vertex is NOT_VISITED
    if (auto it = visited.find(v); it == visited.end()) {
      std::stack<txn_id_t> stack;
      stack.push(v);
      visited.emplace(v, LockManager::VisitedType::IN_STACK);

      auto has_cycle = ProcessDFSTree(txn_id, &stack, &visited);
      if (has_cycle) {
        return true;
      }
    }
  }

  return false;
}

bool LockManager::ProcessDFSTree(txn_id_t *txn_id, std::stack<txn_id_t> *stack,
                                 std::unordered_map<txn_id_t, LockManager::VisitedType> *visited) {
  bool has_cycle = false;

  for (auto &&v : waits_for_[stack->top()]) {
    auto it = visited->find(v);

    // 寻找环
    if (it != visited->end() && it->second == LockManager::VisitedType::IN_STACK) {
      *txn_id = GetYoungestTransactionInCycle(stack, v);
      has_cycle = true;
      break;
    }

    // 进入没有遍历的节点
    if (it == visited->end()) {
      stack->push(v);
      visited->emplace(v, LockManager::VisitedType::IN_STACK);

      has_cycle = ProcessDFSTree(txn_id, stack, visited);
      if (has_cycle) {
        break;
      }
    }
  }

  visited->insert_or_assign(stack->top(), LockManager::VisitedType::VISITED);
  stack->pop();

  return has_cycle;
}
txn_id_t LockManager::GetYoungestTransactionInCycle(std::stack<txn_id_t> *stack, txn_id_t vertex) {
  txn_id_t max_txn_id = 0;
  std::stack<txn_id_t> tmp;
  tmp.push(stack->top());
  stack->pop();

  while (tmp.top() != vertex) {
    tmp.push(stack->top());
    stack->pop();
  }

  while (!tmp.empty()) {
    max_txn_id = std::max(max_txn_id, tmp.top());
    stack->push(tmp.top());
    tmp.pop();
  }

  return max_txn_id;
}
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> r;
  for (const auto &[txn_id, txn_id_v] : waits_for_) {
    std::transform(txn_id_v.begin(), txn_id_v.end(), std::back_inserter(r),
                   [&t1 = txn_id](const auto &t2) { return std::make_pair(t1, t2); });
  }
  return r;
}
// 构建事务的等待图
void LockManager::BuildWaitsForGraph() {
  for (const auto &it : lock_table_) {
    const auto queue = it.second.request_queue_;
    std::vector<txn_id_t> holdings;
    std::vector<txn_id_t> waitings;

    for (const auto &lock_request : queue) {
      // 获取队列中的所有节点
      const auto txn = TransactionManager::GetTransaction(lock_request.txn_id_);
      if (txn->GetState() == TransactionState::ABORTED) {
        continue;
      }

      if (lock_request.granted_) {
        holdings.push_back(lock_request.txn_id_);
      } else {
        waitings.push_back(lock_request.txn_id_);
      }
    }

    for (auto &&t1 : waitings) {
      for (auto &&t2 : holdings) {
        AddEdge(t1, t2);
      }
    }
  }
}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      if (!enable_cycle_detection_) {
        break;
      }

      waits_for_.clear();
      BuildWaitsForGraph();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        for (const auto &wait_on_txn_id : waits_for_[txn_id]) {
          auto wait_on_txn = TransactionManager::GetTransaction(wait_on_txn_id);
          std::unordered_set<RID> lock_set;
          lock_set.insert(wait_on_txn->GetSharedLockSet()->begin(), wait_on_txn->GetSharedLockSet()->end());
          lock_set.insert(wait_on_txn->GetExclusiveLockSet()->begin(), wait_on_txn->GetExclusiveLockSet()->end());
          for (auto locked_rid : lock_set) {
            lock_table_[locked_rid].cv_.notify_all();
          }
        }

        waits_for_.clear();
        BuildWaitsForGraph();
      }
    }
  }
}

}  // namespace bustub
