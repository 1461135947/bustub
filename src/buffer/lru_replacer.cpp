//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_num_pages_(num_pages), num_pages_(0) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(lock);
  if (this->num_pages_ != 0) {
    num_pages_--;
    // 移除队列中的最后一个元素并且解除map中的映射
    auto id = lru.back();
    lru.pop_back();
    mp.erase(id);
    if (frame_id != nullptr) {
      *frame_id = id;
    }
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // 先检查要插入的元素是否在缓冲区中;
  std::lock_guard<std::mutex> guard(lock);
  auto it = mp.find(frame_id);
  if (it != mp.end()) {
    lru.erase(it->second);
    num_pages_--;
    mp.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(lock);
  // 先检查要插入的元素是否在缓冲区中;如果在缓冲区中则需要移除该元素
  auto it = mp.find(frame_id);
  if (it != mp.end()) {
    return;
  }
  // 如果当前容量已经到达最大的容量则需要删除一个元素
  if (num_pages_ == max_num_pages_) {
    lock.unlock();
    this->Victim(nullptr);
    lock.lock();
  }
  // 将元素插入到队列的头部；并且更新map
  auto lru_it = lru.insert(lru.begin(), frame_id);
  mp[frame_id] = lru_it;
  num_pages_++;
}

size_t LRUReplacer::Size() { return this->num_pages_; }  // namespace bustub
}  // namespace bustub
