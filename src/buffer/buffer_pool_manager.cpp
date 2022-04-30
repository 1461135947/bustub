//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <list>
#include <unordered_map>
#include "include/common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> gard(latch_);
  auto it = page_table_.find(page_id);
  Page *page = nullptr;
  frame_id_t frame_id = 0;
  // P存在
  if (it != page_table_.end()) {
    frame_id = it->second;
    page = &pages_[frame_id];
    // 增加pin_count;然后返回
    // page->WLatch();
    page->pin_count_++;
    // page->WUnlatch();
    return page;
  }
  page = FindFrame(&frame_id);
  if (page == nullptr) {
    return page;
  }
  // page->WLatch();
  // 页中设置了脏位将数据写回
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  // 添加新的映射关系
  page_table_[page_id] = frame_id;
  replacer_->Unpin(frame_id);
  // 清空元数据
  page->page_id_ = page_id;
  page->ResetMemory();
  // 从磁盘中读取数据
  disk_manager_->ReadPage(page_id, page->data_);
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  // page->WUnlatch();
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> gard(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    auto frame_id = it->second;
    // pages_[frame_id].WLatch();
    // 更新is_dirty位
    if (is_dirty) {
      pages_[frame_id].is_dirty_ = true;
    }
    // 减少page的pin_count;
    pages_[frame_id].pin_count_--;
    // pages_[frame_id].WUnlatch();
    return true;
  }
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> gard(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 将page的内容刷新到磁盘中
    auto frame_id = it->second;
    Page *page = &pages_[frame_id];
    // page->RLatch();
    page->is_dirty_ = false;
    disk_manager_->WritePage(page_id, page->GetData());
    // page->RUnlatch();
    return true;
  }
  return false;
}

Page *BufferPoolManager::FindFrame(frame_id_t *frame_id) {
  Page *page = nullptr;
  if (free_list_.empty()) {
    size_t size = replacer_->Size();
    bool not_found = true;
    // 寻找一个没有pin住的页面
    for (size_t i = 0; i < size; i++) {
      if (replacer_->Victim(frame_id)) {
        page = &pages_[*frame_id];
        // page->RLatch();
        if (page->pin_count_ <= 0) {
          not_found = false;
          // 解除在map中的映射
          page_table_.erase(page->GetPageId());
          // page->RUnlatch();
          break;
        }
        // 否则将弹出的frame_id重新插入
        replacer_->Unpin(*frame_id);
        //page->RUnlatch();

      } else {
        LOG_ERROR("Victim fail");
      }
    }
    // 所有页面都被pin住
    if (not_found) {
      return nullptr;
    }

  } else {
    // 从free_list_中选取一个空的free
    *frame_id = free_list_.back();
    page = &pages_[*frame_id];
    free_list_.pop_back();
  }
  return page;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> gard(latch_);
  auto page_id_ = disk_manager_->AllocatePage();

  frame_id_t frame_id = 0;
  Page *page = FindFrame(&frame_id);
  if (page == nullptr) {
    return page;
  }
  // page->WLatch();
  // 如果frame绑定其他page需要将page中的数据写入的磁盘中
  if (page->IsDirty()) {
    page->is_dirty_ = false;
    disk_manager_->WritePage(page->page_id_, page->GetData());
  }
  // 添加新的映射关系
  page_table_[page_id_] = frame_id;
  replacer_->Unpin(frame_id);
  // 清空元数据
  page->page_id_ = page_id_;
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  if (page_id != nullptr) {
    *page_id = page_id_;
  }
  // page->WUnlatch();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> gard(latch_);
  auto it = page_table_.find(page_id);
  // 不存在
  if (it == page_table_.end()) {
    return true;
  }
  {
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    // page->WLatch();
    // 有其他线程在引用当前page；不能进行删除
    if (page->pin_count_ > 0) {
      // page->WUnlatch();
      return false;
    }
    page_table_.erase(page->GetPageId());
    free_list_.push_back(frame_id);
    replacer_->Pin(frame_id);
    // 移除page；重置相关的元数据
    disk_manager_->DeallocatePage(page->GetPageId());
    page->page_id_ = INVALID_PAGE_ID;
    page->ResetMemory();
    page->is_dirty_ = false;
    page->pin_count_ = 0;
    // page->WUnlatch();
  }
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (auto it : page_table_) {
    this->FlushPageImpl(it.first);
  }
  // You can do it!
}

}  // namespace bustub
