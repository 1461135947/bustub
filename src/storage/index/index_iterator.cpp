/**
 * index_iterator.cpp
 */
#include <cassert>
#include<iostream>
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *buffer_pool_manager)
    : index_(index), leaf_(leaf), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    UnlockAndUnPin();
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_ == nullptr || index_ >= leaf_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  // std::cout<<"增加迭代器"<<std::endl;
  index_++;
  //已经遍历完当前页
  if ( index_ >= leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    UnlockAndUnPin();
    if (next_page_id == INVALID_PAGE_ID) {
      // 释放最后一个被引用的页
      // buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
      index_ = 0;
      leaf_ = nullptr;
    } else {
      // buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
      Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      next_page->RLatch();
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      index_ = 0;
    }
  }
  return *this;
}
INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::UnlockAndUnPin() {
    buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  }
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
