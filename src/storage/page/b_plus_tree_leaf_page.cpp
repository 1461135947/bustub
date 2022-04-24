//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
// 使用二分查找找到第一个大于
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() >= 0);
  // 二分查找
  int left = 0;
  int right = GetSize() - 1;
  int mid;
  while (left <= right) {
    mid = ((right - left) >> 1) + left;
    if (comparator(array[mid].first, key) >= 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  return right + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 找到第一大于key的位置
  int index = KeyIndex(key, comparator);
  assert(index >= 0);
  for (int i = GetSize() - 1; i >= index; i--) {
    array[i + 1] = array[i];
  }
  array[index].first = key;
  array[index].second = value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int index = size / 2;
  // 设置节点的next_page_id
  SetNextPageId(recipient->GetPageId());
  recipient->CopyNFrom(array + index, size - index);
  SetSize(index);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = 0; i < size; i++) {
    array[i] = items[i];
  }
  SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  // 查找第一个大于等于key的索引
  int index = KeyIndex(key, comparator);
  // 比较索引的值是否等于key
  if (index < GetSize() && comparator(array[index].first, key) == 0) {
    *value = array[index].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int index = KeyIndex(key, comparator);
  if (index == GetSize()) {
    return index;
  }
  if (comparator(array[index].first, key) == 0) {
    for (; index < GetSize() - 1; index++) {
      array[index] = array[index + 1];
    }
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  // 更新sibling page
  recipient->next_page_id_ = next_page_id_;
  int offset = recipient->GetSize();
  // 拷贝数据
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    recipient->array[offset + i] = array[i];
  }
  recipient->IncreaseSize(size);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                  BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  recipient->CopyLastFrom(array[0]);
  for (int i = 0; i < size - 1; i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
  // 修改父节点中的数据
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int size = GetSize();
  array[size] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                   BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array[GetSize() - 1]);
  IncreaseSize(-1);
  // 修改父节点中的数据
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(recipient->GetPageId()), recipient->array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int index = GetSize(); index > 0; index--) {
    array[index] = array[index - 1];
  }
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
