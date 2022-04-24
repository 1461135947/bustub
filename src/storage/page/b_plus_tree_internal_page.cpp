//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_internal_page.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include "common/exception.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // 初始化page的相关成员字段
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
/**
 * 在array中获取下标为index的key
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  MappingType mappingType = array[index];
  return mappingType.first;
}
/**
 * 在array中设置下标为index的key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
/**
 * 在array中获取值为value的index
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
/**
 * 获取下标为index的value
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // assert(GetSize() >= 1);
  int left = 1;
  int right = GetSize() - 1;
  int mid;
  // 使用二分查找进行比较
  while (left <= right) {
    mid = ((right - left) >> 1) + left;
    if (comparator(array[mid].first, key) <= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return array[left - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1].second = new_value;
  array[1].first = new_key;
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(INVALID_PAGE_ID);
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 先定位到old_value的位置
  int index = ValueIndex(old_value);
  int end = GetSize();
  // 将后面的元素向后移动
  for (int i = end - 1; i > index; i--) {
    array[i + 1] = array[i];
  }
  array[index + 1].first = new_key;
  array[index + 1].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 拷贝数据
  int size = GetSize();
  recipient->CopyNFrom(array + size / 2, size - size / 2, buffer_pool_manager);
  SetSize(size / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
/**
 * 从items中拷贝size个数据到page中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size; i++) {
    // 拷贝数据
    MappingType temp = items[i];
    array[i].first = temp.first;
    array[i].second = temp.second;
    // 修改子节点的parent id
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(temp.second)->GetData());
    page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(temp.second, true);
  }
  SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  int size = GetSize();
  // 用后面的数据覆盖前面的数据
  while (index < size - 1) {
    array[index].first = array[index + 1].first;
    array[index].second = array[index + 1].second;
    index++;
  }
  // 减少size
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType res = array[0].second;
  SetSize(0);
  return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  buffer_pool_manager->FetchPage(recipient->GetPageId());
  buffer_pool_manager->FetchPage(GetPageId());

  // 从父节点中删除middle_key
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
  parent_page->Remove(parent_page->ValueIndex(GetPageId()));
  SetKeyAt(0, middle_key);
  int offeset = recipient->GetSize();
  // 拷贝数据
  for (int i = 0; i < GetSize(); i++) {
    recipient->array[i + offeset] = array[i];
    // 修改子页的parent_page id
    BPlusTreePage *child_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array[i].second)->GetData());
    child_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(array[i].second, true);
  }

  // 修改两个页中的KV键值对的数量
  recipient->IncreaseSize(GetSize());
  SetSize(0);
  assert(recipient->GetSize() <= GetMaxSize());
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  buffer_pool_manager->UnpinPage(GetParentPageId(), false);
  buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  //修改父节点页中的数据
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
  int parent_index = parent_page->ValueIndex(GetPageId());
  ValueType val = array[0].second;
  parent_page->SetKeyAt(parent_index, array[1].first);

  // 修改移动的page的parent id为recipient的id
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(val)->GetData());
  child_page->SetParentPageId(recipient->GetPageId());

  // 移动KV键值对
  int recipient_size = recipient->GetSize();
  recipient->array[recipient_size].first = middle_key;
  recipient->array[recipient_size].second = val;
  recipient->IncreaseSize(1);
  Remove(0);
  buffer_pool_manager->UnpinPage(val, true);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  buffer_pool_manager->FetchPage(GetPageId());
  int index = GetSize();
  array[index].first = pair.first;
  array[index].second = pair.second;
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(pair.second)->GetData());
  page->SetParentPageId(GetPageId());
  IncreaseSize(1);
  buffer_pool_manager->UnpinPage(pair.second, true);
  buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  //修改父节点页中的数据
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
  int parent_index = parent_page->ValueIndex(recipient->GetPageId());
  ValueType val = array[GetSize() - 1].second;
  parent_page->SetKeyAt(parent_index, array[GetSize() - 1].first);

  // 修改移动的page的parent id为recipient的id
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(val)->GetData());
  child_page->SetParentPageId(recipient->GetPageId());

  // 移动KV键值对
  int recipient_size = recipient->GetSize();
  for (int i = recipient_size; i > 0; i--) {
    recipient->array[i].first = recipient->array[i - 1].first;
    recipient->array[i].second = recipient->array[i - 1].second;
  }
  recipient->array[1].first = middle_key;
  recipient->array[0].second = val;
  recipient->IncreaseSize(1);
  Remove(GetSize() - 1);
  buffer_pool_manager->UnpinPage(val, true);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
