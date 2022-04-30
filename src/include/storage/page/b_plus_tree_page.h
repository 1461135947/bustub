//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>


// define page type enum
// 页的类型:内部节点页、叶子节点页
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };
enum class Operate_Type{OP_READ=0,OP_DELETE,OP_INSERT};

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
/**
 * 记录了B+树页的元数据信息;总共24字节包括如下资金端
 * PageType(4字节):内部节点还是叶子结点
 * LSN(4字节):日志序列号
 * CurrentSize(4字节):当前页中存储的KV键值对
 * MaxSize(4字节): 页中可以存储的最大KV键值对
 * ParentPageID(4字节):父节点的PageID;用于定位父节点
 * PageID(4字节): 节点的PageID
 */
class BPlusTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);
  bool IsSafe(Operate_Type operate);

 private:
  // member variable, attributes that both internal and leaf page share
  // 页的类型:内部页和叶子页
  IndexPageType page_type_ __attribute__((__unused__));
  // 日志序列号 在project4中使用
  lsn_t lsn_ __attribute__((__unused__));
  // kv键值对的数量
  int size_ __attribute__((__unused__));
  // 当前页可以存储的kv键值对的数量
  int max_size_ __attribute__((__unused__));
  // 父节点的pageid
  page_id_t parent_page_id_ __attribute__((__unused__));
  // 当前页的pageid
  page_id_t page_id_ __attribute__((__unused__));
};

}  // namespace bustub
