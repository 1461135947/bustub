//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>:: root_lock_count = 0;
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 先定位到叶子节点
  
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, false,Operate_Type::OP_READ,transaction);
  if (leaf_page == nullptr)
    return false;
  result->clear();
  ValueType val;
  // 在叶子节点中查找key
  bool res = leaf_page->Lookup(key, &val, comparator_);
  if (res) {
    result->push_back(val);
  }
  FreePagesInTransaction(false,transaction,leaf_page->GetPageId());
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LockRootPageId(true);
  // 如果是空的二叉树需要构建root节点
  if (IsEmpty()) {
    StartNewTree(key, value);
    TryUnlockRootPageId(true);
    return true;
  }
  TryUnlockRootPageId(true);
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;
  // 新建一个页
  Page *page = buffer_pool_manager_->NewPage(&root_page_id);
  if (page == nullptr) {
    throw "out of memory";
  }
  root_page_id_ = root_page_id;
  // 转换成Leaf页
  B_PLUS_TREE_LEAF_PAGE_TYPE *root_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  root_page->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId(1);
  // 插入数据
  root_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 先定位到叶子节点
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, false,Operate_Type::OP_INSERT,transaction);
  ValueType old_val;
  // 叶子节点中存在key
  if (leaf_page->Lookup(key, &old_val, comparator_)) {
    FreePagesInTransaction(true,transaction);
    return false;
  } else {
    int size = leaf_page->Insert(key, value, comparator_);
    //达到上限开始分裂页
    if (size == leaf_max_size_) {
      B_PLUS_TREE_LEAF_PAGE_TYPE *recipent = Split(leaf_page,transaction);
      KeyType key = recipent->KeyAt(0);
      InsertIntoParent(leaf_page, key, recipent, transaction);
    }
  }
  FreePagesInTransaction(true,transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node,Transaction *transaction) {
  // 构建一个新的页
  page_id_t page_id;
  Page *const recipient_page = buffer_pool_manager_->NewPage(&page_id);
  if (recipient_page == nullptr) {
    throw "out of memory";
  }
  recipient_page->WLatch();
  transaction->AddIntoPageSet(recipient_page);
  
  //移动一半元素到新构建的页
  N *recipient = reinterpret_cast<N *>(recipient_page->GetData());
  recipient->Init(page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(recipient, buffer_pool_manager_);

  return recipient;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(new_page != nullptr);
    assert(new_page->GetPinCount() == 1);
    B_PLUS_TREE_INTERNAL_PAGE *new_root_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(new_page->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
    // unpin 两个页
    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }
  // 获取父节点
  page_id_t parent_id = old_node->GetParentPageId();
  auto *page = FetchPage(parent_id);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
  new_node->SetParentPageId(parent_id);
  // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  // 将分裂后节点的值插入
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 父节点达到限制进行分裂
  if (parent->GetSize() == parent->GetMaxSize()) {
    // begin /* Split Parent */
    B_PLUS_TREE_INTERNAL_PAGE *newLeafPage = Split(parent,transaction);  // new page need unpin
    InsertIntoParent(parent, newLeafPage->KeyAt(0), newLeafPage, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  // 先定位到叶子节点;然后从叶子节点中删除数据
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, false,Operate_Type::OP_DELETE,transaction);
  int size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  // 判断是否需要进行页面合并或者重组操作
  if (size < leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_page, transaction);
  }

  FreePagesInTransaction(true,transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // root节点需要特殊处理
  if (node->IsRootPage()) {
    bool delete_root= AdjustRoot(node);
    if(delete_root){
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return delete_root;
  }
  // 寻找兄弟节点
  N *sibling_node;
  bool is_next_node = FindSibling(node, &sibling_node,transaction);
  B_PLUS_TREE_INTERNAL_PAGE *parent_node =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(FetchPage(node->GetParentPageId()));
  // 根据兄弟节点和当点前节点的KV数量决定是合并还是偷取
  // 两者数量和小于maxsize；进行合并
  if (node->GetSize() + sibling_node->GetSize() < node->GetMaxSize()) {
    // 假设node节点在后;sibling节点在前
    if (is_next_node) {
      std::swap(node, sibling_node);
    }
    // 进行合并操作
    int index = parent_node->ValueIndex(node->GetPageId());
    Coalesce(sibling_node, node, parent_node, index, transaction);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return true;
  }
  // 反之进行偷取
  int middle_index;
  if (is_next_node) {
    middle_index = parent_node->ValueIndex(sibling_node->GetPageId());
  } else {
    middle_index = parent_node->ValueIndex(node->GetPageId());
  }

  KeyType middle_key = parent_node->KeyAt(middle_index);
  Redistribute(sibling_node, node, middle_key, is_next_node);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N *node, N **sibling,Transaction *transaction) {
  // 从父节点中找出当前节点相邻的两个兄弟节点
  BPlusTreePage *temp_node = FetchPage(node->GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(temp_node);
  int index = parent_page->ValueIndex(node->GetPageId());
  int sibling_index;
  // 优先选择前一个节点
  bool res;
  if (index == 0) {
    res = true;
    sibling_index = index + 1;
  } else {
    res = false;
    sibling_index = index - 1;
  }
  *sibling = reinterpret_cast<N *>(CrabingProtocalFetchPage(parent_page->ValueAt(sibling_index),Operate_Type::OP_DELETE,-1,transaction));
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  return res;
}
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *const &neighbor_node, N *const &node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *const &parent, int index,
                              Transaction *transaction) {
  KeyType middle_key = parent->KeyAt(index);
  // 将node中所有的数据移动到兄弟节点上
  node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);

  // 删除node节点
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  // buffer_pool_manager_->DeletePage(node->GetPageId());
  // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  
  // 删除父节点中当前节点的数据
  parent->Remove(index);
  if (parent->GetSize() <= parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, const KeyType &middle_key, bool is_next_node) {
  // 进行KV偷取
  // neighbor_node是node的前一个节点
  if (is_next_node) {
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
  } else {
    // neighbor_node是的后一个节点
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
  }
  // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 如果root节点是叶子节点就是case2的情况
  if (old_root_node->IsLeafPage()) {
    // 解除page的引用;删除root节点
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    // buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    // 重置b+树的root_page_id
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  // 如果是内部节点并且只有一个key
  if (old_root_node->GetSize() == 1) {
    // 删除root节点中的key并且将唯一的子节点提升为root节点
    B_PLUS_TREE_INTERNAL_PAGE *root_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
    page_id_t child_page_id = root_page->RemoveAndReturnOnlyChild();
    root_page_id_ = child_page_id;
    UpdateRootPageId();
    // 设置新的root page的parent_page_id
    B_PLUS_TREE_INTERNAL_PAGE *new_root =
        reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    // 删除旧的root页
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    // buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
    // 让外层知道不用释放页
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key;
  // 定位到最左边的页
  auto leaf = FindLeafPage(key, true);
  TryUnlockRootPageId(false);
  
  return INDEXITERATOR_TYPE(leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // 先定位到页
  
  auto leaf = FindLeafPage(key, false,Operate_Type::OP_READ);
  TryUnlockRootPageId(false);
  if (leaf == nullptr) {
    return INDEXITERATOR_TYPE(leaf, 0, buffer_pool_manager_);
  }
  // 再定位到页中的位置
  int index = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost,Operate_Type operate,Transaction *transaction) {
  
  bool is_write=(operate!=Operate_Type::OP_READ);
  LockRootPageId(is_write);
  // 首先判断当前是否是空的B+树
  if (IsEmpty()) {
    TryUnlockRootPageId(is_write);
    return nullptr;
  }

  auto page = CrabingProtocalFetchPage(root_page_id_,operate,-1,transaction);
  page_id_t next;
  // 递归查找符合条件的子树
  for (page_id_t cur = root_page_id_; !page->IsLeafPage(); page = CrabingProtocalFetchPage(next,operate,cur,transaction),cur = next) {
    B_PLUS_TREE_INTERNAL_PAGE *internalPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
    if (leftMost) {
      next = internalPage->ValueAt(0);
    } else {
      next = internalPage->Lookup(key, comparator_);
    }
  }
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
}

/**
 * 统一的Unping页操作
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(bool is_write, Transaction *transaction, page_id_t cur) {

  TryUnlockRootPageId(is_write);
  // transaction为空则直接释放当前节点
  if (transaction == nullptr) {
    assert(!is_write && cur >= 0);
    UnLock(false,cur);
    buffer_pool_manager_->UnpinPage(cur,false);
    return;
  }
  for (Page *page : *transaction->GetPageSet()) {
    int curPid = page->GetPageId();
    UnLock(is_write,page);
    buffer_pool_manager_->UnpinPage(curPid,is_write);
    if (transaction->GetDeletedPageSet()->find(curPid) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(curPid);
      transaction->GetDeletedPageSet()->erase(curPid);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLock(bool exclusive,page_id_t pageId) {
    auto page = buffer_pool_manager_->FetchPage(pageId);
    UnLock(exclusive,page);
    buffer_pool_manager_->UnpinPage(pageId,exclusive);
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLock(bool is_write,Page *page){
    if(is_write){
      page->WUnlatch();
    }else{
      page->RUnlatch();
    }
  }
/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id,Operate_Type operate,page_id_t previous, Transaction *transaction) {
  bool is_write = operate != Operate_Type::OP_READ;
  auto page = buffer_pool_manager_->FetchPage(page_id);
  // 先获取当前节点的锁
  Lock(is_write,page);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // 确认节点安全后释放上一阶段的锁
  if (previous > 0 && (!is_write || tree_page->IsSafe(operate))) {
    FreePagesInTransaction(is_write,transaction,previous);
  }
  if (transaction != nullptr)
    transaction->AddIntoPageSet(page);
  return tree_page;
}


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

}  // namespace bustub
