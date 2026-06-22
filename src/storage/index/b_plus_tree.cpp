//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.template AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.template As<BPlusTreeHeaderPage>();
  return header->root_page_id_ == INVALID_PAGE_ID;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.template As<BPlusTreeHeaderPage>();
  return header->root_page_id_;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;

  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.template As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto curr_guard = bpm_->ReadPage(ctx.root_page_id_);
  ctx.read_set_.push_back(std::move(curr_guard));

  while (true) {
    auto page = ctx.read_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal = ctx.read_set_.back().template As<InternalPage>();
    auto child_page_id = internal->Lookup(key, comparator_);
    auto child_guard = bpm_->ReadPage(child_page_id);

    ctx.read_set_.pop_front();
    ctx.read_set_.push_back(std::move(child_guard));
  }

  auto leaf = ctx.read_set_.back().template As<LeafPage>();
  int idx = leaf->KeyIndex(key, comparator_);
  if (idx < leaf->GetSize() && comparator_(leaf->KeyAt(idx), key) == 0) {
    if (!leaf->IsTombstoned(idx)) {
      result->push_back(leaf->ValueAt(idx));
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  std::scoped_lock mutation_lock(mutation_mutex_);
  // 1. 乐观插入路径 (Optimistic Path)
  {
    Context ctx;
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto header = header_guard.template As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = header->root_page_id_;

    if (ctx.root_page_id_ != INVALID_PAGE_ID) {
      auto curr_guard = bpm_->ReadPage(ctx.root_page_id_);
      ctx.read_set_.push_back(std::move(curr_guard));
      header_guard = ReadPageGuard();  // 尽早释放 header 读锁

      while (true) {
        auto page = ctx.read_set_.back().template As<BPlusTreePage>();
        if (page->IsLeafPage()) {
          break;
        }
        auto internal = ctx.read_set_.back().template As<InternalPage>();
        auto child_page_id = internal->Lookup(key, comparator_);
        auto child_guard = bpm_->ReadPage(child_page_id);
        ctx.read_set_.pop_front();
        ctx.read_set_.push_back(std::move(child_guard));
      }

      // 定位目标叶子页面 ID 并对其加写锁
      page_id_t leaf_page_id = ctx.read_set_.back().GetPageId();
      ctx.read_set_.clear();
      auto leaf_guard = bpm_->WritePage(leaf_page_id);
      ctx.read_set_.clear();  // 释放该路径上的所有读锁

      auto leaf = leaf_guard.template AsMut<LeafPage>();
      bool leaf_matches_key = leaf->GetSize() > 0 && comparator_(key, leaf->KeyAt(0)) >= 0;
      if (leaf_matches_key && comparator_(key, leaf->KeyAt(leaf->GetSize() - 1)) > 0 &&
          leaf->GetNextPageId() != INVALID_PAGE_ID) {
        auto next_leaf_guard = bpm_->ReadPage(leaf->GetNextPageId());
        auto next_leaf = next_leaf_guard.template As<LeafPage>();
        leaf_matches_key = next_leaf->GetSize() == 0 || comparator_(key, next_leaf->KeyAt(0)) < 0;
      }

      // 乐观校验：如果插入后不会导致叶子分裂，则可以直接就地插入
      if (leaf_matches_key && leaf->GetSize() < leaf_max_size_ - 1) {
        int idx = leaf->KeyIndex(key, comparator_);
        if (idx < leaf->GetSize() && comparator_(leaf->KeyAt(idx), key) == 0) {
          if (leaf->IsTombstoned(idx)) {
            leaf->ReviveAt(idx, value);
            return true;
          }
          return false;  // 重复键
        }
        leaf->InsertAt(idx, key, value);
        return true;
      }
      // 不安全，我们将退回，走悲观路径
    }
  }

  // 2. 悲观插入路径 (Pessimistic Path)
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    page_id_t root_id = bpm_->NewPage();
    auto root_guard = bpm_->WritePage(root_id);
    auto root = root_guard.template AsMut<LeafPage>();
    root->Init(leaf_max_size_);
    root->InsertAt(0, key, value);
    header->root_page_id_ = root_id;
    return true;
  }

  auto curr_guard = bpm_->WritePage(ctx.root_page_id_);
  auto root_page = curr_guard.template As<BPlusTreePage>();

  bool root_safe = false;
  if (root_page->IsLeafPage()) {
    root_safe = root_page->GetSize() < leaf_max_size_ - 1;
  } else {
    root_safe = root_page->GetSize() < internal_max_size_;
  }
  if (root_safe) {
    ctx.header_page_ = std::nullopt;
  }
  ctx.write_set_.push_back(std::move(curr_guard));

  while (true) {
    auto page = ctx.write_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal = ctx.write_set_.back().template As<InternalPage>();
    auto child_page_id = internal->Lookup(key, comparator_);
    auto child_guard = bpm_->WritePage(child_page_id);
    auto child_page = child_guard.template As<BPlusTreePage>();

    bool child_safe = false;
    if (child_page->IsLeafPage()) {
      child_safe = child_page->GetSize() < leaf_max_size_ - 1;
    } else {
      child_safe = child_page->GetSize() < internal_max_size_;
    }
    if (child_safe) {
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(child_guard));
  }

  auto leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  int idx = leaf->KeyIndex(key, comparator_);
  if (idx < leaf->GetSize() && comparator_(leaf->KeyAt(idx), key) == 0) {
    if (leaf->IsTombstoned(idx)) {
      leaf->ReviveAt(idx, value);
      return true;
    }
    return false;
  }

  leaf->InsertAt(idx, key, value);
  if (leaf->GetSize() == leaf->GetMaxSize()) {
    Split(&ctx);
  }

  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(Context *ctx) {
  page_id_t new_child_id = INVALID_PAGE_ID;
  KeyType push_up_key;

  while (!ctx->write_set_.empty()) {
    auto curr_guard = std::move(ctx->write_set_.back());
    ctx->write_set_.pop_back();
    auto curr_page = curr_guard.template AsMut<BPlusTreePage>();

    if (curr_page->IsLeafPage()) {
      auto leaf = curr_guard.template AsMut<LeafPage>();
      page_id_t new_leaf_id = bpm_->NewPage();
      auto new_leaf_guard = bpm_->WritePage(new_leaf_id);
      auto new_leaf = new_leaf_guard.template AsMut<LeafPage>();
      new_leaf->Init(leaf_max_size_);

      auto entries = leaf->GetEntries();
      int mid = entries.size() / 2;

      std::vector<typename LeafPage::LeafEntry> first_half(entries.begin(), entries.begin() + mid);
      std::vector<typename LeafPage::LeafEntry> second_half(entries.begin() + mid, entries.end());

      leaf->SetEntries(first_half);
      new_leaf->SetEntries(second_half);

      new_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_leaf_id);

      new_child_id = new_leaf_id;
      push_up_key = new_leaf->KeyAt(0);

    } else {
      auto internal = curr_guard.template AsMut<InternalPage>();
      internal->Insert(push_up_key, new_child_id, comparator_);

      if (internal->GetSize() <= internal->GetMaxSize()) {
        return;
      }

      int size = internal->GetSize();
      int mid = size / 2;
      push_up_key = internal->KeyAt(mid);

      page_id_t new_internal_id = bpm_->NewPage();
      auto new_internal_guard = bpm_->WritePage(new_internal_id);
      auto new_internal = new_internal_guard.template AsMut<InternalPage>();
      new_internal->Init(internal_max_size_);

      new_internal->SetSize(size - mid);
      new_internal->SetValueAt(0, internal->ValueAt(mid));
      for (int i = mid + 1; i < size; ++i) {
        new_internal->SetKeyAt(i - mid, internal->KeyAt(i));
        new_internal->SetValueAt(i - mid, internal->ValueAt(i));
      }
      internal->SetSize(mid);

      new_child_id = new_internal_id;
    }
  }

  page_id_t new_root_id = bpm_->NewPage();
  auto new_root_guard = bpm_->WritePage(new_root_id);
  auto new_root = new_root_guard.template AsMut<InternalPage>();
  new_root->Init(internal_max_size_);
  new_root->SetSize(2);
  new_root->SetValueAt(0, ctx->root_page_id_);
  new_root->SetKeyAt(1, push_up_key);
  new_root->SetValueAt(1, new_child_id);

  auto header = ctx->header_page_->template AsMut<BPlusTreeHeaderPage>();
  header->root_page_id_ = new_root_id;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  std::scoped_lock mutation_lock(mutation_mutex_);

  {
    Context ctx;
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto header = header_guard.template As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = header->root_page_id_;
    if (ctx.root_page_id_ == INVALID_PAGE_ID) {
      return;
    }

    auto curr_guard = bpm_->ReadPage(ctx.root_page_id_);
    ctx.read_set_.push_back(std::move(curr_guard));
    header_guard = ReadPageGuard();

    while (true) {
      auto page = ctx.read_set_.back().template As<BPlusTreePage>();
      if (page->IsLeafPage()) {
        break;
      }
      auto internal = ctx.read_set_.back().template As<InternalPage>();
      auto child_page_id = internal->Lookup(key, comparator_);
      auto child_guard = bpm_->ReadPage(child_page_id);
      ctx.read_set_.pop_front();
      ctx.read_set_.push_back(std::move(child_guard));
    }

    page_id_t leaf_page_id = ctx.read_set_.back().GetPageId();
    ctx.read_set_.clear();
    auto leaf_guard = bpm_->WritePage(leaf_page_id);
    auto leaf = leaf_guard.template AsMut<LeafPage>();
    bool leaf_matches_key = leaf->GetSize() > 0 && comparator_(key, leaf->KeyAt(0)) >= 0;
    if (leaf_matches_key && comparator_(key, leaf->KeyAt(leaf->GetSize() - 1)) > 0 &&
        leaf->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_leaf_guard = bpm_->ReadPage(leaf->GetNextPageId());
      auto next_leaf = next_leaf_guard.template As<LeafPage>();
      leaf_matches_key = next_leaf->GetSize() == 0 || comparator_(key, next_leaf->KeyAt(0)) < 0;
    }

    if (leaf_matches_key) {
      int idx = leaf->KeyIndex(key, comparator_);
      if (idx >= leaf->GetSize() || comparator_(leaf->KeyAt(idx), key) != 0 || leaf->IsTombstoned(idx)) {
        return;
      }

      bool will_shrink = LEAF_PAGE_TOMB_CNT == 0 || leaf->GetTombstones().size() == LEAF_PAGE_TOMB_CNT;
      int size_after_delete = leaf->GetSize() - (will_shrink ? 1 : 0);
      bool leaf_safe = ctx.IsRootPage(leaf_page_id) ? size_after_delete > 0 : size_after_delete >= leaf->GetMinSize();
      if (leaf_safe) {
        leaf->InsertTombstone(idx);
        return;
      }
    }
  }

  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  auto curr_guard = bpm_->WritePage(ctx.root_page_id_);
  auto root_page = curr_guard.template As<BPlusTreePage>();

  bool root_safe = false;
  if (root_page->IsLeafPage()) {
    root_safe = root_page->GetSize() > 1;
  } else {
    root_safe = root_page->GetSize() > 2;
  }
  if (root_safe) {
    ctx.header_page_ = std::nullopt;
  }
  ctx.write_set_.push_back(std::move(curr_guard));

  while (true) {
    auto page = ctx.write_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal = ctx.write_set_.back().template As<InternalPage>();
    auto child_page_id = internal->Lookup(key, comparator_);
    auto child_guard = bpm_->WritePage(child_page_id);
    auto child_page = child_guard.template As<BPlusTreePage>();

    bool child_safe = child_page->GetSize() > child_page->GetMinSize();
    if (child_safe) {
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(child_guard));
  }

  auto leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  int idx = leaf->KeyIndex(key, comparator_);
  if (idx < leaf->GetSize() && comparator_(leaf->KeyAt(idx), key) == 0) {
    if (leaf->IsTombstoned(idx)) {
      return;
    }
    leaf->InsertTombstone(idx);

    if (ctx.IsRootPage(ctx.write_set_.back().GetPageId())) {
      if (leaf->GetSize() == 0) {
        header->root_page_id_ = INVALID_PAGE_ID;
      }
      return;
    }

    if (leaf->GetSize() < leaf->GetMinSize()) {
      CoalesceOrRedistribute(&ctx);
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(Context *ctx) {
  while (ctx->write_set_.size() > 1) {
    auto curr_guard = std::move(ctx->write_set_.back());
    ctx->write_set_.pop_back();
    auto curr_page = curr_guard.template AsMut<BPlusTreePage>();

    auto parent_guard = &ctx->write_set_.back();
    auto parent = parent_guard->template AsMut<InternalPage>();

    if (parent->GetSize() <= 1) {
      continue;
    }

    int node_idx = parent->ValueIndex(curr_guard.GetPageId());

    int sibling_idx = (node_idx > 0) ? (node_idx - 1) : (node_idx + 1);
    page_id_t sibling_id = parent->ValueAt(sibling_idx);
    auto sibling_guard = bpm_->WritePage(sibling_id);

    if (curr_page->IsLeafPage()) {
      auto leaf = curr_guard.template AsMut<LeafPage>();
      auto sibling = sibling_guard.template AsMut<LeafPage>();
      auto get_effective_leaf_size = [](const std::vector<typename LeafPage::LeafEntry> &entries) {
        size_t tomb_count = 0;
        for (const auto &entry : entries) {
          if (entry.is_tomb_) {
            tomb_count++;
          }
        }
        size_t applied_tombs = tomb_count > LEAF_PAGE_TOMB_CNT ? tomb_count - LEAF_PAGE_TOMB_CNT : 0;
        return entries.size() - applied_tombs;
      };

      auto leaf_entries = leaf->GetEntries();
      auto sibling_entries = sibling->GetEntries();
      std::vector<typename LeafPage::LeafEntry> combined_entries;
      if (node_idx > sibling_idx) {
        combined_entries = sibling_entries;
        auto right_entries = leaf_entries;
        for (auto &entry : right_entries) {
          if (entry.is_tomb_) {
            entry.recency_ += sibling->GetTombstones().size();
          }
        }
        combined_entries.insert(combined_entries.end(), right_entries.begin(), right_entries.end());
      } else {
        combined_entries = leaf_entries;
        auto right_entries = sibling_entries;
        for (auto &entry : right_entries) {
          if (entry.is_tomb_) {
            entry.recency_ += leaf->GetTombstones().size();
          }
        }
        combined_entries.insert(combined_entries.end(), right_entries.begin(), right_entries.end());
      }

      if (get_effective_leaf_size(combined_entries) < static_cast<size_t>(leaf_max_size_)) {
        // Coalesce
        if (node_idx > sibling_idx) {
          sibling->SetEntries(combined_entries);
          sibling->SetNextPageId(leaf->GetNextPageId());
          parent->RemoveAt(node_idx);
        } else {
          leaf->SetEntries(combined_entries);
          leaf->SetNextPageId(sibling->GetNextPageId());
          parent->RemoveAt(sibling_idx);
        }
      } else {
        // Redistribute
        if (node_idx > sibling_idx) {
          auto left_entries = sibling->GetEntries();
          auto right_entries = leaf->GetEntries();
          auto moved_entry = left_entries.back();
          left_entries.pop_back();
          if (moved_entry.is_tomb_) {
            moved_entry.recency_ = leaf->GetTombstones().size();
          }
          right_entries.insert(right_entries.begin(), moved_entry);
          sibling->SetEntries(left_entries);
          leaf->SetEntries(right_entries);
          parent->SetKeyAt(node_idx, leaf->KeyAt(0));
        } else {
          auto left_entries = leaf->GetEntries();
          auto right_entries = sibling->GetEntries();
          auto moved_entry = right_entries.front();
          right_entries.erase(right_entries.begin());
          if (moved_entry.is_tomb_) {
            moved_entry.recency_ = leaf->GetTombstones().size();
          }
          left_entries.push_back(moved_entry);
          leaf->SetEntries(left_entries);
          sibling->SetEntries(right_entries);
          parent->SetKeyAt(sibling_idx, sibling->KeyAt(0));
        }
        return;
      }
    } else {
      auto internal = curr_guard.template AsMut<InternalPage>();
      auto sibling = sibling_guard.template AsMut<InternalPage>();

      if (internal->GetSize() + sibling->GetSize() <= internal_max_size_) {
        // Coalesce
        if (node_idx > sibling_idx) {
          sibling->SetKeyAt(sibling->GetSize(), parent->KeyAt(node_idx));
          sibling->SetValueAt(sibling->GetSize(), internal->ValueAt(0));
          sibling->ChangeSizeBy(1);
          for (int i = 1; i < internal->GetSize(); ++i) {
            sibling->SetKeyAt(sibling->GetSize(), internal->KeyAt(i));
            sibling->SetValueAt(sibling->GetSize(), internal->ValueAt(i));
            sibling->ChangeSizeBy(1);
          }
          parent->RemoveAt(node_idx);
        } else {
          internal->SetKeyAt(internal->GetSize(), parent->KeyAt(sibling_idx));
          internal->SetValueAt(internal->GetSize(), sibling->ValueAt(0));
          internal->ChangeSizeBy(1);
          for (int i = 1; i < sibling->GetSize(); ++i) {
            internal->SetKeyAt(internal->GetSize(), sibling->KeyAt(i));
            internal->SetValueAt(internal->GetSize(), sibling->ValueAt(i));
            internal->ChangeSizeBy(1);
          }
          parent->RemoveAt(sibling_idx);
        }
      } else {
        // Redistribute
        if (node_idx > sibling_idx) {
          int size = internal->GetSize();
          for (int i = size; i > 1; --i) {
            internal->SetKeyAt(i, internal->KeyAt(i - 1));
            internal->SetValueAt(i, internal->ValueAt(i - 1));
          }
          internal->SetSize(size + 1);
          internal->SetValueAt(1, internal->ValueAt(0));
          internal->SetKeyAt(1, parent->KeyAt(node_idx));
          internal->SetValueAt(0, sibling->ValueAt(sibling->GetSize() - 1));

          parent->SetKeyAt(node_idx, sibling->KeyAt(sibling->GetSize() - 1));
          sibling->ChangeSizeBy(-1);
        } else {
          internal->SetKeyAt(internal->GetSize(), parent->KeyAt(sibling_idx));
          internal->SetValueAt(internal->GetSize(), sibling->ValueAt(0));
          internal->ChangeSizeBy(1);

          parent->SetKeyAt(sibling_idx, sibling->KeyAt(1));
          sibling->RemoveAt(0);
        }
        return;
      }
    }
  }

  auto root_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto root_page = root_guard.template AsMut<BPlusTreePage>();
  if (!root_page->IsLeafPage() && root_page->GetSize() == 1) {
    auto internal_root = reinterpret_cast<InternalPage *>(root_page);
    auto header = ctx->header_page_->template AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = internal_root->ValueAt(0);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Find the leftmost leaf page first, then construct index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.template As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  auto curr_guard = bpm_->ReadPage(ctx.root_page_id_);
  ctx.read_set_.push_back(std::move(curr_guard));

  while (true) {
    auto page = ctx.read_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal = ctx.read_set_.back().template As<InternalPage>();
    auto child_page_id = internal->ValueAt(0);  // 最左侧孩子始终在索引 0 处
    auto child_guard = bpm_->ReadPage(child_page_id);
    ctx.read_set_.pop_front();
    ctx.read_set_.push_back(std::move(child_guard));
  }

  auto leaf_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  return INDEXITERATOR_TYPE(bpm_, std::move(leaf_guard), 0);
}

/**
 * @brief Find the leaf page that contains the input key first, then construct index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.template As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  auto curr_guard = bpm_->ReadPage(ctx.root_page_id_);
  ctx.read_set_.push_back(std::move(curr_guard));

  while (true) {
    auto page = ctx.read_set_.back().template As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto internal = ctx.read_set_.back().template As<InternalPage>();
    auto child_page_id = internal->Lookup(key, comparator_);
    auto child_guard = bpm_->ReadPage(child_page_id);
    ctx.read_set_.pop_front();
    ctx.read_set_.push_back(std::move(child_guard));
  }

  auto leaf_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  auto leaf = leaf_guard.template As<LeafPage>();
  int idx = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(bpm_, std::move(leaf_guard), idx);
}

/**
 * @brief Construct an index iterator representing the end of the key/value pair in the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
