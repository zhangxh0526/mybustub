//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombs;
  tombs.reserve(num_tombstones_);
  for (size_t i = 0; i < num_tombstones_; ++i) {
    // tombstones_ 数组中存放的是物理数组 index，我们根据 index 找到真实的 key
    tombs.push_back(key_array_[tombstones_[i]]);
  }
  return tombs;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index >= 0 && index < GetSize());
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstoned(int index) const -> bool {
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] == static_cast<size_t>(index)) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int size = GetSize();
  int low = 0;
  int high = size - 1;
  while (low <= high) {
    int mid = low + (high - low) / 2;
    int cmp = comparator(key, key_array_[mid]);
    if (cmp == 0) {
      return mid;
    }
    if (cmp < 0) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return low;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int idx, const KeyType &key, const ValueType &value) {
  int size = GetSize();
  assert(size < GetMaxSize());
  for (int i = size; i > idx; --i) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  key_array_[idx] = key;
  rid_array_[idx] = value;
  SetSize(size + 1);

  // 关键：更新墓碑索引漂移
  for (size_t t = 0; t < num_tombstones_; ++t) {
    if (tombstones_[t] >= static_cast<size_t>(idx)) {
      tombstones_[t]++;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ApplyOldestTombstone() {
  if (num_tombstones_ == 0) {
    return;
  }
  size_t delete_idx = tombstones_[0];
  int size = GetSize();
  for (int i = delete_idx + 1; i < size; ++i) {
    key_array_[i - 1] = key_array_[i];
    rid_array_[i - 1] = rid_array_[i];
  }
  SetSize(size - 1);

  // 调整剩余墓碑的索引
  for (size_t t = 1; t < num_tombstones_; ++t) {
    size_t old_val = tombstones_[t];
    if (old_val > delete_idx) {
      tombstones_[t - 1] = old_val - 1;
    } else {
      tombstones_[t - 1] = old_val;
    }
  }
  num_tombstones_--;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertTombstone(int idx) {
  if (LEAF_PAGE_TOMB_CNT == 0) {
    // 物理直接删除
    int size = GetSize();
    for (int i = idx + 1; i < size; ++i) {
      key_array_[i - 1] = key_array_[i];
      rid_array_[i - 1] = rid_array_[i];
    }
    SetSize(size - 1);
    return;
  }

  if (num_tombstones_ == LEAF_PAGE_TOMB_CNT) {
    size_t delete_idx = tombstones_[0];
    ApplyOldestTombstone();
    if (static_cast<size_t>(idx) > delete_idx) {
      idx--;
    }
  }
  tombstones_[num_tombstones_++] = idx;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ReviveAt(int idx, const ValueType &value) {
  rid_array_[idx] = value;
  // 从墓碑缓冲区移除此物理索引
  size_t t_idx = num_tombstones_;
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] == static_cast<size_t>(idx)) {
      t_idx = i;
      break;
    }
  }
  if (t_idx < num_tombstones_) {
    for (size_t i = t_idx + 1; i < num_tombstones_; ++i) {
      tombstones_[i - 1] = tombstones_[i];
    }
    num_tombstones_--;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetEntries() const -> std::vector<LeafEntry> {
  std::vector<LeafEntry> entries;
  entries.reserve(GetSize());
  for (int i = 0; i < GetSize(); ++i) {
    LeafEntry entry;
    entry.key_ = key_array_[i];
    entry.val_ = rid_array_[i];
    entry.is_tomb_ = false;
    entry.recency_ = 0;
    for (size_t t = 0; t < num_tombstones_; ++t) {
      if (tombstones_[t] == static_cast<size_t>(i)) {
        entry.is_tomb_ = true;
        entry.recency_ = t;
        break;
      }
    }
    entries.push_back(entry);
  }
  return entries;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetEntries(const std::vector<LeafEntry> &entries) {
  auto live_entries = entries;
  while (true) {
    auto oldest_tomb = live_entries.end();
    for (auto iter = live_entries.begin(); iter != live_entries.end(); ++iter) {
      if (iter->is_tomb_ && (oldest_tomb == live_entries.end() || iter->recency_ < oldest_tomb->recency_)) {
        oldest_tomb = iter;
      }
    }

    size_t tomb_count = 0;
    for (const auto &entry : live_entries) {
      if (entry.is_tomb_) {
        tomb_count++;
      }
    }
    if (tomb_count <= LEAF_PAGE_TOMB_CNT) {
      break;
    }
    live_entries.erase(oldest_tomb);
  }

  SetSize(live_entries.size());
  struct TombTemp {
    size_t original_recency_;
    size_t new_physical_idx_;
  };
  std::vector<TombTemp> tombstones_temp;
  for (size_t i = 0; i < live_entries.size(); ++i) {
    key_array_[i] = live_entries[i].key_;
    rid_array_[i] = live_entries[i].val_;
    if (live_entries[i].is_tomb_) {
      tombstones_temp.push_back({live_entries[i].recency_, i});
    }
  }
  // 按原有的 recency（从最老到最新）进行排序
  std::sort(tombstones_temp.begin(), tombstones_temp.end(),
            [](const TombTemp &a, const TombTemp &b) { return a.original_recency_ < b.original_recency_; });

  num_tombstones_ = tombstones_temp.size();
  for (size_t t = 0; t < num_tombstones_; ++t) {
    tombstones_[t] = tombstones_temp[t].new_physical_idx_;
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
