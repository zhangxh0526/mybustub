//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <optional>
#include <stdexcept>
#include "common/config.h"

namespace bustub {

/**
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  bool prefer_mru = !mru_.empty() && (mru_.size() >= mru_target_size_);
  frame_id_t victim_fid = -1;
  bool evicted_from_mru = false;

  if (prefer_mru) {
    for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
      if (alive_map_[*it]->evictable_) {
        victim_fid = *it;
        evicted_from_mru = true;
        break;
      }
    }
    if (victim_fid == -1) {
      for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
        if (alive_map_[*it]->evictable_) {
          victim_fid = *it;
          evicted_from_mru = false;
          break;
        }
      }
    }
  } else {
    for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
      if (alive_map_[*it]->evictable_) {
        victim_fid = *it;
        evicted_from_mru = false;
        break;
      }
    }
    if (victim_fid == -1) {
      for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
        if (alive_map_[*it]->evictable_) {
          victim_fid = *it;
          evicted_from_mru = true;
          break;
        }
      }
    }
  }

  if (victim_fid != -1) {
    auto status = alive_map_[victim_fid];
    page_id_t pid = status->page_id_;

    if (evicted_from_mru) {
      mru_.erase(status->alive_it_);  // O(1) 擦除
      mru_ghost_.push_front(pid);
      status->ghost_it_ = mru_ghost_.begin();  // 记录 Ghost 迭代器
      status->arc_status_ = ArcStatus::MRU_GHOST;
    } else {
      mfu_.erase(status->alive_it_);  // O(1) 擦除
      mfu_ghost_.push_front(pid);
      status->ghost_it_ = mfu_ghost_.begin();  // 记录 Ghost 迭代器
      status->arc_status_ = ArcStatus::MFU_GHOST;
    }

    alive_map_.erase(victim_fid);
    ghost_map_[pid] = status;
    curr_size_--;

    return victim_fid;
  }

  return std::nullopt;
}

/**
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id < 0) {
    throw std::invalid_argument("Invalid frame_id in RecordAccess");
  }

  // Case 1: 内存命中 (Hits in MRU or MFU)
  auto alive_iter = alive_map_.find(frame_id);
  if (alive_iter != alive_map_.end()) {
    auto status = alive_iter->second;
    status->page_id_ = page_id;  // 更新映射

    if (status->arc_status_ == ArcStatus::MRU) {
      mru_.erase(status->alive_it_);  // O(1) 擦除
      mfu_.push_front(frame_id);
      status->alive_it_ = mfu_.begin();  // 记录新迭代器
      status->arc_status_ = ArcStatus::MFU;
    } else if (status->arc_status_ == ArcStatus::MFU) {
      mfu_.erase(status->alive_it_);  // O(1) 擦除
      mfu_.push_front(frame_id);
      status->alive_it_ = mfu_.begin();  // 记录新迭代器
    }
    return;
  }

  // Case 2 & 3: 幽灵命中 (Hits in Ghost Lists)
  auto ghost_iter = ghost_map_.find(page_id);
  if (ghost_iter != ghost_map_.end()) {
    auto status = ghost_iter->second;

    if (status->arc_status_ == ArcStatus::MRU_GHOST) {
      // Case 2: Hit in mru_ghost_ (B1)
      size_t d = 1;
      if (mru_ghost_.size() < mfu_ghost_.size()) {
        d = mfu_ghost_.size() / mru_ghost_.size();
      }
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + d);

      mru_ghost_.erase(status->ghost_it_);  // O(1) 擦除
    } else if (status->arc_status_ == ArcStatus::MFU_GHOST) {
      // Case 3: Hit in mfu_ghost_ (B2)
      size_t d = 1;
      if (mfu_ghost_.size() < mru_ghost_.size()) {
        d = mru_ghost_.size() / mfu_ghost_.size();
      }
      mru_target_size_ = (mru_target_size_ > d) ? (mru_target_size_ - d) : 0;

      mfu_ghost_.erase(status->ghost_it_);  // O(1) 擦除
    }

    // 从幽灵列表中复活，移入 MFU
    ghost_map_.erase(ghost_iter);
    mfu_.push_front(frame_id);

    status->frame_id_ = frame_id;
    status->arc_status_ = ArcStatus::MFU;
    status->evictable_ = false;        // 新移入页面默认锁定
    status->alive_it_ = mfu_.begin();  // 记录新迭代器
    alive_map_[frame_id] = status;
    return;
  }

  // Case 4: 全新未命中 (Misses all lists)
  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    if (!mru_ghost_.empty()) {
      page_id_t victim_page = mru_ghost_.back();
      mru_ghost_.pop_back();
      ghost_map_.erase(victim_page);
    }
  } else if (mru_.size() + mru_ghost_.size() < replacer_size_) {
    size_t total_size = mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size();
    if (total_size == 2 * replacer_size_) {
      if (!mfu_ghost_.empty()) {
        page_id_t victim_page = mfu_ghost_.back();
        mfu_ghost_.pop_back();
        ghost_map_.erase(victim_page);
      }
    }
  }

  mru_.push_front(frame_id);
  auto new_status = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  new_status->alive_it_ = mru_.begin();  // 记录迭代器
  alive_map_[frame_id] = new_status;
}

/**
 * @brief Toggle whether a frame is evictable or non-evictable.
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id < 0) {
    throw std::invalid_argument("Invalid frame_id in SetEvictable");
  }

  auto iter = alive_map_.find(frame_id);
  if (iter == alive_map_.end()) {
    return;
  }

  if (iter->second->evictable_ != set_evictable) {
    iter->second->evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

/**
 * @brief Remove an evictable frame from replacer.
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id < 0) {
    throw std::invalid_argument("Invalid frame_id in Remove");
  }

  auto iter = alive_map_.find(frame_id);
  if (iter == alive_map_.end()) {
    return;
  }

  if (!iter->second->evictable_) {
    throw std::runtime_error("Attempted to remove a non-evictable frame");
  }

  curr_size_--;
  if (iter->second->arc_status_ == ArcStatus::MRU) {
    mru_.erase(iter->second->alive_it_);  // O(1) 擦除
  } else if (iter->second->arc_status_ == ArcStatus::MFU) {
    mfu_.erase(iter->second->alive_it_);  // O(1) 擦除
  }
  alive_map_.erase(iter);
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 */
auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub