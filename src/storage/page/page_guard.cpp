//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)),
      is_valid_(frame_ != nullptr) {
  if (is_valid_) {
    frame_->rwlatch_.lock_shared();
  }
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop(); // 释放自身原先占有的资源
    
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;
    
    that.is_valid_ = false;
    that.page_id_ = INVALID_PAGE_ID;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 */
void ReadPageGuard::Flush() {
  if (!is_valid_) {
    return;
  }
  if (frame_->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    
    DiskRequest req{true, frame_->GetDataMut(), page_id_, std::move(promise)};
    std::vector<DiskRequest> reqs;
    reqs.push_back(std::move(req));
    disk_scheduler_->Schedule(reqs);
    future.get();
    
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data.
 */
void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  
  // 1. 先安全地释放页锁，打破交叉死锁环
  frame_->rwlatch_.unlock_shared();
  
  // 2. 然后获取全局缓冲池锁，修改页元数据和置换器
  {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    frame_->pin_count_--;
    if (frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  
  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)),
      is_valid_(frame_ != nullptr) {
  if (is_valid_) {
    frame_->rwlatch_.lock();
  }
}

/**
 * @brief The move constructor for `WritePageGuard`.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;
    
    that.is_valid_ = false;
    that.page_id_ = INVALID_PAGE_ID;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  // 因为返回了可修改的数据指针，所以标记页面为 dirty（脏页） [1]
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 */
void WritePageGuard::Flush() {
  if (!is_valid_) {
    return;
  }
  if (frame_->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    
    DiskRequest req{true, frame_->GetDataMut(), page_id_, std::move(promise)};
    std::vector<DiskRequest> reqs;
    reqs.push_back(std::move(req));
    disk_scheduler_->Schedule(reqs);
    future.get();
    
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data.
 */
void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  
  // 1. 先安全地释放独占页锁
  frame_->rwlatch_.unlock();
  
  // 2. 然后获取全局缓冲池锁，修改页元数据和置换器
  {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    frame_->pin_count_--;
    if (frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  
  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub