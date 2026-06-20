//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots.
  page_table_.reserve(num_frames_);

  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  
  frame_id_t frame_id = -1;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // 适配新的 ArcReplacer::Evict() 接口
    auto evict_opt = replacer_->Evict();
    if (!evict_opt.has_value()) {
      return INVALID_PAGE_ID;
    }
    frame_id = evict_opt.value();
    
    auto evicted_frame = frames_[frame_id];
    page_id_t evicted_page_id = INVALID_PAGE_ID;
    for (const auto &[pid, fid] : page_table_) {
      if (fid == frame_id) {
        evicted_page_id = pid;
        break;
      }
    }
    if (evicted_page_id != INVALID_PAGE_ID) {
      if (evicted_frame->is_dirty_) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        DiskRequest req{true, evicted_frame->GetDataMut(), evicted_page_id, std::move(promise)};
        std::vector<DiskRequest> reqs;
        reqs.push_back(std::move(req));
        disk_scheduler_->Schedule(reqs);
        future.get();
        evicted_frame->is_dirty_ = false;
      }
      page_table_.erase(evicted_page_id);
    }
  }
  
  page_id_t new_page_id = next_page_id_.fetch_add(1);
  
  auto frame = frames_[frame_id];
  frame->Reset();
  
  page_table_[new_page_id] = frame_id;
  // 重要修改：通过 NewPage 分配的页在没有 Guard 保护前，pin_count 应该初始化为 0，并且设为可驱逐 [1]
  frame->pin_count_ = 0;
  frame->is_dirty_ = false;
  
  replacer_->RecordAccess(frame_id, new_page_id);
  replacer_->SetEvictable(frame_id, true);
  
  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }

  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true; // 缓存未命中，视为删除成功
  }
  
  frame_id_t frame_id = iter->second;
  auto frame = frames_[frame_id];
  
  if (frame->pin_count_ > 0) {
    return false; // 当前页正被引用，无法删除
  }
  
  if (frame->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest req{true, frame->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> reqs;
    reqs.push_back(std::move(req));
    disk_scheduler_->Schedule(reqs);
    future.get();
    frame->is_dirty_ = false;
  }
  
  page_table_.erase(iter);
  replacer_->Remove(frame_id);
  frame->Reset();
  free_frames_.push_back(frame_id);
  
  disk_scheduler_->DeallocatePage(page_id);
  
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::shared_ptr<FrameHeader> frame = nullptr;
  {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    auto iter = page_table_.find(page_id);
    if (iter != page_table_.end()) {
      frame_id_t frame_id = iter->second;
      frame = frames_[frame_id];
      frame->pin_count_++;
      replacer_->RecordAccess(frame_id, page_id);
      replacer_->SetEvictable(frame_id, false);
    } else {
      frame_id_t frame_id = -1;
      if (!free_frames_.empty()) {
        frame_id = free_frames_.front();
        free_frames_.pop_front();
      } else {
        // 适配新的 ArcReplacer::Evict() 接口
        auto evict_opt = replacer_->Evict();
        if (!evict_opt.has_value()) {
          return std::nullopt;
        }
        frame_id = evict_opt.value();
        
        auto evicted_frame = frames_[frame_id];
        page_id_t evicted_page_id = INVALID_PAGE_ID;
        for (const auto &[pid, fid] : page_table_) {
          if (fid == frame_id) {
            evicted_page_id = pid;
            break;
          }
        }
        if (evicted_page_id != INVALID_PAGE_ID) {
          if (evicted_frame->is_dirty_) {
            auto promise = disk_scheduler_->CreatePromise();
            auto future = promise.get_future();
            DiskRequest req{true, evicted_frame->GetDataMut(), evicted_page_id, std::move(promise)};
            std::vector<DiskRequest> reqs;
            reqs.push_back(std::move(req));
            disk_scheduler_->Schedule(reqs);
            future.get();
            evicted_frame->is_dirty_ = false;
          }
          page_table_.erase(evicted_page_id);
        }
      }
      
      frame = frames_[frame_id];
      frame->Reset();
      
      // 发送读请求，将磁盘数据加载进物理帧中
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest req{false, frame->GetDataMut(), page_id, std::move(promise)};
      std::vector<DiskRequest> reqs;
      reqs.push_back(std::move(req));
      disk_scheduler_->Schedule(reqs);
      future.get();
      
      page_table_[page_id] = frame_id;
      frame->pin_count_ = 1;
      frame->is_dirty_ = false;
      replacer_->RecordAccess(frame_id, page_id);
      replacer_->SetEvictable(frame_id, false);
    }
  }
  
  return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::shared_ptr<FrameHeader> frame = nullptr;
  {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    auto iter = page_table_.find(page_id);
    if (iter != page_table_.end()) {
      frame_id_t frame_id = iter->second;
      frame = frames_[frame_id];
      frame->pin_count_++;
      replacer_->RecordAccess(frame_id, page_id);
      replacer_->SetEvictable(frame_id, false);
    } else {
      frame_id_t frame_id = -1;
      if (!free_frames_.empty()) {
        frame_id = free_frames_.front();
        free_frames_.pop_front();
      } else {
        // 适配新的 ArcReplacer::Evict() 接口
        auto evict_opt = replacer_->Evict();
        if (!evict_opt.has_value()) {
          return std::nullopt;
        }
        frame_id = evict_opt.value();
        
        auto evicted_frame = frames_[frame_id];
        page_id_t evicted_page_id = INVALID_PAGE_ID;
        for (const auto &[pid, fid] : page_table_) {
          if (fid == frame_id) {
            evicted_page_id = pid;
            break;
          }
        }
        if (evicted_page_id != INVALID_PAGE_ID) {
          if (evicted_frame->is_dirty_) {
            auto promise = disk_scheduler_->CreatePromise();
            auto future = promise.get_future();
            DiskRequest req{true, evicted_frame->GetDataMut(), evicted_page_id, std::move(promise)};
            std::vector<DiskRequest> reqs;
            reqs.push_back(std::move(req));
            disk_scheduler_->Schedule(reqs);
            future.get();
            evicted_frame->is_dirty_ = false;
          }
          page_table_.erase(evicted_page_id);
        }
      }
      
      frame = frames_[frame_id];
      frame->Reset();
      
      // 加载页面数据到内存
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest req{false, frame->GetDataMut(), page_id, std::move(promise)};
      std::vector<DiskRequest> reqs;
      reqs.push_back(std::move(req));
      disk_scheduler_->Schedule(reqs);
      future.get();
      
      page_table_[page_id] = frame_id;
      frame->pin_count_ = 1;
      frame->is_dirty_ = false;
      replacer_->RecordAccess(frame_id, page_id);
      replacer_->SetEvictable(frame_id, false);
    }
  }
  
  return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  
  frame_id_t frame_id = iter->second;
  auto frame = frames_[frame_id];
  
  if (frame->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest req{true, frame->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> reqs;
    reqs.push_back(std::move(req));
    disk_scheduler_->Schedule(reqs);
    future.get();
    
    frame->is_dirty_ = false;
  }
  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::shared_ptr<FrameHeader> frame = nullptr;
  {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
      return false;
    }
    frame = frames_[iter->second];
  }
  
  // 1. 先安全加页独占锁
  std::unique_lock<std::shared_mutex> page_lock(frame->rwlatch_);
  
  // 2. 然后加全局锁做元数据二次检验与物理刷盘
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || frames_[iter->second] != frame) {
    return false;
  }
  
  if (frame->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest req{true, frame->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> reqs;
    reqs.push_back(std::move(req));
    disk_scheduler_->Schedule(reqs);
    future.get();
    
    frame->is_dirty_ = false;
  }
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  for (const auto &[page_id, frame_id] : page_table_) {
    auto frame = frames_[frame_id];
    if (frame->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest req{true, frame->GetDataMut(), page_id, std::move(promise)};
      std::vector<DiskRequest> reqs;
      reqs.push_back(std::move(req));
      disk_scheduler_->Schedule(reqs);
      future.get();
      
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 */
void BufferPoolManager::FlushAllPages() {
  std::vector<std::pair<page_id_t, std::shared_ptr<FrameHeader>>> active_pages;
  {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    for (const auto &[page_id, frame_id] : page_table_) {
      active_pages.emplace_back(page_id, frames_[frame_id]);
    }
  }
  
  // 按照无死锁的方式对每页进行序列化安全刷盘
  for (const auto &[page_id, frame] : active_pages) {
    std::unique_lock<std::shared_mutex> page_lock(frame->rwlatch_);
    std::scoped_lock<std::mutex> lock(*bpm_latch_);
    
    auto iter = page_table_.find(page_id);
    if (iter != page_table_.end() && frames_[iter->second] == frame) {
      if (frame->is_dirty_) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        DiskRequest req{true, frame->GetDataMut(), page_id, std::move(promise)};
        std::vector<DiskRequest> reqs;
        reqs.push_back(std::move(req));
        disk_scheduler_->Schedule(reqs);
        future.get();
        
        frame->is_dirty_ = false;
      }
    }
  }
}

/**
 * @brief Retrieves the pin count of a page.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return std::nullopt;
  }
  return frames_[iter->second]->pin_count_.load();
}

}  // namespace bustub