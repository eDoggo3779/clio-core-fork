/*
 * Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology
 * All rights reserved.
 *
 * This file is part of IOWarp Core.
 *
 * ...
 */

#ifndef CLIO_BDEV_BLOCK_ALLOCATOR_H_
#define CLIO_BDEV_BLOCK_ALLOCATOR_H_

#include <clio_runtime/clio_runtime.h>
#include <clio_runtime/bdev/bdev_tasks.h>
#include <clio_runtime/comutex.h>
#include <atomic>
#include <vector>
#include <list>
#include <mutex>
#include <string>

namespace clio::run::bdev {

// Free-list bucket sizes. MUST match kBlockSizes[] in block_allocator.cc.
// Sub-4KB buckets (issue #862): a ~1KB blob no longer pins a whole 4KB block
// on byte-addressable tiers — the RAM bdev allocates at 512B granularity (see
// mem_bdev_transport.cc); device-aligned tiers (file O_DIRECT) keep a 4KB
// alignment quantum, so their requests still round up past the small buckets.
enum class BlockSizeCategory : clio::run::u32 {
  k512B = 0,
  k1KB = 1,
  k2KB = 2,
  k4KB = 3,
  k16KB = 4,
  k32KB = 5,
  k64KB = 6,
  k128KB = 7,
  k1MB = 8,
  kMaxCategories = 9
};

extern const size_t kBlockSizes[];

/**
 * Worker-local block map to reduce lock contention
 */
class WorkerBlockMap {
 public:
  WorkerBlockMap();

  bool AllocateBlock(int block_type, Block& block, size_t min_size = 0);
  /** Pull ANY freed block whose size_ is <= max_bytes (largest first), for
   *  fragmentation reuse (issue #820). Unlike AllocateBlock, which finds a
   *  block >= a size in one category, this scans every bucket for a block that
   *  FITS what is left -- a freed 8 KiB block bucketed under 16 KiB is exactly
   *  the case the >= match could never reach. */
  bool AllocateAnyUpTo(size_t max_bytes, Block& block);
  void FreeBlock(Block block);

 private:
  std::vector<std::list<Block>> blocks_;
};

/**
 * Global block map with per-worker caching and locking
 */
class GlobalBlockMap {
 public:
  GlobalBlockMap();

  void Init(size_t num_workers);
  bool AllocateBlock(int worker, size_t io_size, Block& block);
  /** @copydoc WorkerBlockMap::AllocateAnyUpTo -- searches this worker's map
   *  first, then steals from the others. */
  bool AllocateAnyUpTo(int worker, size_t max_bytes, Block& block);
  bool FreeBlock(int worker, Block& block);

  /** Map an I/O size to its block-size category, or -1 if larger than all. */
  static int FindBlockType(size_t io_size);

 private:
  std::vector<WorkerBlockMap> worker_maps_;
  std::vector<clio::run::CoMutex> worker_locks_;
};

/**
 * Heap allocator for new blocks
 */
class Heap {
 public:
  Heap();

  void Init(clio::run::u64 total_size, clio::run::u32 alignment = 4096);
  bool Allocate(size_t block_size, int block_type, Block& block);
  clio::run::u64 GetRemainingSize() const;

  /**
   * Raise the bump pointer to at least `end_offset` (issue: restart
   * overwrites live data). Init() resets the pointer to 0, which is correct
   * for a fresh device and CATASTROPHIC for one whose backing file still
   * holds blocks that restored metadata points at -- the next allocation
   * hands out offset 0 and the first write lands on top of live data.
   */
  void ReserveUpTo(clio::run::u64 end_offset);

  /** Current bump pointer (bytes handed out from the heap so far). */
  clio::run::u64 Current() const { return heap_.load(); }

 private:
  std::atomic<clio::run::u64> heap_;
  clio::run::u64 total_size_;
  clio::run::u32 alignment_;
};

/**
 * Standard Allocator containing GlobalBlockMap and Heap
 */
class StandardBlockAllocator {
 public:
  StandardBlockAllocator() : alignment_(4096), capacity_(0) {}

  void Init(size_t num_workers, clio::run::u64 capacity, clio::run::u32 alignment) {
    capacity_ = capacity;
    alignment_ = alignment;
    global_block_map_.Init(num_workers);
    heap_.Init(capacity, alignment);
  }

  bool AllocateBlocks(size_t size, int worker_id, std::vector<Block>& blocks);
  void FreeBlocks(int worker_id, const std::vector<Block>& blocks);

  clio::run::u64 GetRemainingSize() const;
  clio::run::u64 GetCapacity() const { return capacity_; }

  /**
   * Make the heap watermark durable in `path` (issue: restart overwrites
   * live data). Call once, right after Init(), for PERSISTENT devices only:
   * a memory tier's contents do not survive the process, so its heap must
   * restart at 0.
   *
   * On call, any watermark previously stored in `path` is read back and the
   * heap is advanced past it, so a restarted device never re-hands-out an
   * extent whose bytes are still referenced by restored metadata. The file
   * holds one decimal u64 and is rewritten in CHUNKS (see kWatermarkChunk):
   * a crash can therefore only ever leave the stored value AHEAD of what was
   * really handed out, which wastes a little space and is safe, never behind
   * it, which would corrupt.
   */
  void InitPersistence(const std::string& path);

 private:
  GlobalBlockMap global_block_map_;
  Heap heap_;
  clio::run::u32 alignment_;
  clio::run::u64 capacity_;
  std::atomic<clio::run::u64> allocated_bytes_{0};

  /** Durable-watermark state; inert unless InitPersistence() was called. */
  std::string watermark_path_;
  std::atomic<clio::run::u64> watermark_persisted_{0};
  std::mutex watermark_mutex_;
  /** Persist in 8 MiB steps: one small fsync per 8 MiB of fresh heap. */
  static constexpr clio::run::u64 kWatermarkChunk = 8ULL << 20;

  /** Ensure the stored watermark covers `needed` before it is handed out. */
  void PersistWatermark(clio::run::u64 needed);

  clio::run::u64 AlignSize(clio::run::u64 size) {
    if (alignment_ == 0) alignment_ = 4096;
    return ((size + alignment_ - 1) / alignment_) * alignment_;
  }
};

} // namespace clio::run::bdev

#endif // CLIO_BDEV_BLOCK_ALLOCATOR_H_
