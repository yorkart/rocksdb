//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <algorithm>
#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

// This file contains utilities to handle the alignment of pages and buffers.

// Truncate to a multiple of page_size, which is also a page boundary. This
// helps to figuring out the right alignment.
// Example:
//   TruncateToPageBoundary(4096, 5000)  => 4096
//   TruncateToPageBoundary((4096, 10000) => 8192
/**
 * 用于计算page边界，s值应该是内存地址的offset，计算对齐后的offset是多少
 * @param page_size page大小
 * @param s 一般传过来是个offset值
 * @return 返回按page_size对齐后的offset值，该值应该 <= s
 */
inline size_t TruncateToPageBoundary(size_t page_size, size_t s) {
  // 同s -= s % page_size
  s -= (s & (page_size - 1));
  assert((s % page_size) == 0);
  return s;
}

// Round up x to a multiple of y.
// Example:
//   Roundup(13, 5)   => 15
//   Roundup(201, 16) => 208
/**
 * round意思做四舍五入
 * 简化问题模型： a * y = round(x)，a为整数，且round(x) >= x，求round(x)最小值
 * Example:
 *   Roundup(13, 5)   => a * 5  = round(13)  => 15
 *   Roundup(201, 16) => a * 16 = round(201) => 208
 *
 * @param x 参与round的值
 * @param y 基数
 * @return 返回round(x)最小值
 **/
inline size_t Roundup(size_t x, size_t y) {
  return ((x + y - 1) / y) * y;
}

// Round down x to a multiple of y.
// Example:
//   Rounddown(13, 5)   => 10
//   Rounddown(201, 16) => 192
/**
 * 类似Roundup，
 * 简化问题模型： a * y = round(x)，a为整数，且round(x) <= x，求round(x)最大值
 * @param x 参与round的值
 * @param y 基数
 * @return 返回round(x)最大值
 */
inline size_t Rounddown(size_t x, size_t y) { return (x / y) * y; }

// AlignedBuffer manages a buffer by taking alignment into consideration, and
// aligns the buffer start and end positions. It is mainly used for direct I/O,
// though it can be used other purposes as well.
// It also supports expanding the managed buffer, and copying whole or part of
// the data from old buffer into the new expanded buffer. Such a copy especially
// helps in cases avoiding an IO to re-fetch the data from disk.
//
// Example:
//   AlignedBuffer buf;
//   buf.Alignment(alignment);
//   buf.AllocateNewBuffer(user_requested_buf_size);
//   ...
//   buf.AllocateNewBuffer(2*user_requested_buf_size, /*copy_data*/ true,
//                         copy_offset, copy_len);
/**
 * AlignedBuffer通过考虑对齐来管理缓冲区，并对缓冲区的开始和结束位置进行对齐。
 * 它主要用于direct I/O，但也可以用于其他目的。
 * 它还支持扩展托管缓冲区，并将全部或部分数据从旧缓冲区复制到新的扩展缓冲区。
 * 这种复制特别有助于避免从磁盘重新获取数据的IO。
 */
class AlignedBuffer {
  /**
   * 对齐大小
   */
  size_t alignment_;
  /**
   * buf指针，
   */
  std::unique_ptr<char[]> buf_;
  /**
   * 容量
   */
  size_t capacity_;
  /**
   * 当前大小
   */
  size_t cursize_;
  /**
   * buf的起始地址
   */
  char* bufstart_;

public:
  AlignedBuffer()
    : alignment_(),
      capacity_(0),
      cursize_(0),
      bufstart_(nullptr) {
  }

  AlignedBuffer(AlignedBuffer&& o) ROCKSDB_NOEXCEPT {
    *this = std::move(o);
  }

  AlignedBuffer& operator=(AlignedBuffer&& o) ROCKSDB_NOEXCEPT {
    alignment_ = std::move(o.alignment_);
    buf_ = std::move(o.buf_);
    capacity_ = std::move(o.capacity_);
    cursize_ = std::move(o.cursize_);
    bufstart_ = std::move(o.bufstart_);
    return *this;
  }

  AlignedBuffer(const AlignedBuffer&) = delete;

  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  static bool isAligned(const void* ptr, size_t alignment) {
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
  }

  static bool isAligned(size_t n, size_t alignment) {
    return n % alignment == 0;
  }

  size_t Alignment() const {
    return alignment_;
  }

  size_t Capacity() const {
    return capacity_;
  }

  size_t CurrentSize() const {
    return cursize_;
  }

  const char* BufferStart() const {
    return bufstart_;
  }

  char* BufferStart() { return bufstart_; }

  void Clear() {
    cursize_ = 0;
  }

  char* Release() {
    cursize_ = 0;
    capacity_ = 0;
    bufstart_ = nullptr;
    return buf_.release();
  }

  /**
   * 设置对齐的值
   * @param alignment 必须是2的n次方
   */
  void Alignment(size_t alignment) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0); // 校验是否是2的n次方
    alignment_ = alignment;
  }

  // Allocates a new buffer and sets the start position to the first aligned
  // byte.
  //
  // requested_capacity: requested new buffer capacity. This capacity will be
  //     rounded up based on alignment.
  // copy_data: Copy data from old buffer to new buffer. If copy_offset and
  //     copy_len are not passed in and the new requested capacity is bigger
  //     than the existing buffer's capacity, the data in the exising buffer is
  //     fully copied over to the new buffer.
  // copy_offset: Copy data from this offset in old buffer.
  // copy_len: Number of bytes to copy.
  //
  // The function does nothing if the new requested_capacity is smaller than
  // the current buffer capacity and copy_data is true i.e. the old buffer is
  // retained as is.
  /**
   * 分配一个buffer。
   * 对容量大小，新buffer的起始位置做了对齐
   * @param requested_capacity 申请的容量
   * @param copy_data 是否要从老的buffer里拷贝数据填充到新的里面
   * @param copy_offset 拷贝老的buffer的offset
   * @param copy_len 拷贝老的buffer的字节长度
   */
  void AllocateNewBuffer(size_t requested_capacity, bool copy_data = false,
                         uint64_t copy_offset = 0, size_t copy_len = 0) {
    assert(alignment_ > 0);
    assert((alignment_ & (alignment_ - 1)) == 0);

    // copy_len不传，默认为cursize_
    copy_len = copy_len > 0 ? copy_len : cursize_;
    if (copy_data && requested_capacity < copy_len) {
      // If we are downsizing to a capacity that is smaller than the current
      // data in the buffer -- Ignore the request.
      // 这种情况下，只需要限制下len，就可以，不涉及数据移动，复用当前的buffer
      return;
    }

    // 容量大小对齐
    size_t new_capacity = Roundup(requested_capacity, alignment_);
    // 多分配一个alignment_大小，防止对new_buf起始位置对齐后，容量不够，
    char* new_buf = new char[new_capacity + alignment_];
    // new_buf起始位置对齐。
    // 和Roundup操作效果一样，按照alignment_大小进行窗口计算，得出起始窗口
    char* new_bufstart = reinterpret_cast<char*>(
        (reinterpret_cast<uintptr_t>(new_buf) + (alignment_ - 1)) &
        ~static_cast<uintptr_t>(alignment_ - 1));

    if (copy_data) {
      assert(bufstart_ + copy_offset + copy_len <= bufstart_ + cursize_);
      // 从老的buffer里拷贝数到新的buffer
      memcpy(new_bufstart, bufstart_ + copy_offset, copy_len);
      cursize_ = copy_len;
    } else {
      cursize_ = 0;
    }

    bufstart_ = new_bufstart;
    capacity_ = new_capacity;

    // 绑定新的buffer
    buf_.reset(new_buf);
  }

  // Append to the buffer.
  //
  // src         : source to copy the data from.
  // append_size : number of bytes to copy from src.
  // Returns the number of bytes appended.
  //
  // If append_size is more than the remaining buffer size only the
  // remaining-size worth of bytes are copied.
  /**
   * 追加数据到buffer。
   * 注意：如果追加的数据大小查出buffer可用大小，则按buffer大小追加
   * @param src 数据指针
   * @param append_size 追加大小
   * @return 实际追加的大小
   */
  size_t Append(const char* src, size_t append_size) {
    size_t buffer_remaining = capacity_ - cursize_;
    // 注意这里如果剩余buffer不够，不会扩容
    size_t to_copy = std::min(append_size, buffer_remaining);

    if (to_copy > 0) {
      memcpy(bufstart_ + cursize_, src, to_copy);
      cursize_ += to_copy;
    }
    return to_copy;
  }

  // Read from the buffer.
  //
  // dest      : destination buffer to copy the data to.
  // offset    : the buffer offset to start reading from.
  // read_size : the number of bytes to copy from the buffer to dest.
  // Returns the number of bytes read/copied to dest.
  /**
   * 拷贝数据，填充到dest中
   * @param dest 填充目标指针
   * @param offset 从buffer读取的位置
   * @param read_size 从buffer读取的大小，最多只能把数据读完
   * @return 实际读取大小
   */
  size_t Read(char* dest, size_t offset, size_t read_size) const {
    assert(offset < cursize_);

    size_t to_read = 0;
    if(offset < cursize_) {
      // 最多读完所有数据
      to_read = std::min(cursize_ - offset, read_size);
    }
    if (to_read > 0) {
      memcpy(dest, bufstart_ + offset, to_read);
    }
    return to_read;
  }

  // Pad to the end of alignment with "padding"
  /**
   * pad对齐，就是对当前位置对齐，被对齐的部分填充padding值
   * ┌───────────────► cursize_
   * ├─────────────┼─────────────┬────────────┐
   * │ alignment_  │ alignment_  │alignment_  │
   * └─────────────┴─────────────┴────────────┘
   *
   * |                ◄─pad_size─►
   *
   * ┌───────────────────────────► total_size
   * ├─────────────┼─────────────┬────────────┐
   * │ alignment_  │alignment_   │alignment_  │
   * └─────────────┴─────────────┴────────────┘
   * @param padding
   */
  void PadToAlignmentWith(int padding) {
    size_t total_size = Roundup(cursize_, alignment_);
    size_t pad_size = total_size - cursize_;

    if (pad_size > 0) {
      assert((pad_size + cursize_) <= capacity_);
      memset(bufstart_ + cursize_, padding, pad_size);
      cursize_ += pad_size;
    }
  }

  /**
   * 以padding值填充一片内存
   * @param pad_size 填充长度
   * @param padding 填充内容
   */
  void PadWith(size_t pad_size, int padding) {
    assert((pad_size + cursize_) <= capacity_);
    memset(bufstart_ + cursize_, padding, pad_size);
    cursize_ += pad_size;
  }

  // After a partial flush move the tail to the beginning of the buffer.
  /**
   * 把指定位置内存空间，拷贝的起始位置
   * @param tail_offset 距离bufstart_的offset
   * @param tail_size 拷贝大小
   */
  void RefitTail(size_t tail_offset, size_t tail_size) {
    if (tail_size > 0) {
      memmove(bufstart_, bufstart_ + tail_offset, tail_size);
    }
    cursize_ = tail_size;
  }

  // Returns a place to start appending.
  // WARNING: Note that it is possible to write past the end of the buffer if
  // the buffer is modified without using the write APIs or encapsulation
  // offered by AlignedBuffer. It is up to the user to guard against such
  // errors.
  /**
   * 当前可以追加数据的起始指针地址
   * @return
   */
  char* Destination() {
    return bufstart_ + cursize_;
  }

  void Size(size_t cursize) {
    cursize_ = cursize;
  }
};
}  // namespace ROCKSDB_NAMESPACE
