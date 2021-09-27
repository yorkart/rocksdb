//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

#include "port/likely.h"
#include "port/port.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// An array of core-local values. Ideally the value type, T, is cache aligned to
// prevent false sharing.
/**
 * 根据cpu核数构造数组，进行缓存数据。
 * 类似于threadlocal，只不过是跟cpu core绑定的
 * @tparam T
 */
template <typename T>
class CoreLocalArray {
 public:
  CoreLocalArray();

  size_t Size() const;
  // returns pointer to the element corresponding to the core that the thread
  // currently runs on.
  T* Access() const;
  // same as above, but also returns the core index, which the client can cache
  // to reduce how often core ID needs to be retrieved. Only do this if some
  // inaccuracy is tolerable, as the thread may migrate to a different core.
  std::pair<T*, size_t> AccessElementAndIndex() const;
  // returns pointer to element for the specified core index. This can be used,
  // e.g., for aggregation, or if the client caches core index.
  T* AccessAtCore(size_t core_idx) const;

 private:
  std::unique_ptr<T[]> data_;
  int size_shift_;
};

/**
 * 根据cpu个数，按照2的次方对齐，计算该值，且该值最少为8。
 * 根据计算值，构造data_数据
 * eg: cpu: 1 => 8, cpu: 9 => 16
 * @tparam T
 */
template <typename T>
CoreLocalArray<T>::CoreLocalArray() {
  int num_cpus = static_cast<int>(std::thread::hardware_concurrency());
  // find a power of two >= num_cpus and >= 8
  size_shift_ = 3;
  while (1 << size_shift_ < num_cpus) {
    ++size_shift_;
  }
  data_.reset(new T[static_cast<size_t>(1) << size_shift_]);
}

template <typename T>
size_t CoreLocalArray<T>::Size() const {
  return static_cast<size_t>(1) << size_shift_;
}

template <typename T>
T* CoreLocalArray<T>::Access() const {
  return AccessElementAndIndex().first;
}

/**
 * 根据所在cpuid，获取对应索引位置的数据
 * @tparam T
 * @return
 */
template <typename T>
std::pair<T*, size_t> CoreLocalArray<T>::AccessElementAndIndex() const {
  int cpuid = port::PhysicalCoreID();
  size_t core_idx;
  if (UNLIKELY(cpuid < 0)) {
    // cpu id unavailable, just pick randomly
    core_idx = Random::GetTLSInstance()->Uniform(1 << size_shift_);
  } else {
    // 取余cpuid % 1<< size_shift_
    core_idx = static_cast<size_t>(cpuid & ((1 << size_shift_) - 1));
  }
  return {AccessAtCore(core_idx), core_idx};
}

/**
 * 根据下标，定位数据
 * @tparam T
 * @param core_idx
 * @return
 */
template <typename T>
T* CoreLocalArray<T>::AccessAtCore(size_t core_idx) const {
  assert(core_idx < static_cast<size_t>(1) << size_shift_);
  return &data_[core_idx];
}

}  // namespace ROCKSDB_NAMESPACE
