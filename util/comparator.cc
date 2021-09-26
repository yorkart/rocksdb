//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/comparator.h"

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <mutex>

#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }
  static const char* kClassName() { return "leveldb.BytewiseComparator"; }
  const char* Name() const override { return kClassName(); }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  bool Equal(const Slice& a, const Slice& b) const override { return a == b; }

  /**
   * 提取start和limit的最小前缀，结果放到start中。
   * 先尝试返回公共前缀的后继，再尝试返回start自身。
   * 前提：start和limit大小关系不一定
   * 求值：Min(result)
   * 满足：result >= start && result < limit
   * eg：[]表示变化的值
   *      start          limit     => result
   *   1. a b c          a b c     => a b c
   *   2. a b c          a b c d   => a b c
   *   3. a b c          a b c d   => a b c
   *   4. a b c a        a b d e   => a b c [b]
   *   5. a b c \0xff a  a b d     => a b c 0xff [b]
   * @param start
   * @param limit
   */
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    // 两个字符串的最小长度
    size_t min_length = std::min(start->size(), limit.size());

    // 从0开始比较两个字符串，找到存在不同字符的索引下标
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
      // 说明limit完全前缀包含start。start就不用赋值之类调整，本身就是公共前缀
    } else {
      uint8_t start_byte = static_cast<uint8_t>((*start)[diff_index]);
      uint8_t limit_byte = static_cast<uint8_t>(limit[diff_index]);
      if (start_byte >= limit_byte) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        // 从差异位置的第一个字符判断，如果start>limit，不符合该方法语义，放弃，直接返回
        return;
      }
      assert(start_byte < limit_byte);

      if (diff_index < limit.size() - 1 || start_byte + 1 < limit_byte) {
        // 快速检测，如果满足在diff_index位置把字符加1，截取start返回
        // eg:
        //   start: abc, limit: abd， 不满足，c + 1后，导致start == limit。
        //   start: abcb, limit: abda， 不满足，c + 1后，导致start > limit。
        //   start: abc, limit: abda, 满足， c + 1后，abd < abda， 返回abd。（满足条件：diff_index < limit.size() - 1）
        //   start: abc, limit: abe,  满足， c+1 < e， 返回abd。（满足条件：start_byte + 1 < limit_byte）
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
      } else {
        // 这里针对的情况：
        // eg:
        //   start: abc, limit: abd
        //   start: abcbbb, limit: abdaaa，虽然c比d小，但后续start都比limit大
        //   start: abcb, limit: abda，虽然c比d小，但后续start都比limit大

        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < start->size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>((*start)[diff_index]) <
              static_cast<uint8_t>(0xff)) {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            break;
          }
          diff_index++;
        }
      }
      assert(Compare(*start, limit) < 0);
    }
  }

  /**
   * key最短后继。将key变成一个比原key大的短字符串，并赋值给*key返回
   * 求值：Min(result)
   * 满足：result > key
   * eg：
   *      key         => result
   *   1. a           => b
   *   2. a a a       => b
   *   3. \0xff a     => \0xff b
   *   4. \0xff \0xff => \0xff \0xff
   * @param key
   */
  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }

  /**
   * 给定两个slice s和t，判断t是不是s的后继
   * eg: []表示变化的值，([empty]除外)
   *      s            t               result
   *   1. [empty]      [empty]      => false 都为空，不存在后继关系
   *   2. 1 2 3        1 2 3 0      => false 长度不一致
   *   2. 1 2 3        1 2 3        => false 相等，不存在后继关系
   *   3. 1 2 3        1 2 [4]      => true  满足长度一致，尾部差1
   *   4. 1 2 3 0xff   1 2 [4] 0x00 => true  满足类似进位操作，3 0xff +1 = 4 0x00
   * @param s
   * @param t
   * @return
   */
  bool IsSameLengthImmediateSuccessor(const Slice& s,
                                      const Slice& t) const override {
    // 大小为0，或长度不一样，直接返回false
    if (s.size() != t.size() || s.size() == 0) {
      return false;
    }

    // 两个slice中首个字节不等的索引位置
    size_t diff_ind = s.difference_offset(t);

    // same slice
    // diff_ind越界，说明没找到不等的索引位置，也就是连个slice相等
    if (diff_ind >= s.size()) return false;

    uint8_t byte_s = static_cast<uint8_t>(s[diff_ind]);
    uint8_t byte_t = static_cast<uint8_t>(t[diff_ind]);
    // first different byte must be consecutive, and remaining bytes must be
    // 0xff for s and 0x00 for t
    if (byte_s != uint8_t{0xff} && byte_s + 1 == byte_t) {
      for (size_t i = diff_ind + 1; i < s.size(); ++i) {
        byte_s = static_cast<uint8_t>(s[i]);
        byte_t = static_cast<uint8_t>(t[i]);
        if (byte_s != uint8_t{0xff} || byte_t != uint8_t{0x00}) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  bool CanKeysWithDifferentByteContentsBeEqual() const override {
    return false;
  }

  using Comparator::CompareWithoutTimestamp;
  int CompareWithoutTimestamp(const Slice& a, bool /*a_has_ts*/, const Slice& b,
                              bool /*b_has_ts*/) const override {
    return a.compare(b);
  }

  bool EqualWithoutTimestamp(const Slice& a, const Slice& b) const override {
    return a == b;
  }
};

class ReverseBytewiseComparatorImpl : public BytewiseComparatorImpl {
 public:
  ReverseBytewiseComparatorImpl() { }

  static const char* kClassName() {
    return "rocksdb.ReverseBytewiseComparator";
  }
  const char* Name() const override { return kClassName(); }

  int Compare(const Slice& a, const Slice& b) const override {
    return -a.compare(b);
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    assert(diff_index <= min_length);
    if (diff_index == min_length) {
      // Do not shorten if one string is a prefix of the other
      //
      // We could handle cases like:
      //     V
      // A A 2 X Y
      // A A 2
      // in a similar way as BytewiseComparator::FindShortestSeparator().
      // We keep it simple by not implementing it. We can come back to it
      // later when needed.
    } else {
      uint8_t start_byte = static_cast<uint8_t>((*start)[diff_index]);
      uint8_t limit_byte = static_cast<uint8_t>(limit[diff_index]);
      if (start_byte > limit_byte && diff_index < start->size() - 1) {
        // Case like
        //     V
        // A A 3 A A
        // A A 1 B B
        //
        // or
        //     v
        // A A 2 A A
        // A A 1 B B
        // In this case "AA2" will be good.
#ifndef NDEBUG
        std::string old_start = *start;
#endif
        start->resize(diff_index + 1);
#ifndef NDEBUG
        assert(old_start >= *start);
#endif
        assert(Slice(*start).compare(limit) > 0);
      }
    }
  }

  void FindShortSuccessor(std::string* /*key*/) const override {
    // Don't do anything for simplicity.
  }

  bool CanKeysWithDifferentByteContentsBeEqual() const override {
    return false;
  }

  using Comparator::CompareWithoutTimestamp;
  int CompareWithoutTimestamp(const Slice& a, bool /*a_has_ts*/, const Slice& b,
                              bool /*b_has_ts*/) const override {
    return -a.compare(b);
  }
};
}// namespace

const Comparator* BytewiseComparator() {
  static BytewiseComparatorImpl bytewise;
  return &bytewise;
}

const Comparator* ReverseBytewiseComparator() {
  static ReverseBytewiseComparatorImpl rbytewise;
  return &rbytewise;
}

#ifndef ROCKSDB_LITE
static int RegisterBuiltinComparators(ObjectLibrary& library,
                                      const std::string& /*arg*/) {
  library.Register<const Comparator>(
      BytewiseComparatorImpl::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<const Comparator>* /*guard */,
         std::string* /* errmsg */) { return BytewiseComparator(); });
  library.Register<const Comparator>(
      ReverseBytewiseComparatorImpl::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<const Comparator>* /*guard */,
         std::string* /* errmsg */) { return ReverseBytewiseComparator(); });
  return 2;
}
#endif  // ROCKSDB_LITE

Status Comparator::CreateFromString(const ConfigOptions& config_options,
                                    const std::string& value,
                                    const Comparator** result) {
#ifndef ROCKSDB_LITE
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinComparators(*(ObjectLibrary::Default().get()), "");
  });
#endif  // ROCKSDB_LITE
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, *result, value,
                                              &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  if (id == BytewiseComparatorImpl::kClassName()) {
    *result = BytewiseComparator();
  } else if (id == ReverseBytewiseComparatorImpl::kClassName()) {
    *result = ReverseBytewiseComparator();
  } else if (value.empty()) {
    // No Id and no options.  Clear the object
    *result = nullptr;
    return Status::OK();
  } else if (id.empty()) {  // We have no Id but have options.  Not good
    return Status::NotSupported("Cannot reset object ", id);
  } else {
#ifndef ROCKSDB_LITE
    status = config_options.registry->NewStaticObject(id, result);
#else
    status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif  // ROCKSDB_LITE
    if (!status.ok()) {
      if (config_options.ignore_unsupported_options &&
          status.IsNotSupported()) {
        return Status::OK();
      } else {
        return status;
      }
    } else if (!opt_map.empty()) {
      Comparator* comparator = const_cast<Comparator*>(*result);
      status = comparator->ConfigureFromMap(config_options, opt_map);
    }
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE
