//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/rate_limiter.h"

#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

/**
 * 请求令牌来读取或写入字节并可能更新统计信息。
 * 考虑GetSingleBurstBytes()和对齐(例如，在直接I/O的情况下)来分配适当的字节数，这可能少于请求的字节数。
 * @param bytes 请求操作字节数
 * @param alignment 对齐大小
 * @param io_priority 操作优先级
 * @param stats 输出参数，操作状态
 * @param op_type
 * @return
 */
size_t RateLimiter::RequestToken(size_t bytes, size_t alignment,
                                 Env::IOPriority io_priority, Statistics* stats,
                                 RateLimiter::OpType op_type) {
  if (io_priority < Env::IO_TOTAL && IsRateLimited(op_type)) {
    bytes = std::min(bytes, static_cast<size_t>(GetSingleBurstBytes()));

    if (alignment > 0) {
      // Here we may actually require more than burst and block
      // but we can not write less than one page at a time on direct I/O
      // thus we may want not to use ratelimiter
      bytes = std::max(alignment, TruncateToPageBoundary(alignment, bytes));
    }
    Request(bytes, io_priority, stats, op_type);
  }
  return bytes;
}

// Pending request
struct GenericRateLimiter::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu)
      : request_bytes(_bytes), bytes(_bytes), cv(_mu), granted(false) {}
  int64_t request_bytes;
  int64_t bytes;
  port::CondVar cv;
  bool granted;
};

GenericRateLimiter::GenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness,
    RateLimiter::Mode mode, const std::shared_ptr<SystemClock>& clock,
    bool auto_tuned)
    : RateLimiter(mode),
      refill_period_us_(refill_period_us),
      rate_bytes_per_sec_(auto_tuned ? rate_bytes_per_sec / 2
                                     : rate_bytes_per_sec),
      refill_bytes_per_period_(
          CalculateRefillBytesPerPeriod(rate_bytes_per_sec_)),
      clock_(clock),
      stop_(false),
      exit_cv_(&request_mutex_),
      requests_to_wait_(0),
      available_bytes_(0),
      next_refill_us_(NowMicrosMonotonic()),
      fairness_(fairness > 100 ? 100 : fairness),
      rnd_((uint32_t)time(nullptr)),
      leader_(nullptr),
      auto_tuned_(auto_tuned),
      num_drains_(0),
      prev_num_drains_(0),
      max_bytes_per_sec_(rate_bytes_per_sec),
      tuned_time_(NowMicrosMonotonic()) {
  total_requests_[0] = 0;
  total_requests_[1] = 0;
  total_bytes_through_[0] = 0;
  total_bytes_through_[1] = 0;
}

GenericRateLimiter::~GenericRateLimiter() {
  MutexLock g(&request_mutex_);
  stop_ = true;
  requests_to_wait_ = static_cast<int32_t>(queue_[Env::IO_LOW].size() +
                                           queue_[Env::IO_HIGH].size());
  for (auto& r : queue_[Env::IO_HIGH]) {
    r->cv.Signal();
  }
  for (auto& r : queue_[Env::IO_LOW]) {
    r->cv.Signal();
  }
  while (requests_to_wait_ > 0) {
    exit_cv_.Wait();
  }
}

// This API allows user to dynamically change rate limiter's bytes per second.
void GenericRateLimiter::SetBytesPerSecond(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  rate_bytes_per_sec_ = bytes_per_second;
  refill_bytes_per_period_.store(
      CalculateRefillBytesPerPeriod(bytes_per_second),
      std::memory_order_relaxed);
}

/**
 * 请求一个token
 * 每个周期可获取token流量：token = rate_bytes_per_sec * (refill_period_us / 1000) ms / 1000ms
 * @param bytes 申请操作的字节大小
 * @param pri 操作优先级
 * @param stats 输出参数，操作统计
 */
void GenericRateLimiter::Request(int64_t bytes, const Env::IOPriority pri,
                                 Statistics* stats) {
  assert(bytes <= refill_bytes_per_period_.load(std::memory_order_relaxed));
  TEST_SYNC_POINT("GenericRateLimiter::Request");
  TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:1",
                           &rate_bytes_per_sec_);
  MutexLock g(&request_mutex_);

  if (auto_tuned_) {
    static const int kRefillsPerTune = 100;
    std::chrono::microseconds now(NowMicrosMonotonic());
    if (now - tuned_time_ >=
        kRefillsPerTune * std::chrono::microseconds(refill_period_us_)) {
      Status s = Tune();
      s.PermitUncheckedError();  //**TODO: What to do on error?
    }
  }

  if (stop_) {
    return;
  }

  // 对应优先级的操作次数计数
  ++total_requests_[pri];

  // 剩余字节额度满足
  if (available_bytes_ >= bytes) {
    // Refill thread assigns quota and notifies requests waiting on
    // the queue under mutex. So if we get here, that means nobody
    // is waiting?
    available_bytes_ -= bytes; // 获取额度
    total_bytes_through_[pri] += bytes; // 对应优先级的分配字节统计
    return;
  }

  // Request cannot be satisfied at this moment, enqueue
  // 额度不够， 把申请封装成Req放到队列
  Req r(bytes, &request_mutex_);
  queue_[pri].push_back(&r);

  do {
    bool timedout = false;
    // Leader election, candidates can be:
    // (1) a new incoming request,
    // (2) a previous leader, whose quota has not been not assigned yet due
    //     to lower priority
    // (3) a previous waiter at the front of queue, who got notified by
    //     previous leader
    if (leader_ == nullptr &&
        ((!queue_[Env::IO_HIGH].empty() &&
            &r == queue_[Env::IO_HIGH].front()) ||
         (!queue_[Env::IO_LOW].empty() &&
            &r == queue_[Env::IO_LOW].front()))) {
      // 如果当前没有leader，且队首（先high再low判断）是自己，说明自己是竞争到了leader
      // 疑问：入队是queue_[pri].push_back(&r)，指定了pri；查找为什么还要指定high和low

      // 竞争到leader_
      leader_ = &r;

      // 查出refill窗口内的剩余时间
      int64_t delta = next_refill_us_ - NowMicrosMonotonic();
      delta = delta > 0 ? delta : 0;

      if (delta == 0) {
        // 窗口过期
        timedout = true;
      } else {
        // 计算可以在本次窗口内的等待时长
        int64_t wait_until = clock_->NowMicros() + delta;

        // todo: 消费次数计数？
        RecordTick(stats, NUMBER_RATE_LIMITER_DRAINS);

        // 一个配额窗口用完，增加计数
        ++num_drains_;
        // 这里模拟阻塞，因为窗口配额分配完了，但时间还没到，这里等到到窗口边界
        timedout = r.cv.TimedWait(wait_until);
      }
    } else {
      // Not at the front of queue or an leader has already been elected
      r.cv.Wait();
    }

    // request_mutex_ is held from now on
    if (stop_) {
      --requests_to_wait_;
      exit_cv_.Signal();
      return;
    }

    // Make sure the waken up request is always the header of its queue
    assert(r.granted ||
           (!queue_[Env::IO_HIGH].empty() &&
            &r == queue_[Env::IO_HIGH].front()) ||
           (!queue_[Env::IO_LOW].empty() &&
            &r == queue_[Env::IO_LOW].front()));
    assert(leader_ == nullptr ||
           (!queue_[Env::IO_HIGH].empty() &&
            leader_ == queue_[Env::IO_HIGH].front()) ||
           (!queue_[Env::IO_LOW].empty() &&
            leader_ == queue_[Env::IO_LOW].front()));

    if (leader_ == &r) {
      // Waken up from TimedWait()
      // 窗口超时到期，开启填充下一个窗口
      if (timedout) {
        // Time to do refill!
        // 填充下一个窗口配额
        Refill();

        // Re-elect a new leader regardless. This is to simplify the
        // election handling.
        // 这里表明要重新选举。因为Refill()会对队列里请求，尝试进行处理。
        // 有可能全处理，有可能处理一半。就是处理进度不确定，leader_对象有没有被处理也不确定，
        // 那重新选举就好了。
        leader_ = nullptr;

        // Notify the header of queue if current leader is going away
        if (r.granted) {
          // Current leader already got granted with quota. Notify header
          // of waiting queue to participate next round of election.
          assert((queue_[Env::IO_HIGH].empty() ||
                    &r != queue_[Env::IO_HIGH].front()) &&
                 (queue_[Env::IO_LOW].empty() ||
                    &r != queue_[Env::IO_LOW].front()));
          if (!queue_[Env::IO_HIGH].empty()) {
            queue_[Env::IO_HIGH].front()->cv.Signal();
          } else if (!queue_[Env::IO_LOW].empty()) {
            queue_[Env::IO_LOW].front()->cv.Signal();
          }
          // Done
          break;
        }
      } else {
        // Spontaneous wake up, need to continue to wait
        assert(!r.granted);
        leader_ = nullptr;
      }
    } else {
      // Waken up by previous leader:
      // (1) if requested quota is granted, it is done.
      // (2) if requested quota is not granted, this means current thread
      // was picked as a new leader candidate (previous leader got quota).
      // It needs to participate leader election because a new request may
      // come in before this thread gets waken up. So it may actually need
      // to do Wait() again.
      assert(!timedout);
    }
  } while (!r.granted);
}

/**
 * 重新分配窗口，填充token配额
 * 1. 把上一期没用完的配额，加到当期
 * 2. 对上一期优先级队列里的请求，协助处理
 */
void GenericRateLimiter::Refill() {
  TEST_SYNC_POINT("GenericRateLimiter::Refill");
  // 本期refill窗口的结束位置
  next_refill_us_ = NowMicrosMonotonic() + refill_period_us_;

  // Carry over the left over quota from the last period
  // 上一期窗口配额没用完，转接到本期。
  auto refill_bytes_per_period =
      refill_bytes_per_period_.load(std::memory_order_relaxed);
  if (available_bytes_ < refill_bytes_per_period) {
    available_bytes_ += refill_bytes_per_period;
  }

  // 根据优先级计算，如果fairness_ = 10， rnd_.OneIn(fairness_)为true表示1/10概率
  // 计算结果：1/10概率为0，9/10概率为1。0和1用于配合后面循环
  int use_low_pri_first = rnd_.OneIn(fairness_) ? 0 : 1;

  for (int q = 0; q < 2; ++q) {
    // 循环两次，q值分别为0和1，use_low_pri_first也是0和1，
    // 两轮循环use_pri结果只有两种：1. IO_LOW、IO_HIGH， 2.IO_HIGH、IO_LOW
    // 集合use_low_pri_first的概率，得出结论：
    // 1/10概率，先取IO_LOW队列进行执行；9/10概率取IO_HIGH队列进行执行。
    auto use_pri = (use_low_pri_first == q) ? Env::IO_LOW : Env::IO_HIGH;

    // 按概率定位优先级队列
    auto* queue = &queue_[use_pri];
    while (!queue->empty()) {
      auto* next_req = queue->front();

      // 申请字节数太大了。 这里直接break，没有进行next_req->granted，相当于不处理了
      // 这里应该是要等到下一个窗口，在统计
      if (available_bytes_ < next_req->request_bytes) {
        // avoid starvation
        // 以为超出当前窗口配额，先扣除，直接退出，等到下个窗口分配时在检查是否够。
        next_req->request_bytes -= available_bytes_;
        // 当前窗口配额清零
        available_bytes_ = 0;
        break;
      }

      // 扣除配额
      available_bytes_ -= next_req->request_bytes;
      next_req->request_bytes = 0;
      // 已分配计数
      total_bytes_through_[use_pri] += next_req->bytes;
      // 出队该请求
      queue->pop_front();

      // 标记配额已授权
      next_req->granted = true;

      if (next_req != leader_) {
        // Quota granted, signal the thread
        // 配额已经授权，唤醒等待线程
        next_req->cv.Signal();
      }
    }
  }
}

int64_t GenericRateLimiter::CalculateRefillBytesPerPeriod(
    int64_t rate_bytes_per_sec) {
  if (port::kMaxInt64 / rate_bytes_per_sec < refill_period_us_) {
    // Avoid unexpected result in the overflow case. The result now is still
    // inaccurate but is a number that is large enough.
    return port::kMaxInt64 / 1000000;
  } else {
    return std::max(kMinRefillBytesPerPeriod,
                    rate_bytes_per_sec * refill_period_us_ / 1000000);
  }
}

Status GenericRateLimiter::Tune() {
  const int kLowWatermarkPct = 50;
  const int kHighWatermarkPct = 90;
  const int kAdjustFactorPct = 5;
  // computed rate limit will be in
  // `[max_bytes_per_sec_ / kAllowedRangeFactor, max_bytes_per_sec_]`.
  const int kAllowedRangeFactor = 20;

  std::chrono::microseconds prev_tuned_time = tuned_time_;
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonic());

  int64_t elapsed_intervals = (tuned_time_ - prev_tuned_time +
                               std::chrono::microseconds(refill_period_us_) -
                               std::chrono::microseconds(1)) /
                              std::chrono::microseconds(refill_period_us_);
  // We tune every kRefillsPerTune intervals, so the overflow and division-by-
  // zero conditions should never happen.
  assert(num_drains_ - prev_num_drains_ <= port::kMaxInt64 / 100);
  assert(elapsed_intervals > 0);
  int64_t drained_pct =
      (num_drains_ - prev_num_drains_) * 100 / elapsed_intervals;

  int64_t prev_bytes_per_sec = GetBytesPerSecond();
  int64_t new_bytes_per_sec;
  if (drained_pct == 0) {
    new_bytes_per_sec = max_bytes_per_sec_ / kAllowedRangeFactor;
  } else if (drained_pct < kLowWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec =
        std::min(prev_bytes_per_sec, port::kMaxInt64 / 100);
    new_bytes_per_sec =
        std::max(max_bytes_per_sec_ / kAllowedRangeFactor,
                 sanitized_prev_bytes_per_sec * 100 / (100 + kAdjustFactorPct));
  } else if (drained_pct > kHighWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec = std::min(
        prev_bytes_per_sec, port::kMaxInt64 / (100 + kAdjustFactorPct));
    new_bytes_per_sec =
        std::min(max_bytes_per_sec_,
                 sanitized_prev_bytes_per_sec * (100 + kAdjustFactorPct) / 100);
  } else {
    new_bytes_per_sec = prev_bytes_per_sec;
  }
  if (new_bytes_per_sec != prev_bytes_per_sec) {
    SetBytesPerSecond(new_bytes_per_sec);
  }
  num_drains_ = prev_num_drains_;
  return Status::OK();
}

RateLimiter* NewGenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us /* = 100 * 1000 */,
    int32_t fairness /* = 10 */,
    RateLimiter::Mode mode /* = RateLimiter::Mode::kWritesOnly */,
    bool auto_tuned /* = false */) {
  assert(rate_bytes_per_sec > 0);
  assert(refill_period_us > 0);
  assert(fairness > 0);
  return new GenericRateLimiter(rate_bytes_per_sec, refill_period_us, fairness,
                                mode, SystemClock::Default(), auto_tuned);
}

}  // namespace ROCKSDB_NAMESPACE
