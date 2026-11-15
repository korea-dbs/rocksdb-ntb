//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"

#include <dlfcn.h>
#include <pthread.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>

#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace {
#ifndef NPERF_CONTEXT
Statistics* stats_for_report(SystemClock* clock, Statistics* stats) {
  if (clock != nullptr && stats != nullptr &&
      stats->get_stats_level() > kExceptTimeForMutex) {
    return stats;
  } else {
    return nullptr;
  }
}
#endif  // NPERF_CONTEXT
}  // namespace


namespace {
inline uint64_t MuHoldNowNs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());
}
uint64_t MuHoldThresholdNs() {
  static const uint64_t t = [] {
    const char* e = std::getenv("RC_MUHOLD_MS");
    const double ms = e ? std::atof(e) : 20.0;
    fprintf(stderr, "[muhold] DB mutex hold/wait report threshold = %.1f ms (v-muhold)\n", ms);
    return static_cast<uint64_t>(ms * 1e6);
  }();
  return t;
}
uintptr_t MuHoldOff(void* ra) {
  Dl_info di{};
  if (ra != nullptr && dladdr(ra, &di) != 0 && di.dli_fbase != nullptr)
    return reinterpret_cast<uintptr_t>(ra) - reinterpret_cast<uintptr_t>(di.dli_fbase);
  return 0;
}
void MuHoldPrint(const char* kind, uint64_t ns, void* lock_ra, void* unlock_ra) {
  char name[32] = "?";
  pthread_getname_np(pthread_self(), name, sizeof(name));
  fprintf(stderr, "[muhold] kind=%s ms=%.1f thread=%s lock_off=0x%zx unlock_off=0x%zx t_ms=%llu\n",
          kind, ns / 1e6, name, static_cast<size_t>(MuHoldOff(lock_ra)),
          static_cast<size_t>(MuHoldOff(unlock_ra)),
          static_cast<unsigned long long>(MuHoldNowNs() / 1000000));
}
}  // namespace

void InstrumentedMutex::Lock() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_mutex_lock_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(clock_, stats_), stats_code_);
  const bool mh = (stats_code_ == DB_MUTEX_WAIT_MICROS);
  const uint64_t mh_t0 = mh ? MuHoldNowNs() : 0;
  LockInternal();
  if (mh) {
    void* ra = __builtin_return_address(0);
    const uint64_t t1 = MuHoldNowNs();
    if (t1 - mh_t0 >= MuHoldThresholdNs()) MuHoldPrint("wait", t1 - mh_t0, ra, nullptr);
    mh_lock_ra_ = ra;
    mh_start_ns_ = MuHoldNowNs();
  }
}

__attribute__((noinline)) void InstrumentedMutex::Unlock() {
  if (stats_code_ == DB_MUTEX_WAIT_MICROS && mh_start_ns_ != 0) {
    const uint64_t held = MuHoldNowNs() - mh_start_ns_;
    void* lra = mh_lock_ra_;
    mh_start_ns_ = 0;
    mutex_.Unlock();
    if (held >= MuHoldThresholdNs()) MuHoldPrint("hold", held, lra, __builtin_return_address(0));
    return;
  }
  mutex_.Unlock();
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
#ifdef COERCE_CONTEXT_SWITCH
  if (stats_code_ == DB_MUTEX_WAIT_MICROS) {
    thread_local Random rnd(301);
    if (rnd.OneIn(2)) {
      if (bg_cv_) {
        bg_cv_->SignalAll();
      }
      sched_yield();
    } else {
      uint32_t sleep_us = rnd.Uniform(11) * 1000;
      if (bg_cv_) {
        bg_cv_->SignalAll();
      }
      SystemClock::Default()->SleepForMicroseconds(sleep_us);
    }
  }
#endif
  mutex_.Lock();
}

void InstrumentedCondVar::Wait() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(clock_, stats_), stats_code_);
  const bool mh = (mh_mu_->stats_code_ == DB_MUTEX_WAIT_MICROS);
  if (mh && mh_mu_->mh_start_ns_ != 0) {
    const uint64_t held = MuHoldNowNs() - mh_mu_->mh_start_ns_;
    if (held >= MuHoldThresholdNs()) MuHoldPrint("hold", held, mh_mu_->mh_lock_ra_, __builtin_return_address(0));
  }
  WaitInternal();
  if (mh) { mh_mu_->mh_lock_ra_ = __builtin_return_address(0); mh_mu_->mh_start_ns_ = MuHoldNowNs(); }
}

void InstrumentedCondVar::WaitInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  cond_.Wait();
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(clock_, stats_), stats_code_);
  const bool mh = (mh_mu_->stats_code_ == DB_MUTEX_WAIT_MICROS);
  if (mh && mh_mu_->mh_start_ns_ != 0) {
    const uint64_t held = MuHoldNowNs() - mh_mu_->mh_start_ns_;
    if (held >= MuHoldThresholdNs()) MuHoldPrint("hold", held, mh_mu_->mh_lock_ra_, __builtin_return_address(0));
  }
  const bool r = TimedWaitInternal(abs_time_us);
  if (mh) { mh_mu_->mh_lock_ra_ = __builtin_return_address(0); mh_mu_->mh_start_ns_ = MuHoldNowNs(); }
  return r;
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif

  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);

  return cond_.TimedWait(abs_time_us);
}

}  // namespace ROCKSDB_NAMESPACE
