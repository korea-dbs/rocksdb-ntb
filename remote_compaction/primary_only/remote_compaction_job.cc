// ============================================================
//  primary_only/remote_compaction_job.cc  (v16 – DispatchAsync 실패 시 release_cb 누락 수정)
//  [PRIMARY (srv3) 전용]
//
//  v14 유지:
//    - FinishJob: fnum 재발급으로 "output fnum already in LSM tree" 해결
//
//  v15 유지:
//    - FinishJob: fnum 재발급 + key range 충돌 검사 + LogAndApply를
//      단일 mutex 블록 안에서 처리
// ============================================================
#include <fcntl.h>  // v-psplit
#include <unistd.h>  // v-psplit
#include <atomic>  // v-psplit
#include <thread>  // v-psplit
#include <vector>  // v-psplit
#include <cstring>  // v-psplit
#include <cerrno>  // v-psplit
#include <cstdlib>  // v-psplit
#include <cstdio>  // v-psplit
#include <algorithm>  // v-psplit
#include "file/writable_file_writer.h"  // v-prdb
#include "rocksdb/file_system.h"  // v-prdb
#include "remote_compaction_job.h"
#include <chrono>          // v-preopen
#include <limits>
#include <mutex>
#include <unordered_map>
#include "db/column_family.h"
#include "db/table_cache.h"
#include "rocksdb/table_properties.h"
namespace {  // v-preopen: 미리 연 출력 파일의 tail_size를 AddFile까지 전달 (file_number -> tail_size)
std::mutex g_rc_tail_mu;
std::unordered_map<uint64_t, uint64_t> g_rc_tail;
void RcPutTailSize(uint64_t fnum, uint64_t ts) {
  std::lock_guard<std::mutex> lk(g_rc_tail_mu);
  g_rc_tail[fnum] = ts;
}
uint64_t RcTakeTailSize(uint64_t fnum) {
  std::lock_guard<std::mutex> lk(g_rc_tail_mu);
  auto it = g_rc_tail.find(fnum);
  if (it == g_rc_tail.end()) return 0;
  const uint64_t v = it->second;
  g_rc_tail.erase(it);
  return v;
}
}  // namespace
#include "db/compaction/cmpout_log.h"  // v-cmpout

#include <chrono>
#include <thread>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <fcntl.h>   // v-fsync
#include <unistd.h>  // v-fsync
#include <atomic>
#include <fstream>
#include <memory>

#include "db/compaction/compaction.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/table_reader.h"
#include "util/autovector.h"
#include "util/mutexlock.h"

#include "../shared/ntb_mem_file.h"
#include <cstdlib>   // v-pdirect
#include <cstring>   // v-pdirect
#include <string>    // v-pdirect
#include <sys/types.h>  // v-pdirect

// ---- v-pdirect ----
static bool RcPersistDirectOn() {
  static const bool on = [] {
    const char* e = getenv("RC_PERSIST_DIRECT");
    bool v = !(e && e[0] == '0');
    fprintf(stderr, "[rc] output persist: direct=%d (v-pdirect)\n", v ? 1 : 0);
    return v;
  }();
  return on;
}

//Dohwan
// ===== v-puring: RC_PERSIST_URING=1 -> 출력 SST 쓰기를 io_uring으로 =====
// 스레드마다 ring 하나 + 4KB 정렬 버퍼 QD개(fixed buffer로 등록)를 두고,
// PIECE 크기 조각을 최대 QD개까지 동시에 제출한다. 기존 기본 경로
// (RcPersistWriteFileRdb: O_DIRECT + preallocation + fdatasync)와 조건을
// 맞추기 위해 O_DIRECT(RC_PERSIST_DIRECT)와 fallocate(KEEP_SIZE)를 유지하고,
// 마지막 조각의 4KB 패딩은 ftruncate로 잘라낸다(IORING_OP_FTRUNCATE는 커널
// 6.9+라 동기 호출). fdatasync도 동기 호출(내구성 의미는 기존과 동일).
//   RC_PERSIST_URING=1       켜기 (기본 0 = 기존 경로)
//   RC_URING_QD=N            동시 제출 수 (기본 16, 1..256)
//   RC_URING_PIECE_KB=K      조각 크기 KB (기본 1024, 4KB 배수, 4..65536)
//   RC_URING_PREALLOC=0      fallocate 끄기 (기본 1 = 기존 Rdb 경로와 같음)
#ifdef ROCKSDB_IOURING_PRESENT
#include <liburing.h>
#endif
#include <vector>
struct RcUringCfg {
  bool   on       = false;
  int    qd       = 16;
  size_t piece    = 1u << 20;
  bool   prealloc = true;
};
static const RcUringCfg& RcUringConfig() {
  static const RcUringCfg cfg = [] {
    RcUringCfg c;
    const char* e = std::getenv("RC_PERSIST_URING");
    c.on = e && std::atoi(e) != 0;
    if ((e = std::getenv("RC_URING_QD")) && *e) c.qd = std::atoi(e);
    if (c.qd < 1) c.qd = 1;
    if (c.qd > 256) c.qd = 256;
    long kb = 1024;
    if ((e = std::getenv("RC_URING_PIECE_KB")) && *e) kb = std::atol(e);
    if (kb < 4) kb = 4;
    if (kb > 65536) kb = 65536;
    kb = (kb + 3) / 4 * 4;
    c.piece = static_cast<size_t>(kb) << 10;
    if ((e = std::getenv("RC_URING_PREALLOC")) && *e) c.prealloc = std::atoi(e) != 0;
#ifndef ROCKSDB_IOURING_PRESENT
    if (c.on) {
      fprintf(stderr, "[rc] RC_PERSIST_URING=1 but built without liburing -> off (v-puring)\n");
      c.on = false;
    }
#endif
    if (c.on)
      fprintf(stderr, "[rc] output persist: io_uring qd=%d piece=%zuKB prealloc=%d (v-puring)\n",
              c.qd, c.piece >> 10, c.prealloc ? 1 : 0);
    return c;
  }();
  return cfg;
}
static inline bool RcPersistUringOn() { return RcUringConfig().on; }

#ifdef ROCKSDB_IOURING_PRESENT
// 스레드당 하나. persist 스레드(finish/stream)가 오래 살아서 ring과 버퍼를 재사용한다.
struct RcUringCtx {
  struct io_uring     ring;
  bool                ring_inited = false;
  bool                ok          = false;
  bool                fixed       = false;
  int                 qd          = 0;
  size_t              piece       = 0;
  std::vector<char*>  bufs;
  RcUringCtx() {
    const RcUringCfg& c = RcUringConfig();
    qd    = c.qd;
    piece = c.piece;
    int r = io_uring_queue_init(static_cast<unsigned>(qd), &ring, 0);
    if (r < 0) {
      fprintf(stderr, "[rc] v-puring: io_uring_queue_init failed: %s -> fallback\n", strerror(-r));
      return;
    }
    ring_inited = true;
    bufs.assign(static_cast<size_t>(qd), nullptr);
    std::vector<struct iovec> iov(static_cast<size_t>(qd));
    for (int i = 0; i < qd; i++) {
      void* p = nullptr;
      if (posix_memalign(&p, 4096, piece) != 0) {
        fprintf(stderr, "[rc] v-puring: posix_memalign failed -> fallback\n");
        return;  // 소멸자가 정리
      }
      bufs[i] = static_cast<char*>(p);
      iov[i].iov_base = p;
      iov[i].iov_len  = piece;
    }
    fixed = io_uring_register_buffers(&ring, iov.data(), static_cast<unsigned>(qd)) == 0;
    ok = true;
  }
  ~RcUringCtx() {
    if (ring_inited) {
      if (fixed) io_uring_unregister_buffers(&ring);
      io_uring_queue_exit(&ring);
    }
    for (char* b : bufs) free(b);
  }
};
static RcUringCtx* RcUringGet() {
  thread_local std::unique_ptr<RcUringCtx> ctx;
  if (!ctx) ctx.reset(new RcUringCtx());
  return ctx->ok ? ctx.get() : nullptr;
}

struct RcUringStats {
  std::atomic<long long> files{0}, bytes{0}, write_us{0}, sync_us{0};
};
static RcUringStats g_rc_uring_stats;

// 0 = 성공, 아니면 errno. *handled=false 면 io_uring을 못 써서 호출자가 기존 경로로 진행.
static int RcPersistWriteFileUring(const std::string& dst, const char* data, size_t size,
                                   bool* used_direct, bool* handled) {
  *handled = false;
  RcUringCtx* u = RcUringGet();
  if (u == nullptr) return 0;
  *handled = true;
  const size_t kAlign = 4096;
  const RcUringCfg& cfg = RcUringConfig();
  *used_direct = false;

  int fd = -1;
  if (RcPersistDirectOn()) {
    fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_CLOEXEC, 0644);
    if (fd >= 0) {
      *used_direct = true;
    } else if (errno != EINVAL) {
      return errno;  // EINVAL = fs가 O_DIRECT 미지원 -> 아래 buffered
    }
  }
  if (fd < 0) {
    fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return errno;
  }
  const bool direct = *used_direct;
  if (cfg.prealloc && size > 0) {
    // 기존 Rdb 경로의 SetPreallocationBlockSize(size)와 같은 효과. 실패해도 쓰기는 계속.
    (void)::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, static_cast<off_t>(size));
  }

  auto t0 = std::chrono::steady_clock::now();
  struct Slot { size_t off = 0, len = 0, done = 0; };
  std::vector<Slot> slot(static_cast<size_t>(u->qd));
  std::vector<int>  free_idx;
  free_idx.reserve(static_cast<size_t>(u->qd));
  for (int i = u->qd - 1; i >= 0; i--) free_idx.push_back(i);

  int    err         = 0;
  int    outstanding = 0;  // 제출했고 아직 CQE를 못 받은 SQE 수
  size_t next_off    = 0;
  bool   padded      = false;

  auto prep = [&](int idx) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&u->ring);  // outstanding <= qd 라 항상 있음
    Slot& s = slot[idx];
    char* p = u->bufs[idx] + s.done;
    const unsigned n = static_cast<unsigned>(s.len - s.done);
    const uint64_t o = static_cast<uint64_t>(s.off + s.done);
    if (u->fixed) io_uring_prep_write_fixed(sqe, fd, p, n, o, idx);
    else          io_uring_prep_write(sqe, fd, p, n, o);
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(idx));
    outstanding++;
  };

  while (err == 0 && (next_off < size || outstanding > 0)) {
    while (next_off < size && !free_idx.empty()) {
      const int idx = free_idx.back();
      free_idx.pop_back();
      const size_t n  = std::min(cfg.piece, size - next_off);
      size_t       wn = n;
      memcpy(u->bufs[idx], data + next_off, n);
      if (direct && (n % kAlign) != 0) {  // 마지막 조각만 해당
        wn = (n + kAlign - 1) / kAlign * kAlign;
        memset(u->bufs[idx] + n, 0, wn - n);
        padded = true;
      }
      slot[idx].off  = next_off;
      slot[idx].len  = wn;
      slot[idx].done = 0;
      prep(idx);
      next_off += n;
    }
    int r = io_uring_submit_and_wait(&u->ring, 1);
    if (r < 0 && r != -EINTR) { err = -r; break; }
    struct io_uring_cqe* cqe = nullptr;
    while (err == 0 && io_uring_peek_cqe(&u->ring, &cqe) == 0) {
      const int idx = static_cast<int>(io_uring_cqe_get_data64(cqe));
      const int res = cqe->res;
      io_uring_cqe_seen(&u->ring, cqe);
      outstanding--;
      Slot& s = slot[idx];
      if (res == -EAGAIN || res == -EINTR) {
        prep(idx);
      } else if (res < 0) {
        err = -res;
      } else if (res == 0) {
        err = EIO;
      } else {
        s.done += static_cast<size_t>(res);
        if (s.done < s.len) {
          if (direct && (s.done % kAlign) != 0) err = EIO;  // O_DIRECT 정렬 깨진 short write
          else prep(idx);                                 // 남은 부분 다시 제출
        } else {
          free_idx.push_back(idx);
        }
      }
    }
  }
  // 에러로 빠져나와도 버퍼가 커널에서 쓰이는 중일 수 있으므로 남은 CQE를 모두 회수한다.
  if (outstanding > 0) io_uring_submit(&u->ring);
  while (outstanding > 0) {
    struct io_uring_cqe* cqe = nullptr;
    int r = io_uring_wait_cqe(&u->ring, &cqe);
    if (r == -EINTR) continue;
    if (r < 0) break;  // ring 자체 이상: 더 기다릴 수 없음
    io_uring_cqe_seen(&u->ring, cqe);
    outstanding--;
  }

  // 패딩 제거: 실제 SST 크기로 (RocksDB PosixWritableFile::Close의 ftruncate와 같음)
  if (err == 0 && padded && ::ftruncate(fd, static_cast<off_t>(size)) != 0) err = errno;
  auto t1 = std::chrono::steady_clock::now();
  if (err == 0 && ::fdatasync(fd) != 0) err = errno;
  auto t2 = std::chrono::steady_clock::now();
  if (::close(fd) != 0 && err == 0) err = errno;

  if (err == 0) {
    RcUringStats& st = g_rc_uring_stats;
    const long long w = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    const long long y = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    st.bytes += static_cast<long long>(size);
    st.write_us += w;
    st.sync_us += y;
    const long long n = ++st.files;
    if (n % 200 == 0) {
      const double wms = st.write_us.load() / 1000.0;
      fprintf(stderr,
              "[rc] v-puring: files=%lld avg_write_ms=%.1f avg_sync_ms=%.1f write_MBps=%.0f fixed=%d\n",
              n, wms / n, st.sync_us.load() / 1000.0 / n,
              wms > 0 ? (st.bytes.load() / 1048576.0) / (wms / 1000.0) : 0.0, u->fixed ? 1 : 0);
    }
  }
  return err;
}
#endif  // ROCKSDB_IOURING_PRESENT
// ===== end v-puring =====
//Dohwan end
// ===== v-pdefer: RC_PERSIST_DEFER=1 → 출력 SST를 fsync 없이 쓰고 fdatasync는 백그라운드에서 (측정용, crash-safe 아님) =====
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
static bool RcPersistDeferOn() {
  static const bool on = [] {
    const char* e = std::getenv("RC_PERSIST_DEFER");
    bool v = e && std::atoi(e) != 0;
    if (v) fprintf(stderr, "[rc] output persist: DEFERRED sync (measurement only, not crash-safe) (v-pdefer)\n");
    return v;
  }();
  return on;
}
struct RcDeferSyncer {
  std::mutex mu; std::condition_variable cv; std::deque<int> q;
  std::atomic<long long> files{0}; std::atomic<long long> total_us{0};
  RcDeferSyncer() {
    const char* e = std::getenv("RC_PERSIST_DEFER_THREADS");
    int n = e ? std::atoi(e) : 1; if (n < 1) n = 1; if (n > 16) n = 16;
    fprintf(stderr, "[rc] deferred sync threads=%d (RC_PERSIST_DEFER_THREADS) (v-pdefer)\n", n);
    for (int i = 0; i < n; i++) std::thread([this] { pthread_setname_np(pthread_self(), "rc_dsync"); Loop(); }).detach();
  }
  void Push(int fd) { { std::lock_guard<std::mutex> l(mu); q.push_back(fd); } cv.notify_one(); }
  void Loop() {
    for (;;) {
      int fd; size_t backlog;
      { std::unique_lock<std::mutex> l(mu); cv.wait(l, [this] { return !q.empty(); }); fd = q.front(); q.pop_front(); backlog = q.size(); }
      auto t0 = std::chrono::steady_clock::now();
      ::fdatasync(fd); ::close(fd);
      long long us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count();
      long long n = ++files; total_us += us;
      if (n % 200 == 0) fprintf(stderr, "[rc] deferred sync: files=%lld avg_ms=%.1f backlog=%zu (v-pdefer)\n", n, total_us.load() / 1000.0 / n, backlog);
    }
  }
  static RcDeferSyncer& Get() { static RcDeferSyncer* s = new RcDeferSyncer(); return *s; }
};
static inline const char* RcDeferPath(const std::string& p) { return p.c_str(); }
static inline const char* RcDeferPath(const char* p) { return p; }
template <typename D>
static int RcPersistWriteFileDefer(const D& dst, const void* data, size_t size, bool* used_direct) {
  if (used_direct) *used_direct = false;
  int fd = ::open(RcDeferPath(dst), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return errno;
  const char* p = static_cast<const char*>(data); size_t left = size;
  while (left > 0) {
    ssize_t w = ::write(fd, p, left);
    if (w < 0) { if (errno == EINTR) continue; int e = errno; ::close(fd); return e; }
    p += w; left -= (size_t)w;
  }
  RcDeferSyncer::Get().Push(fd);  // fd는 sync 스레드가 fdatasync 후 close
  return 0;
}
// ===== end v-pdefer =====
// 0 = 성공, 아니면 errno.  *used_direct = 실제로 O_DIRECT로 썼는지
static int RcPersistWriteFile(const std::string& dst, const char* data, size_t size,
                              bool* used_direct) {
  if (RcPersistDeferOn()) return RcPersistWriteFileDefer(dst, data, size, used_direct);  // v-pdefer
//Dohwan
#ifdef ROCKSDB_IOURING_PRESENT
  if (RcPersistUringOn()) {  // v-puring
    bool handled = false;
    int e = RcPersistWriteFileUring(dst, data, size, used_direct, &handled);
    if (handled) return e;
  }
#endif
//Dohwan end
  const size_t kAlign = 4096;
  const size_t kPiece = static_cast<size_t>(4) << 20;  // 4 MiB (kAlign 배수)
  *used_direct = false;
  int fd = -1;
  if (RcPersistDirectOn()) {
    fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_CLOEXEC, 0644);
    if (fd >= 0) {
      *used_direct = true;
    } else if (errno != EINVAL) {
      return errno;  // EINVAL = fs가 O_DIRECT 미지원 -> 아래 buffered
    }
  }
  if (fd < 0) {
    fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return errno;
  }
  int err = 0;
  if (*used_direct) {
    void* bounce = nullptr;
    if (posix_memalign(&bounce, kAlign, kPiece) != 0) err = ENOMEM;
    size_t off = 0;
    while (err == 0 && off < size) {
      size_t n = (size - off < kPiece) ? (size - off) : kPiece;
      size_t wn = (n + kAlign - 1) / kAlign * kAlign;  // 마지막 조각만 패딩됨
      memcpy(bounce, data + off, n);
      if (wn > n) memset(static_cast<char*>(bounce) + n, 0, wn - n);
      size_t done = 0;
      while (done < wn) {
        ssize_t w = ::pwrite(fd, static_cast<char*>(bounce) + done, wn - done,
                             static_cast<off_t>(off + done));
        if (w < 0) {
          if (errno == EINTR) continue;
          err = errno;
          break;
        }
        if (w == 0) { err = EIO; break; }
        done += static_cast<size_t>(w);
      }
      off += n;
    }
    free(bounce);
    // 패딩 제거: 실제 SST 크기로 (RocksDB PosixWritableFile::Close의 ftruncate(fd_, filesize_)와 같음)
    if (err == 0 && ::ftruncate(fd, static_cast<off_t>(size)) != 0) err = errno;
  } else {
    size_t off = 0;
    while (off < size) {
      ssize_t w = ::write(fd, data + off, size - off);
      if (w < 0) {
        if (errno == EINTR) continue;
        err = errno;
        break;
      }
      if (w == 0) { err = EIO; break; }
      off += static_cast<size_t>(w);
    }
  }
  if (err == 0 && ::fdatasync(fd) != 0) err = errno;
  if (::close(fd) != 0 && err == 0) err = errno;
  return err;
}
// ---- end v-pdirect ----
static bool RcPersistRdbOn() {
  static const bool v = [] {
    const char* e = std::getenv("RC_PERSIST_RDB");
    bool on = !(e != nullptr && std::atoi(e) == 0);
    fprintf(stderr, "[rc] output persist writer: %s (v-prdb)\n",
            on ? "rocksdb WritableFileWriter" : "raw pwrite (v-pdirect)");
    return on;
  }();
  return v;
}
static size_t RcPersistWbufBytes() {
  static const size_t v = [] {
    const char* e = std::getenv("RC_PERSIST_WBUF_KB");
    long kb = (e && *e) ? std::atol(e) : 1024;
    if (kb < 64) kb = 64;
    fprintf(stderr, "[rc] output persist writer buffer: %ld KiB (v-prdb)\n", kb);
    return static_cast<size_t>(kb) << 10;
  }();
  return v;
}
static int RcPersistWriteFileRdb(const std::string& dst, const char* data, size_t size,
                                 bool* used_direct) {
  if (RcPersistDeferOn()) return RcPersistWriteFileDefer(dst, data, size, used_direct);  // v-pdefer
//Dohwan
#ifdef ROCKSDB_IOURING_PRESENT
  if (RcPersistUringOn()) {  // v-puring
    bool handled = false;
    int e = RcPersistWriteFileUring(dst, data, size, used_direct, &handled);
    if (handled) return e;
  }
#endif
//Dohwan end
  using namespace ROCKSDB_NAMESPACE;  // v-prdb: helper가 namespace 밖에 있음
  *used_direct = RcPersistDirectOn();
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  FileOptions fo;
  fo.use_direct_writes = *used_direct;
  fo.writable_file_max_buffer_size = RcPersistWbufBytes();
  std::unique_ptr<FSWritableFile> wf;
  IOStatus s = fs->NewWritableFile(dst, fo, &wf, nullptr);
  if (!s.ok()) {
    fprintf(stderr, "[rc] v-prdb: NewWritableFile %s: %s\n", dst.c_str(), s.ToString().c_str());
    return EIO;
  }
  wf->SetPreallocationBlockSize(size);
  WritableFileWriter w(std::move(wf), dst, fo);
  IOOptions io;
  s = w.Append(io, Slice(data, size));
  if (s.ok()) s = w.Sync(io, /*use_fsync=*/false);
  IOStatus cs = w.Close(io);
  if (s.ok()) s = cs;
  if (!s.ok()) {
    fprintf(stderr, "[rc] v-prdb: write %s: %s\n", dst.c_str(), s.ToString().c_str());
    return EIO;
  }
  return 0;
}
// ---- end v-prdb ----


// v-stream: dispatcher(다른 파일)에서 쓰는 persist 진입점 -- 기존 persist와 같은 쓰기 경로

// ===== v-psplit: 출력 파일 1개를 RC_PERSIST_SPLIT개 구간으로 나눠 스레드들이 동시에 pwrite =====
//   RC_PERSIST_SPLIT=K (기본 1 = 끔). 구간은 1 MiB 경계로 나눔.
//   각 스레드: 자기 4 KiB 정렬 1 MiB 버퍼에 memcpy -> pwrite 1 MiB (O_DIRECT, 기존 Rdb 경로와 같은 단위)
//   공통: 처음에 fallocate(KEEP_SIZE, 파일 크기) 1번, 끝에 ftruncate(패딩 제거) + fdatasync 1번
static int RcPersistSplitN() {
  static const int v = [] {
    const char* e = std::getenv("RC_PERSIST_SPLIT");
    int x = (e && *e) ? std::atoi(e) : 1;
    if (x < 1) x = 1;
    if (x > 16) x = 16;
    std::fprintf(stderr, "[rc] v-psplit: RC_PERSIST_SPLIT=%d (1=off)\n", x);
    return x;
  }();
  return v;
}
static int RcPersistWriteFileSplit(const std::string& dst, const char* data,
                                   size_t size, int k) {
  const int flags = O_WRONLY | O_CREAT | O_TRUNC;
  int fd = ::open(dst.c_str(), flags | O_DIRECT, 0644);
  const bool direct = fd >= 0;
  if (fd < 0) fd = ::open(dst.c_str(), flags, 0644);
  if (fd < 0) return errno ? errno : EIO;
  if (size > 0) (void)::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, (off_t)size);
  constexpr size_t kAlign = 4096, kPiece = size_t(1) << 20;
  size_t per = (size + (size_t)k - 1) / (size_t)k;
  per = (per + kPiece - 1) / kPiece * kPiece;
  if (per == 0) per = kPiece;
  std::atomic<int> err{0};
  auto work = [&](size_t beg, size_t end) {
    void* buf = nullptr;
    if (posix_memalign(&buf, kAlign, kPiece) != 0) { err.store(ENOMEM); return; }
    for (size_t off = beg; off < end && err.load() == 0; off += kPiece) {
      const size_t n = std::min(kPiece, end - off);
      const size_t wn = direct ? (n + kAlign - 1) / kAlign * kAlign : n;
      std::memcpy(buf, data + off, n);
      if (wn > n) std::memset(static_cast<char*>(buf) + n, 0, wn - n);
      size_t done = 0;
      while (done < wn) {
        ssize_t r = ::pwrite(fd, static_cast<char*>(buf) + done, wn - done,
                             (off_t)(off + done));
        if (r < 0) { if (errno == EINTR) continue; err.store(errno); break; }
        done += (size_t)r;
      }
    }
    std::free(buf);
  };
  std::vector<std::thread> th;
  for (size_t beg = per; beg < size; beg += per)
    th.emplace_back(work, beg, std::min(size, beg + per));
  work(0, std::min(size, per));
  for (auto& t : th) t.join();
  int e = err.load();
  if (e == 0 && direct && (size % kAlign) != 0 && ::ftruncate(fd, (off_t)size) != 0) e = errno;
  if (e == 0 && ::fdatasync(fd) != 0) e = errno;
  if (::close(fd) != 0 && e == 0) e = errno;
  return e;
}
// ===== end v-psplit =====

int RcStreamWriteFileExt(const std::string& dst, const char* data, size_t size) {
  if (RcPersistSplitN() > 1) return RcPersistWriteFileSplit(dst, data, size, RcPersistSplitN());  // v-psplit
  bool used_direct = false;
  return RcPersistRdbOn() ? RcPersistWriteFileRdb(dst, data, size, &used_direct)
                          : RcPersistWriteFile(dst, data, size, &used_direct);
}

namespace ROCKSDB_NAMESPACE {
int RcStreamMode();  // v-stream (compaction_dispatcher.cc)
Status RcStreamFinalize(uint64_t job_id, const CompactionResult& wres,
                        const CompactionResult& fixed, const std::string& db_path);  // v-stream

RemoteCompactionJob::RemoteCompactionJob(DBImpl*               db,
                                          Compaction*           c,
                                          ColumnFamilyData*     cfd,
                                          CompactionDispatcher* dispatcher,
                                          FSDirectory*          db_dir)
    : db_(db), compaction_(c), cfd_(cfd),
      dispatcher_(dispatcher), db_dir_(db_dir) {}

// ── Run (동기 버전) ───────────────────────────────────────────────
Status RemoteCompactionJob::Run(
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber                     ewc_snapshot,
    SequenceNumber                     preserve_seqnum) {

  const ImmutableOptions& iopts      = cfd_->ioptions();
  const MutableCFOptions& mutable_cf = cfd_->GetLatestMutableCFOptions();

  CompactionSpec spec;
  Status s = dispatcher_->BuildSpec(compaction_, cfd_, iopts, mutable_cf,
                                    snapshots, ewc_snapshot, preserve_seqnum,
                                    &spec);
  if (!s.ok()) return s;

  CompactionResult               result;
  std::vector<std::vector<char>> out_bufs;

  s = dispatcher_->Dispatch(compaction_, cfd_, iopts, mutable_cf,
                             snapshots, ewc_snapshot, preserve_seqnum,
                             &result, &out_bufs);
  if (!s.ok()) return s;

  return FinishJob(spec, result, out_bufs);
}

// ── RunAsync (비동기 버전) ────────────────────────────────────────
Status RemoteCompactionJob::RunAsync(
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber                     ewc_snapshot,
    SequenceNumber                     preserve_seqnum,
    std::shared_ptr<Compaction>        compaction_holder,
    std::function<void(Status)>        release_cb) {

  const ImmutableOptions& iopts      = cfd_->ioptions();
  const MutableCFOptions& mutable_cf = cfd_->GetLatestMutableCFOptions();

  CompactionSpec spec;
  Status s = dispatcher_->BuildSpec(compaction_, cfd_, iopts, mutable_cf,
                                    snapshots, ewc_snapshot, preserve_seqnum,
                                    &spec);
  if (!s.ok()) return s;

  std::shared_ptr<std::list<uint64_t>::iterator> pout_keep;  // v-pout
  {
    InstrumentedMutexLock l(db_->mutex());
    // v15: worker 임시 fnum은 FinishJob에서 재발급하므로 예약 불필요
    // 0으로 설정하면 worker가 10000번부터 자체 카운터 사용
    spec.first_output_file_number = 0;

    auto new_pout_iter = std::make_shared<std::list<uint64_t>::iterator>(
        db_->CaptureCurrentFileNumberInPendingOutputsForRemote());
    pout_keep = new_pout_iter;  // v-pout
    auto orig_release_cb = std::move(release_cb);
    release_cb = [this_db = db_, new_pout_iter,
                  orig_cb = std::move(orig_release_cb)](Status s) mutable {
      {  // v-pout: pending_outputs_는 DB mutex 보호 대상. 이 콜백은 디스패처 스레드에서
         // mutex 없이 불린다. orig_cb는 스스로 mutex를 잡으므로 락 밖에서 부른다.
        InstrumentedMutexLock l(this_db->mutex());
        auto v = std::make_unique<std::list<uint64_t>::iterator>(*new_pout_iter);
        this_db->ReleaseFileNumberFromPendingOutputsForRemote(std::move(v));
      }
      if (orig_cb) orig_cb(s);
    };
  }

  auto self = shared_from_this();
  // v36: job 전체(디스패치→워커 왕복→install) 총 소요시간 계측.
  auto t_total_start = std::make_shared<std::chrono::steady_clock::time_point>(
      std::chrono::steady_clock::now());

  // v16: release_cb를 shared_ptr로 감싸서 아래 두 경로 모두에서
  // "정확히 한 번" 호출될 수 있게 함.
  //   (a) DispatchAsync가 큐에 넣는 데 성공 → on_complete 콜백 안에서 호출
  //   (b) DispatchAsync가 즉시 실패(예: TryReserve 실패로 Busy) →
  //       on_complete은 한 번도 안 불리므로, 아래에서 직접 호출
  auto release_cb_shared =
      std::make_shared<std::function<void(Status)>>(std::move(release_cb));

  s = dispatcher_->DispatchAsync(
      std::move(spec),
      std::move(compaction_holder),
      [self, release_cb_shared, t_total_start](
          Status st,
          CompactionSpec&& spec_back,
          CompactionResult&& result,
          std::vector<std::vector<char>>&& out_bufs) mutable {
        if (!st.ok()) {
          fprintf(stderr, "[rc] async job failed: %s\n",
                  st.ToString().c_str());
          (*release_cb_shared)(st);
          return;
        }

        auto rc_f0 = std::chrono::steady_clock::now();  // v-fintime
        Status finish_s = self->FinishJob(spec_back, result, out_bufs);
        auto rc_f1 = std::chrono::steady_clock::now();  // v-fintime

        // v36: job total = DispatchAsync 호출 시점부터 install까지 전체.
        auto t_total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - *t_total_start).count();
        fprintf(stderr, "[timing] rc job=%llu total=%lldms\n",
                (unsigned long long)spec_back.job_id, (long long)t_total_ms);

        auto rc_f2 = std::chrono::steady_clock::now();  // v-fintime
        (*release_cb_shared)(finish_s);
        fprintf(stderr, "[timing] rc job=%llu fin_finishjob_ms=%lld fin_release_cb_ms=%lld\n", (unsigned long long)spec_back.job_id,
                (long long)std::chrono::duration_cast<std::chrono::milliseconds>(rc_f1 - rc_f0).count(),
                (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - rc_f2).count());  // v-fintime

        if (!finish_s.ok()) {
          fprintf(stderr, "[rc] async install failed: %s\n",
                  finish_s.ToString().c_str());
        }
      });

  if (!s.ok()) {
    // DispatchAsync가 큐에 넣기도 전에 실패한 경우(예: 같은 입력 SST가
    // 이미 다른 job에 예약돼 있어 TryReserve 실패) → 위의 on_complete
    // 콜백은 절대 호출되지 않으므로 여기서 직접 release해서
    // pending-outputs capture가 새지 않도록 함.
    fprintf(stderr, "[rc] RunAsync: DispatchAsync failed before enqueue: %s\n",
            s.ToString().c_str());
    // v-pout: 이 경우 호출자가 같은 compaction을 로컬로 fallback한다(c_own=false).
    // release_cb 전체를 부르면 orig_cb가 ReleaseCompactionFiles + on_async_done까지
    // 실행해서, 로컬로 돌 compaction의 입력 표시와 pending_outputs 보호가 먼저 풀린다.
    // 여기서는 RunAsync가 잡은 번호만 푼다.
    if (pout_keep) {
      InstrumentedMutexLock l(db_->mutex());
      auto v = std::make_unique<std::list<uint64_t>::iterator>(*pout_keep);
      db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(v));
    }
  }

  return s;
}

// ── FinishJob ─────────────────────────────────────────────────────
Status RemoteCompactionJob::FinishJob(
    const CompactionSpec&                 spec,
    const CompactionResult&               result,
    const std::vector<std::vector<char>>& out_bufs) {

  const ImmutableOptions& iopts = cfd_->ioptions();

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);

  const std::string& db_path = iopts.cf_paths[0].path;

  
  CompactionResult result_fixed = result;
  for (size_t i = 0; i < result_fixed.output_files.size() && i < out_bufs.size(); i++) {
    if (out_bufs[i].empty() && RcStreamMode()) continue;  // v-ringfix: ring path leaves out_bufs empty; keep worker size (preopen checks it against the file on disk)
    uint64_t actual = static_cast<uint64_t>(out_bufs[i].size());
    if (result_fixed.output_files[i].file_size != actual) {
      result_fixed.output_files[i].file_size = actual;
    }
  }

  // v14: fnum 재발급 (mutex 안)
  std::unique_ptr<std::list<uint64_t>::iterator> finish_pout_iter;
  {
    auto rc_fm0 = std::chrono::steady_clock::now();  // v-fintime
    InstrumentedMutexLock l(db_->mutex());
    fprintf(stderr, "[timing] rc job=%llu step4m_fnum_mutex_wait=%lldms\n", (unsigned long long)spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - rc_fm0).count());  // v-fintime
    for (size_t i = 0; i < result_fixed.output_files.size(); i++) {
      uint64_t new_fnum = db_->GetVersionSet()->NewFileNumber();
      result_fixed.output_files[i].file_number = new_fnum;
    }
    // v15: 새로 발급한 fnum들을 pending_outputs에 등록해서 purge 블로킹 방지
    finish_pout_iter = std::make_unique<std::list<uint64_t>::iterator>(
        db_->CaptureCurrentFileNumberInPendingOutputsForRemote());
  }

  // persist는 mutex 밖에서 (시간이 걸리므로)
  // v36: step4a_persist 계측 복원.
  auto t_persist_start = std::chrono::steady_clock::now();
  Status s = RcStreamMode() ? RcStreamFinalize(spec.job_id, result, result_fixed, db_path)  // v-stream: 이미 쓴 임시 파일 rename
                           : PersistOutputSSTs(result_fixed, out_bufs, db_path);
  auto t_pfiles_done = std::chrono::steady_clock::now();  // v-pdirect
  if (s.ok()) {  // v-fsync: 새 파일 이름(디렉터리 엔트리) 영속화 = SyncOutputDirectories
    int dfd = ::open(db_path.c_str(), O_RDONLY | O_DIRECTORY);
    int rc = (dfd >= 0) ? ::fsync(dfd) : -1;
    int err = errno;
    if (dfd >= 0) ::close(dfd);
    if (rc != 0) {
      // 이미 쓴 출력 파일은 MANIFEST에 없으므로 다음 full scan(FindObsoleteFiles)에서 지워진다
      s = Status::IOError("PersistOutputSSTs: dir fsync failed for " + db_path +
                          ": " + strerror(err));
    }
  }
  {  // v-pdirect: persist 분해 계측
    auto t_pdir_done = std::chrono::steady_clock::now();
    unsigned long long pbytes = 0;
    for (const auto& b : out_bufs) pbytes += b.size();
    fprintf(stderr,
            "[timing] rc persist_split files=%zu bytes=%llu write_sync_ms=%lld dirsync_ms=%lld direct=%d\n",
            out_bufs.size(), pbytes,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t_pfiles_done - t_persist_start).count(),
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t_pdir_done - t_pfiles_done).count(),
            RcPersistDirectOn() ? 1 : 0);
  }
  if (!s.ok()) {
    {  // v-pout: 실패 경로에서도 번호를 푼다 (안 풀면 그 이후 SST가 안 지워짐)
      InstrumentedMutexLock l(db_->mutex());
      if (finish_pout_iter) db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));
    }
    dispatcher_->ReleaseReservation(fnums);
    return s;
  }
  fprintf(stderr, "[timing] rc job=%llu step4a_persist=%lldms\n",
          (unsigned long long)spec.job_id,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t_persist_start).count());
  if (s.ok()) {  // v-preopen: mutex 밖에서 출력 파일을 테이블 캐시에 미리 연다
    auto t_pre = std::chrono::steady_clock::now();
    const MutableCFOptions& pre_mcf = cfd_->GetLatestMutableCFOptions();
    ReadOptions pre_ro;
    size_t pre_ok = 0, pre_tail = 0;
    for (const auto& of : result_fixed.output_files) {
      FileMetaData fm;
      fm.fd = FileDescriptor(of.file_number, 0, of.file_size, of.smallest_seqno, of.largest_seqno);
      fm.smallest.DecodeFrom(of.smallest_ikey);
      fm.largest.DecodeFrom(of.largest_ikey);
      std::shared_ptr<const TableProperties> tp;
      Status ps = cfd_->table_cache()->GetTableProperties(*cfd_->soptions(), pre_ro, cfd_->internal_comparator(),
                                                          fm, &tp, pre_mcf);
      if (!ps.ok() || !tp) {
        fprintf(stderr, "[rc] v-preopen: open failed job=%llu file=%llu: %s\n",
                (unsigned long long)spec.job_id, (unsigned long long)of.file_number, ps.ToString().c_str());
        continue;
      }
      pre_ok++;
      if (tp->tail_start_offset > 0 && tp->tail_start_offset < of.file_size) {
        RcPutTailSize(of.file_number, of.file_size - tp->tail_start_offset);
        pre_tail++;
      }
    }
    fprintf(stderr, "[timing] rc job=%llu step4p_preopen=%lldms files=%zu ok=%zu tail=%zu\n",
            (unsigned long long)spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t_pre).count(),
            result_fixed.output_files.size(), pre_ok, pre_tail);
  }

  VersionEdit edit;
  s = BuildVersionEdit(result_fixed, spec, &edit);
  if (!s.ok()) {
    // input L3 파일 set (DeleteFile 대상 → 겹쳐도 정상)
    std::unordered_set<uint64_t> input_fnums;
    for (const auto& f : spec.inputs) input_fnums.insert(f.file_number);

    for (const auto& of : result_fixed.output_files) {
      char fname[32];
      snprintf(fname, sizeof(fname), "%06llu.sst",
               (unsigned long long)of.file_number);
      ::remove((db_path + "/" + fname).c_str());
    }
    {  // v-pout
      InstrumentedMutexLock l(db_->mutex());
      if (finish_pout_iter) db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));
    }
    dispatcher_->ReleaseReservation(fnums);
    return s;
  }

  // v15: mutex 안에서 key range 충돌 검사 후 LogAndApply
  // v36: step4b_mutex_wait / step4c_logandapply 계측 복원.
  static std::atomic<int> g_concurrent_installers{0};
  int concurrent_now = ++g_concurrent_installers;
  fprintf(stderr, "[timing] rc job=%llu finish_concurrent=%d\n", (unsigned long long)spec.job_id, concurrent_now);
  auto t_lock_before = std::chrono::steady_clock::now();
  {
    InstrumentedMutexLock l(db_->mutex());
    fprintf(stderr, "[timing] rc job=%llu step4b_mutex_wait=%lldms\n",
            (unsigned long long)spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t_lock_before).count());

    // v15: output key range가 현재 L3와 겹치는지 검사
    // persist 후 LogAndApply 전 사이에 다른 compaction이 L3를 바꿀 수 있음.
    // 겹치면 실패 반환 → RocksDB가 재스케줄
    auto* vstorage = cfd_->current()->storage_info();
    const Comparator* ucmp = cfd_->user_comparator();

    // input L3 파일 set (DeleteFile 대상 → 겹쳐도 정상)
    std::unordered_set<uint64_t> input_fnums;
    for (const auto& f : spec.inputs) input_fnums.insert(f.file_number);

    if (spec.output_level != 0) {
    for (const auto& of : result_fixed.output_files) {
      InternalKey out_smallest, out_largest;
      out_smallest.DecodeFrom(of.smallest_ikey);
      out_largest.DecodeFrom(of.largest_ikey);
      Slice out_small_user = out_smallest.user_key();
      Slice out_large_user = out_largest.user_key();


    for (const auto* fmd : vstorage->LevelFiles(spec.output_level)) {
      if (input_fnums.count(fmd->fd.GetNumber())) continue;
        Slice exist_small = fmd->smallest.user_key();
        Slice exist_large = fmd->largest.user_key();

        // 겹침 조건: out_small <= exist_large && exist_small <= out_large
        if (ucmp->Compare(out_small_user, exist_large) <= 0 &&
            ucmp->Compare(exist_small, out_large_user) <= 0) {
          fprintf(stderr,
                  "[rc] FinishJob: key range overlap detected: "
                  "output fnum=%llu vs existing fnum=%llu in L%d — aborting\n",
                  (unsigned long long)of.file_number,
                  (unsigned long long)fmd->fd.GetNumber(),
                  spec.output_level);
          // persist한 파일 삭제
          for (const auto& del : result_fixed.output_files) {
            char fname[32];
            snprintf(fname, sizeof(fname), "%06llu.sst",
                     (unsigned long long)del.file_number);
            ::remove((db_path + "/" + fname).c_str());
          }
          if (finish_pout_iter) db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));  // v-pout (mutex 안)
          --g_concurrent_installers;  // v-pout
          dispatcher_->ReleaseReservation(fnums);
          return Status::Corruption(
              "remote compaction output overlaps existing L" +
              std::to_string(spec.output_level) + " file");
        }
      }
    }
    }

    auto t_lna_start = std::chrono::steady_clock::now();
    s = InstallUnderLock(&edit, spec.job_id);
    if (finish_pout_iter) db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));
    fprintf(stderr, "[timing] rc job=%llu step4c_logandapply=%lldms level=%d\n",
            (unsigned long long)spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t_lna_start).count(),
            spec.output_level);
  }

  --g_concurrent_installers;
  fprintf(stderr, "[timing] rc job=%llu step4_persist_install=%lldms\n",
          (unsigned long long)spec.job_id,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t_persist_start).count());

  if (!s.ok()) {
    // input L3 파일 set (DeleteFile 대상 → 겹쳐도 정상)
    std::unordered_set<uint64_t> input_fnums;
    for (const auto& f : spec.inputs) input_fnums.insert(f.file_number);

    for (const auto& of : result_fixed.output_files) {
      char fname[32];
      snprintf(fname, sizeof(fname), "%06llu.sst",
               (unsigned long long)of.file_number);
      ::remove((db_path + "/" + fname).c_str());
    }
  }

  dispatcher_->ReleaseReservation(fnums);
  return s;
}

// ── PersistOutputSSTs ─────────────────────────────────────────────
Status RemoteCompactionJob::PersistOutputSSTs(
    const CompactionResult&               result,
    const std::vector<std::vector<char>>& bufs,
    const std::string&                    db_path) {

  const size_t n = result.output_files.size();
  if (n == 0) return Status::OK();

  std::vector<Status>      statuses(n, Status::OK());
  std::vector<std::string> paths(n);
  std::vector<std::thread> writers(n);

  for (size_t i = 0; i < n; i++) {
    const auto& of = result.output_files[i];
    char fname[32];
    snprintf(fname, sizeof(fname), "%06llu.sst",
             (unsigned long long)of.file_number);
    paths[i] = db_path + "/" + fname;
  }

  // v-pser: RC_PERSIST_SERIAL=1 -> 출력 파일을 한 스레드에서 차례로 쓰기 (파일별 병렬 persist 끔)
  static const bool kPersistSerial = [] { const char* e = getenv("RC_PERSIST_SERIAL"); bool v = (e != nullptr && atoi(e) != 0);
    fprintf(stderr, "[rc] persist: %s (v-pser)\n", v ? "serial" : "parallel per file"); return v; }();
  auto persist_one = [&](size_t i) {
      const auto& buf = bufs[i];
      const auto& dst = paths[i];

      {  // v-pdirect: 로컬 컴팩션처럼 O_DIRECT 쓰기 + fdatasync (WriterSyncClose -> Sync = fdatasync)
        bool used_direct = false;
        int err = RcPersistRdbOn()  // v-prdb
            ? RcPersistWriteFileRdb(dst, buf.data(), buf.size(), &used_direct)
            : RcPersistWriteFile(dst, buf.data(), buf.size(), &used_direct);
        if (err != 0) {
          ::remove(dst.c_str());
          statuses[i] = Status::IOError("PersistOutputSSTs: write failed for " + dst +
                                        ": " + strerror(err));
          return;
        }
      }
  };
  if (kPersistSerial) {
    for (size_t i = 0; i < n; i++) persist_one(i);
  } else {
    for (size_t i = 0; i < n; i++) writers[i] = std::thread([&, i]() { persist_one(i); });
    for (auto& t : writers) t.join();
  }

  for (size_t i = 0; i < n; i++) {
    if (!statuses[i].ok()) {
      for (size_t j = 0; j < n; j++) {
        if (statuses[j].ok()) ::remove(paths[j].c_str());
      }
      return statuses[i];
    }
  }
  return Status::OK();
}

// ── BuildVersionEdit ──────────────────────────────────────────────
Status RemoteCompactionJob::BuildVersionEdit(
    const CompactionResult& result,
    const CompactionSpec&   spec,
    VersionEdit*            edit_out) {

  edit_out->SetColumnFamily(cfd_->GetID());

  for (const auto& f : spec.inputs) {
    edit_out->DeleteFile(f.level, f.file_number);
  }

  // v-preopen: 로컬 CompactionJob과 같은 규칙 (입력 최솟값, 없으면 현재 시각)
  int64_t rc_now_i = 0;
  if (!cfd_->ioptions().clock->GetCurrentTime(&rc_now_i).ok()) rc_now_i = 0;
  const uint64_t rc_now = static_cast<uint64_t>(rc_now_i);
  uint64_t rc_oat = compaction_ ? compaction_->MinInputFileOldestAncesterTime(nullptr, nullptr)
                                : std::numeric_limits<uint64_t>::max();
  if (rc_oat == std::numeric_limits<uint64_t>::max() || rc_oat == 0) rc_oat = rc_now;
  for (const auto& of : result.output_files) {
    InternalKey smallest, largest;
    smallest.DecodeFrom(of.smallest_ikey);
    largest.DecodeFrom(of.largest_ikey);

    edit_out->AddFile(
        spec.output_level,
        of.file_number,
        /*file_path_id=*/0,
        of.file_size,
        smallest,
        largest,
        of.smallest_seqno,
        of.largest_seqno,
        /*marked_for_compaction=*/false,
        Temperature::kUnknown,
        /*oldest_blob_file_number=*/kInvalidBlobFileNumber,
        /*oldest_ancester_time=*/rc_oat,  // v-preopen
        /*file_creation_time=*/rc_now,  // v-preopen
        /*epoch_number=*/(spec.output_level == 0)
            ? cfd_->NewEpochNumber()
            : (uint64_t)1,
        /*file_checksum=*/"",
        /*file_checksum_func_name=*/"",
        kNullUniqueId64x2,
        /*compensated_range_deletion_size=*/(uint64_t)0,
        /*tail_size=*/RcTakeTailSize(of.file_number),  // v-preopen
        /*user_defined_timestamps_persisted=*/true);
  }
  return Status::OK();
}

// ── InstallUnderLock ──────────────────────────────────────────────
Status RemoteCompactionJob::InstallUnderLock(VersionEdit* edit, uint64_t job_id) {
  const ReadOptions  read_opts(Env::IOActivity::kCompaction);
  const WriteOptions write_opts(Env::IOActivity::kCompaction);

  autovector<VersionEdit*> edit_list;
  LogCmpOut("remote", compaction_, edit);  // v-cmpout
  edit_list.emplace_back(edit);
  // gyu modify: v48 job_id로 리모트 큐 9개 중 하나 라운드로빈 선택
  {  // v-serialmf: RC_SERIAL_MANIFEST=1 -> 기본 MANIFEST 큐 하나로 (병렬 커밋 끔)
    static const bool kSerialMf = [] {
      const char* e = getenv("RC_SERIAL_MANIFEST");
      bool v = (e != nullptr && atoi(e) != 0);
      fprintf(stderr, "[rc] manifest commit: %s\n", v ? "serial (single queue)" : "parallel (remote queues)");
      return v;
    }();
    (void)kSerialMf;  // v-serialonly: 병렬 MANIFEST 큐 안 씀, 항상 기본 MANIFEST 하나로 직렬
  }

  Status s = db_->GetVersionSet()->LogAndApply(
      cfd_,
      read_opts,
      write_opts,
      edit_list,
      db_->mutex(),
      db_dir_);

  if (!s.ok())
    return Status::IOError("LogAndApply failed: " + s.ToString());

  auto t_obsolete_start = std::chrono::steady_clock::now();
  static std::atomic<int> g_obsolete_job_id{0};
  JobContext job_context(g_obsolete_job_id.fetch_add(1), true);
  db_->FindObsoleteFiles(&job_context, false);
  bool had_something = job_context.HaveSomethingToClean() ||
                        job_context.HaveSomethingToDelete();
  if (had_something) {
    db_->mutex()->Unlock();
    if (job_context.HaveSomethingToDelete()) {
      db_->PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
    db_->mutex()->Lock();
  }
  fprintf(stderr, "[timing] rc job=%llu step4d_obsolete_purge=%lldms had_work=%d\n",
          (unsigned long long)job_id,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t_obsolete_start).count(),
          had_something ? 1 : 0);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
