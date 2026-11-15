// ============================================================
//
//
//
//
//      RC_CALLBACK_BEGIN, "async compaction installed successfully";
//
// ============================================================
#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "remote_compaction_job.h"
#include <chrono>
#include <limits>
#include <mutex>
#include <unordered_map>
#include "db/column_family.h"
#include "db/table_cache.h"
#include "rocksdb/table_properties.h"
namespace {
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
#include "db/compaction/cmpout_log.h"

#include <chrono>
#include <thread>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
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
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/types.h>

static bool RcPersistDirectOn() {
  static const bool on = [] {
    const char* e = getenv("RC_PERSIST_DIRECT");
    bool v = !(e && e[0] == '0');
    fprintf(stderr, "[rc] output persist: direct=%d (v-pdirect)\n", v ? 1 : 0);
    return v;
  }();
  return on;
}

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
  RcDeferSyncer::Get().Push(fd);
  return 0;
}
static int RcPersistWriteFile(const std::string& dst, const char* data, size_t size,
                              bool* used_direct) {
  if (RcPersistDeferOn()) return RcPersistWriteFileDefer(dst, data, size, used_direct);
  const size_t kAlign = 4096;
  const size_t kPiece = static_cast<size_t>(4) << 20;
  *used_direct = false;
  int fd = -1;
  if (RcPersistDirectOn()) {
    fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_CLOEXEC, 0644);
    if (fd >= 0) {
      *used_direct = true;
    } else if (errno != EINVAL) {
      return errno;
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
      size_t wn = (n + kAlign - 1) / kAlign * kAlign;
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
// FileSystem::NewWritableFile(use_direct_writes) + WritableFileWriter:
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
  if (RcPersistDeferOn()) return RcPersistWriteFileDefer(dst, data, size, used_direct);
  using namespace ROCKSDB_NAMESPACE;
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


int RcStreamWriteFileExt(const std::string& dst, const char* data, size_t size) {
  bool used_direct = false;
  return RcPersistRdbOn() ? RcPersistWriteFileRdb(dst, data, size, &used_direct)
                          : RcPersistWriteFile(dst, data, size, &used_direct);
}

namespace ROCKSDB_NAMESPACE {
int RcStreamMode();
Status RcStreamFinalize(uint64_t job_id, const CompactionResult& wres,
                        const CompactionResult& fixed, const std::string& db_path);

RemoteCompactionJob::RemoteCompactionJob(DBImpl*               db,
                                          Compaction*           c,
                                          ColumnFamilyData*     cfd,
                                          CompactionDispatcher* dispatcher,
                                          FSDirectory*          db_dir)
    : db_(db), compaction_(c), cfd_(cfd),
      dispatcher_(dispatcher), db_dir_(db_dir) {}

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

  std::shared_ptr<std::list<uint64_t>::iterator> pout_keep;
  {
    InstrumentedMutexLock l(db_->mutex());
    spec.first_output_file_number = 0;

    auto new_pout_iter = std::make_shared<std::list<uint64_t>::iterator>(
        db_->CaptureCurrentFileNumberInPendingOutputsForRemote());
    pout_keep = new_pout_iter;
    auto orig_release_cb = std::move(release_cb);
    release_cb = [this_db = db_, new_pout_iter,
                  orig_cb = std::move(orig_release_cb)](Status s) mutable {
      {
        InstrumentedMutexLock l(this_db->mutex());
        auto v = std::make_unique<std::list<uint64_t>::iterator>(*new_pout_iter);
        this_db->ReleaseFileNumberFromPendingOutputsForRemote(std::move(v));
      }
      if (orig_cb) orig_cb(s);
    };
  }

  auto self = shared_from_this();
  auto t_total_start = std::make_shared<std::chrono::steady_clock::time_point>(
      std::chrono::steady_clock::now());

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

        Status finish_s = self->FinishJob(spec_back, result, out_bufs);

        auto t_total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - *t_total_start).count();
        fprintf(stderr, "[timing] rc job=%llu total=%lldms\n",
                (unsigned long long)spec_back.job_id, (long long)t_total_ms);

        (*release_cb_shared)(finish_s);

        if (!finish_s.ok()) {
          fprintf(stderr, "[rc] async install failed: %s\n",
                  finish_s.ToString().c_str());
        }
      });

  if (!s.ok()) {
    fprintf(stderr, "[rc] RunAsync: DispatchAsync failed before enqueue: %s\n",
            s.ToString().c_str());
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
    uint64_t actual = static_cast<uint64_t>(out_bufs[i].size());
    if (result_fixed.output_files[i].file_size != actual) {
      result_fixed.output_files[i].file_size = actual;
    }
  }

  std::unique_ptr<std::list<uint64_t>::iterator> finish_pout_iter;
  {
    InstrumentedMutexLock l(db_->mutex());
    for (size_t i = 0; i < result_fixed.output_files.size(); i++) {
      uint64_t new_fnum = db_->GetVersionSet()->NewFileNumber();
      result_fixed.output_files[i].file_number = new_fnum;
    }
    finish_pout_iter = std::make_unique<std::list<uint64_t>::iterator>(
        db_->CaptureCurrentFileNumberInPendingOutputsForRemote());
  }

  auto t_persist_start = std::chrono::steady_clock::now();
  Status s = RcStreamMode() ? RcStreamFinalize(spec.job_id, result, result_fixed, db_path)
                           : PersistOutputSSTs(result_fixed, out_bufs, db_path);
  auto t_pfiles_done = std::chrono::steady_clock::now();
  if (s.ok()) {
    int dfd = ::open(db_path.c_str(), O_RDONLY | O_DIRECTORY);
    int rc = (dfd >= 0) ? ::fsync(dfd) : -1;
    int err = errno;
    if (dfd >= 0) ::close(dfd);
    if (rc != 0) {
      s = Status::IOError("PersistOutputSSTs: dir fsync failed for " + db_path +
                          ": " + strerror(err));
    }
  }
  {
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
    {
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
  if (s.ok()) {
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
    std::unordered_set<uint64_t> input_fnums;
    for (const auto& f : spec.inputs) input_fnums.insert(f.file_number);

    for (const auto& of : result_fixed.output_files) {
      char fname[32];
      snprintf(fname, sizeof(fname), "%06llu.sst",
               (unsigned long long)of.file_number);
      ::remove((db_path + "/" + fname).c_str());
    }
    {
      InstrumentedMutexLock l(db_->mutex());
      if (finish_pout_iter) db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));
    }
    dispatcher_->ReleaseReservation(fnums);
    return s;
  }

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

    auto* vstorage = cfd_->current()->storage_info();
    const Comparator* ucmp = cfd_->user_comparator();

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

        if (ucmp->Compare(out_small_user, exist_large) <= 0 &&
            ucmp->Compare(exist_small, out_large_user) <= 0) {
          fprintf(stderr,
                  "[rc] FinishJob: key range overlap detected: "
                  "output fnum=%llu vs existing fnum=%llu in L%d — aborting\n",
                  (unsigned long long)of.file_number,
                  (unsigned long long)fmd->fd.GetNumber(),
                  spec.output_level);
          for (const auto& del : result_fixed.output_files) {
            char fname[32];
            snprintf(fname, sizeof(fname), "%06llu.sst",
                     (unsigned long long)del.file_number);
            ::remove((db_path + "/" + fname).c_str());
          }
          if (finish_pout_iter) db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));
          --g_concurrent_installers;
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

  static const bool kPersistSerial = [] { const char* e = getenv("RC_PERSIST_SERIAL"); bool v = (e != nullptr && atoi(e) != 0);
    fprintf(stderr, "[rc] persist: %s (v-pser)\n", v ? "serial" : "parallel per file"); return v; }();
  auto persist_one = [&](size_t i) {
      const auto& buf = bufs[i];
      const auto& dst = paths[i];

      {
        bool used_direct = false;
        int err = RcPersistRdbOn()
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
        /*oldest_ancester_time=*/rc_oat,
        /*file_creation_time=*/rc_now,
        /*epoch_number=*/(spec.output_level == 0)
            ? cfd_->NewEpochNumber()
            : (uint64_t)1,
        /*file_checksum=*/"",
        /*file_checksum_func_name=*/"",
        kNullUniqueId64x2,
        /*compensated_range_deletion_size=*/(uint64_t)0,
        /*tail_size=*/RcTakeTailSize(of.file_number),
        /*user_defined_timestamps_persisted=*/true);
  }
  return Status::OK();
}

// ── InstallUnderLock ──────────────────────────────────────────────
Status RemoteCompactionJob::InstallUnderLock(VersionEdit* edit, uint64_t job_id) {
  const ReadOptions  read_opts(Env::IOActivity::kCompaction);
  const WriteOptions write_opts(Env::IOActivity::kCompaction);

  autovector<VersionEdit*> edit_list;
  LogCmpOut("remote", compaction_, edit);
  edit_list.emplace_back(edit);
  {
    static const bool kSerialMf = [] {
      const char* e = getenv("RC_SERIAL_MANIFEST");
      bool v = (e != nullptr && atoi(e) != 0);
      fprintf(stderr, "[rc] manifest commit: %s\n", v ? "serial (single queue)" : "parallel (remote queues)");
      return v;
    }();
    if (!kSerialMf) edit->MarkDeferManifestSync(static_cast<int>(job_id % 9));
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
