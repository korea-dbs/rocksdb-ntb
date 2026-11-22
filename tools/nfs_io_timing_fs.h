// TimingFS: FileSystem wrapper used by nfs_compaction_worker to measure, per
// thread, the time and bytes of SST reads and writes and the time of
// metadata operations (stat, list, create/rename/delete). The counters feed
// the [RCMP_LATENCY] line of each job.
#pragma once
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {
namespace nfsio {

struct IoAcc {
  int64_t sst_read_ns = 0, sst_read_bytes = 0, sst_read_ops = 0;
  int64_t sst_write_ns = 0, sst_write_bytes = 0, sst_write_ops = 0;
  int64_t sst_sync_ns = 0;
  int64_t other_ns = 0, other_bytes = 0;
  // Metadata operations: stat-like, directory listing, and directory
  // changes (create, rename, delete, new directory/logger).
  int64_t m_stat_ns = 0, m_stat_n = 0, m_list_ns = 0, m_list_n = 0,
          m_dir_ns = 0, m_dir_n = 0;
};

inline thread_local IoAcc g_tls;

inline int64_t NowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch()).count();
}
inline bool IsSst(const std::string& f) {
  return f.size() >= 4 && f.compare(f.size() - 4, 4, ".sst") == 0;
}
inline void AddRead(bool sst, int64_t ns, int64_t bytes) {
  if (sst) {
    g_tls.sst_read_ns += ns; g_tls.sst_read_bytes += bytes; g_tls.sst_read_ops += 1;
  } else {
    g_tls.other_ns += ns; g_tls.other_bytes += bytes;
  }
}
inline void AddWrite(bool sst, int64_t ns, int64_t bytes, bool is_sync) {
  if (sst) {
    if (is_sync) { g_tls.sst_sync_ns += ns; }
    else { g_tls.sst_write_ns += ns; g_tls.sst_write_bytes += bytes; g_tls.sst_write_ops += 1; }
  } else {
    g_tls.other_ns += ns; g_tls.other_bytes += bytes;
  }
}

class TSeq : public FSSequentialFileOwnerWrapper {
 public:
  TSeq(std::unique_ptr<FSSequentialFile>&& t, bool sst)
      : FSSequentialFileOwnerWrapper(std::move(t)), sst_(sst) {}
  IOStatus Read(size_t n, const IOOptions& o, Slice* r, char* s,
                IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Read(n, o, r, s, d);
    AddRead(sst_, NowNs() - t0, st.ok() ? (int64_t)r->size() : 0);
    return st;
  }
  IOStatus PositionedRead(uint64_t off, size_t n, const IOOptions& o, Slice* r,
                          char* s, IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->PositionedRead(off, n, o, r, s, d);
    AddRead(sst_, NowNs() - t0, st.ok() ? (int64_t)r->size() : 0);
    return st;
  }
 private:
  bool sst_;
};

class TRand : public FSRandomAccessFileOwnerWrapper {
 public:
  TRand(std::unique_ptr<FSRandomAccessFile>&& t, bool sst)
      : FSRandomAccessFileOwnerWrapper(std::move(t)), sst_(sst) {}
  IOStatus Read(uint64_t off, size_t n, const IOOptions& o, Slice* r, char* s,
                IODebugContext* d) const override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Read(off, n, o, r, s, d);
    AddRead(sst_, NowNs() - t0, st.ok() ? (int64_t)r->size() : 0);
    return st;
  }
  IOStatus MultiRead(FSReadRequest* reqs, size_t num, const IOOptions& o,
                     IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->MultiRead(reqs, num, o, d);
    int64_t bytes = 0;
    for (size_t i = 0; i < num; i++)
      if (reqs[i].status.ok()) bytes += (int64_t)reqs[i].result.size();
    AddRead(sst_, NowNs() - t0, bytes);
    return st;
  }
 private:
  bool sst_;
};

class TWrite : public FSWritableFileOwnerWrapper {
 public:
  TWrite(std::unique_ptr<FSWritableFile>&& t, bool sst)
      : FSWritableFileOwnerWrapper(std::move(t)), sst_(sst) {}
  IOStatus Append(const Slice& data, const IOOptions& o,
                  IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Append(data, o, d);
    AddWrite(sst_, NowNs() - t0, st.ok() ? (int64_t)data.size() : 0, false);
    return st;
  }
  IOStatus Append(const Slice& data, const IOOptions& o,
                  const DataVerificationInfo& v, IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Append(data, o, v, d);
    AddWrite(sst_, NowNs() - t0, st.ok() ? (int64_t)data.size() : 0, false);
    return st;
  }
  // Direct I/O writes go through PositionedAppend.
  IOStatus PositionedAppend(const Slice& data, uint64_t off, const IOOptions& o,
                            IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->PositionedAppend(data, off, o, d);
    AddWrite(sst_, NowNs() - t0, st.ok() ? (int64_t)data.size() : 0, false);
    return st;
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t off, const IOOptions& o,
                            const DataVerificationInfo& v,
                            IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->PositionedAppend(data, off, o, v, d);
    AddWrite(sst_, NowNs() - t0, st.ok() ? (int64_t)data.size() : 0, false);
    return st;
  }
  IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Flush(o, d);
    AddWrite(sst_, NowNs() - t0, 0, true);
    return st;
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Sync(o, d);
    AddWrite(sst_, NowNs() - t0, 0, true);
    return st;
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Fsync(o, d);
    AddWrite(sst_, NowNs() - t0, 0, true);
    return st;
  }
  IOStatus Close(const IOOptions& o, IODebugContext* d) override {
    int64_t t0 = NowNs();
    IOStatus st = target()->Close(o, d);
    AddWrite(sst_, NowNs() - t0, 0, true);
    return st;
  }
 private:
  bool sst_;
};

class TimingFS : public FileSystemWrapper {
 public:
  explicit TimingFS(const std::shared_ptr<FileSystem>& t) : FileSystemWrapper(t) {}
  const char* Name() const override { return "NfsIoTimingFS"; }
  // Metadata calls, timed in the calling thread.
  static int64_t RcNowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
  IOStatus FileExists(const std::string& f, const IOOptions& o, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->FileExists(f, o, d); g_tls.m_stat_ns += RcNowNs() - t; g_tls.m_stat_n++; return s; }
  IOStatus GetFileSize(const std::string& f, const IOOptions& o, uint64_t* sz, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->GetFileSize(f, o, sz, d); g_tls.m_stat_ns += RcNowNs() - t; g_tls.m_stat_n++; return s; }
  IOStatus GetFileModificationTime(const std::string& f, const IOOptions& o, uint64_t* mt, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->GetFileModificationTime(f, o, mt, d); g_tls.m_stat_ns += RcNowNs() - t; g_tls.m_stat_n++; return s; }
  IOStatus IsDirectory(const std::string& p, const IOOptions& o, bool* isd, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->IsDirectory(p, o, isd, d); g_tls.m_stat_ns += RcNowNs() - t; g_tls.m_stat_n++; return s; }
  IOStatus GetChildren(const std::string& dir, const IOOptions& o, std::vector<std::string>* r, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->GetChildren(dir, o, r, d); g_tls.m_list_ns += RcNowNs() - t; g_tls.m_list_n++; return s; }
  IOStatus GetChildrenFileAttributes(const std::string& dir, const IOOptions& o, std::vector<FileAttributes>* r, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->GetChildrenFileAttributes(dir, o, r, d); g_tls.m_list_ns += RcNowNs() - t; g_tls.m_list_n++; return s; }
  IOStatus NewDirectory(const std::string& n, const IOOptions& o, std::unique_ptr<FSDirectory>* r, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->NewDirectory(n, o, r, d); g_tls.m_dir_ns += RcNowNs() - t; g_tls.m_dir_n++; return s; }
  IOStatus CreateDirIfMissing(const std::string& n, const IOOptions& o, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->CreateDirIfMissing(n, o, d); g_tls.m_dir_ns += RcNowNs() - t; g_tls.m_dir_n++; return s; }
  IOStatus RenameFile(const std::string& a, const std::string& b, const IOOptions& o, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->RenameFile(a, b, o, d); g_tls.m_dir_ns += RcNowNs() - t; g_tls.m_dir_n++; return s; }
  IOStatus DeleteFile(const std::string& f, const IOOptions& o, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->DeleteFile(f, o, d); g_tls.m_dir_ns += RcNowNs() - t; g_tls.m_dir_n++; return s; }
  IOStatus NewLogger(const std::string& f, const IOOptions& o, std::shared_ptr<Logger>* r, IODebugContext* d) override {
    int64_t t = RcNowNs(); IOStatus s = target()->NewLogger(f, o, r, d); g_tls.m_dir_ns += RcNowNs() - t; g_tls.m_dir_n++; return s; }
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& o,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* d) override {
    std::unique_ptr<FSSequentialFile> base;
    IOStatus s = target()->NewSequentialFile(f, o, &base, d);
    if (s.ok()) r->reset(new TSeq(std::move(base), IsSst(f)));
    return s;
  }
  IOStatus NewRandomAccessFile(const std::string& f, const FileOptions& o,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* d) override {
    std::unique_ptr<FSRandomAccessFile> base;
    IOStatus s = target()->NewRandomAccessFile(f, o, &base, d);
    if (s.ok()) r->reset(new TRand(std::move(base), IsSst(f)));
    return s;
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* d) override {
    std::unique_ptr<FSWritableFile> base;
    IOStatus s = target()->NewWritableFile(f, o, &base, d);
    if (s.ok()) r->reset(new TWrite(std::move(base), IsSst(f)));
    return s;
  }
};

inline void ResetTls() { g_tls = IoAcc(); }
inline IoAcc SnapshotTls() { return g_tls; }

}  // namespace nfsio
}  // namespace ROCKSDB_NAMESPACE
