#pragma once
#include "rocksdb/file_system.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class ManifestRedirectFS : public FileSystemWrapper {
 public:
  ManifestRedirectFS(std::shared_ptr<FileSystem> target,
                      std::string redirect_dir)
      : FileSystemWrapper(std::move(target)),
        redirect_dir_(std::move(redirect_dir)) {}

  const char* Name() const override { return "ManifestRedirectFS"; }

  static bool IsManifestFile(const std::string& fname) {
    size_t slash = fname.find_last_of('/');
    std::string base = (slash == std::string::npos) ? fname : fname.substr(slash + 1);
    return base.rfind("MANIFEST-", 0) == 0 || base == "CURRENT";
  }

  std::string Redirect(const std::string& fname) const {
    if (!IsManifestFile(fname)) return fname;
    size_t slash = fname.find_last_of('/');
    std::string base = (slash == std::string::npos) ? fname : fname.substr(slash + 1);
    return redirect_dir_ + "/" + base;
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                            std::unique_ptr<FSWritableFile>* r,
                            IODebugContext* dbg) override {
    return target()->NewWritableFile(Redirect(fname), opts, r, dbg);
  }
  IOStatus ReopenWritableFile(const std::string& fname, const FileOptions& opts,
                               std::unique_ptr<FSWritableFile>* r,
                               IODebugContext* dbg) override {
    return target()->ReopenWritableFile(Redirect(fname), opts, r, dbg);
  }
  IOStatus NewRandomAccessFile(const std::string& fname, const FileOptions& opts,
                                std::unique_ptr<FSRandomAccessFile>* r,
                                IODebugContext* dbg) override {
    return target()->NewRandomAccessFile(Redirect(fname), opts, r, dbg);
  }
  IOStatus NewSequentialFile(const std::string& fname, const FileOptions& opts,
                              std::unique_ptr<FSSequentialFile>* r,
                              IODebugContext* dbg) override {
    return target()->NewSequentialFile(Redirect(fname), opts, r, dbg);
  }
  IOStatus FileExists(const std::string& fname, const IOOptions& io_opts,
                       IODebugContext* dbg) override {
    return target()->FileExists(Redirect(fname), io_opts, dbg);
  }
  IOStatus GetFileSize(const std::string& fname, const IOOptions& io_opts,
                        uint64_t* size, IODebugContext* dbg) override {
    return target()->GetFileSize(Redirect(fname), io_opts, size, dbg);
  }
  IOStatus DeleteFile(const std::string& fname, const IOOptions& io_opts,
                       IODebugContext* dbg) override {
    return target()->DeleteFile(Redirect(fname), io_opts, dbg);
  }
  IOStatus RenameFile(const std::string& s, const std::string& t,
                       const IOOptions& io_opts, IODebugContext* dbg) override {
    std::string new_t = Redirect(t);
    if (new_t == t) {
      return target()->RenameFile(s, t, io_opts, dbg);
    }
    // Destination redirected to tmpfs but source (a temp file like
    // NNNNNN.dbtmp) still lives on the original dbname filesystem.
    // rename(2) cannot cross filesystems (EXDEV), so copy the (tiny)
    // content over instead of renaming.
    std::unique_ptr<FSSequentialFile> src_file;
    IOStatus rs = target()->NewSequentialFile(s, FileOptions(), &src_file, dbg);
    if (!rs.ok()) return rs;

    std::string data;
    char buf[4096];
    for (;;) {
      Slice result;
      IOStatus read_s = src_file->Read(sizeof(buf), io_opts, &result, buf, dbg);
      if (!read_s.ok()) return read_s;
      if (result.empty()) break;
      data.append(result.data(), result.size());
    }
    src_file.reset();

    std::unique_ptr<FSWritableFile> dst_file;
    IOStatus ws = target()->NewWritableFile(new_t, FileOptions(), &dst_file, dbg);
    if (!ws.ok()) return ws;
    ws = dst_file->Append(Slice(data), io_opts, dbg);
    if (!ws.ok()) return ws;
    ws = dst_file->Sync(io_opts, dbg);
    if (!ws.ok()) return ws;
    ws = dst_file->Close(io_opts, dbg);
    if (!ws.ok()) return ws;

    return target()->DeleteFile(s, io_opts, dbg);
  }

  FileOptions OptimizeForManifestWrite(const FileOptions& opts) const override {
    FileOptions o = target()->OptimizeForManifestWrite(opts);
    o.use_mmap_writes = true;
    o.use_direct_writes = false;
    return o;
  }
  FileOptions OptimizeForManifestRead(const FileOptions& opts) const override {
    FileOptions o = target()->OptimizeForManifestRead(opts);
    o.use_mmap_reads = true;
    o.use_direct_reads = false;
    return o;
  }

 private:
  std::string redirect_dir_;
};

}  // namespace ROCKSDB_NAMESPACE
