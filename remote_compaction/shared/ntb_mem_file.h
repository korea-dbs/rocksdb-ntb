#pragma once
// ============================================================
//  shared/ntb_mem_file.h
//
// ============================================================

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// ─────────────────────────────────────────────────────────────────
//  MemReadableFile
//
// ─────────────────────────────────────────────────────────────────
class MemReadableFile : public FSRandomAccessFile {
public:
  MemReadableFile(const char* data, uint64_t size)
      : data_(data), size_(size) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions&,
                Slice* result, char* scratch,
                IODebugContext*) const override {
    if (offset >= size_) { *result = Slice(); return IOStatus::OK(); }
    if (offset + n > size_) {
      n = static_cast<size_t>(size_ - offset);
    }
    memcpy(scratch, data_ + offset, n);
    *result = Slice(scratch, n);
    return IOStatus::OK();
  }

  IOStatus MultiRead(FSReadRequest* reqs, size_t num,
                     const IOOptions& opts, IODebugContext* dbg) override {
    for (size_t i = 0; i < num; i++)
      reqs[i].status = Read(reqs[i].offset, reqs[i].len, opts,
                            &reqs[i].result, reqs[i].scratch, dbg);
    return IOStatus::OK();
  }

  IOStatus Prefetch(uint64_t, size_t, const IOOptions&,
                    IODebugContext*) override { return IOStatus::OK(); }

  IOStatus GetFileSize(uint64_t* size) override {
    *size = size_;
    return IOStatus::OK();
  }

  bool   use_direct_io() const override { return false; }
  size_t GetRequiredBufferAlignment() const override { return 1; }
  IOStatus InvalidateCache(size_t, size_t) override { return IOStatus::OK(); }

private:
  const char* data_;
  uint64_t    size_;
};

// ─────────────────────────────────────────────────────────────────
//  MemWritableFile
// ─────────────────────────────────────────────────────────────────
class MemWritableFile : public FSWritableFile {
public:
  explicit MemWritableFile(std::vector<char>* buf) : buf_(buf) {}

  IOStatus Append(const Slice& data, const IOOptions&,
                  IODebugContext*) override {
    buf_->insert(buf_->end(), data.data(), data.data() + data.size());
    return IOStatus::OK();
  }

  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /*info*/,
                  IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                             const IOOptions&,
                             IODebugContext*) override {
    uint64_t end = offset + data.size();
    if (end > buf_->size()) buf_->resize(end);
    memcpy(buf_->data() + offset, data.data(), data.size());
    return IOStatus::OK();
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                             const IOOptions& opts,
                             const DataVerificationInfo& /*info*/,
                             IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }

  IOStatus Flush(const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }

  IOStatus Fsync(const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions&, IODebugContext*) override {
    return IOStatus::OK();
  }

  uint64_t GetFileSize(const IOOptions&, IODebugContext*) override {
    return static_cast<uint64_t>(buf_->size());
  }

  bool   use_direct_io() const override { return false; }
  size_t GetRequiredBufferAlignment() const override { return 1; }
  IOStatus InvalidateCache(size_t, size_t) override { return IOStatus::OK(); }

private:
  std::vector<char>* buf_;
};


inline std::unique_ptr<RandomAccessFileReader> MakeMemFileReader(
    const char*        data,
    uint64_t           size,
    const std::string& label = "[mem]") {
  auto f = std::make_unique<MemReadableFile>(data, size);
  return std::make_unique<RandomAccessFileReader>(
      std::move(f), label, /*env=*/nullptr);
}

inline std::unique_ptr<WritableFileWriter> MakeMemFileWriter(
    std::vector<char>* buf,
    const std::string& label = "[mem_out]") {
  auto f = std::make_unique<MemWritableFile>(buf);
  FileOptions fopts;
  fopts.writable_file_max_buffer_size = 0;
  return std::make_unique<WritableFileWriter>(
      std::move(f), label, fopts);
}

}  // namespace ROCKSDB_NAMESPACE
