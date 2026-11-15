#pragma once
// ============================================================
//  shared/compaction_spec.h
// ============================================================

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

struct SSTFileSpec {
  uint64_t    file_number  = 0;
  uint64_t    file_size    = 0;
  uint64_t    smallest_seqno = 0;
  uint64_t    largest_seqno  = 0;
  std::string smallest_ikey;
  std::string largest_ikey;
  int         level        = 0;
  int         assigned_seg = -1;
};

struct SnapshotSpec {
  std::vector<SequenceNumber> list;
  SequenceNumber earliest_write_conflict_snapshot = 0;
  SequenceNumber preserve_deletes_seqnum          = 0;
};

struct GPFileSpec {
  std::string smallest_ikey;
  std::string largest_ikey;
  uint64_t    file_size = 0;
};
static constexpr uint32_t kSpecExtGpCut = 0x54435047u;

struct OutputSpec {
  uint64_t           target_file_size   = 64ULL << 20;
  CompressionType    compression        = kSnappyCompression;
  CompressionOptions compression_opts;
  uint64_t                gp_target_file_size     = 0;  // Compaction::target_output_file_size()
  uint64_t                gp_max_file_size        = 0;  // min(Compaction::max_output_file_size(), kChunkDataSize)
  uint64_t                gp_max_compaction_bytes = 0;  // Compaction::max_compaction_bytes()
  std::vector<GPFileSpec> grandparents;
};

struct CompactionSpec {
  uint64_t    job_id                   = 0;
  uint64_t    first_output_file_number = 0;
  int         seg                   = -1;
  int         output_level = 0;
  bool        bottommost   = false;

  std::string comparator_name;
  std::string compaction_filter_name;

  bool        has_start = false;
  std::string start_user_key;
  bool        has_end   = false;
  std::string end_user_key;

  std::vector<SSTFileSpec> inputs;
  SnapshotSpec             snapshots;
  OutputSpec               output;

  std::string Serialize() const;
  static bool Deserialize(const Slice& data, CompactionSpec* out);
};

inline bool NtbDiagCrcOn() {
  static const bool on = [] {
    const char* e = std::getenv("RC_OUTPUT_CRC");
    bool v = (e != nullptr && std::atoi(e) != 0);
    fprintf(stderr, "[rc] output content check: %s\n",
            v ? "on (FNV-1a, v-crcdiag)" : "off (v-nocrc)");
    return v;
  }();
  return on;
}

inline uint32_t NtbDiagCrc32(const char* data, size_t len) {
  if (!NtbDiagCrcOn()) return 0;
  uint32_t h = 2166136261u;
  for (size_t i = 0; i < len; i++) {
    h ^= static_cast<unsigned char>(data[i]);
    h *= 16777619u;
  }
  return h;
}

struct CompactionResult {
  uint64_t    job_id                   = 0;
  uint64_t    first_output_file_number = 0;
  bool        success = false;
  std::string error_msg;

  struct OutputFile {
    uint64_t    file_number   = 0;
    uint64_t    file_size     = 0;
    std::string smallest_ikey;
    std::string largest_ikey;
    uint64_t    smallest_seqno = 0;
    uint64_t    largest_seqno  = 0;
    uint64_t    num_entries    = 0;
    uint64_t    num_deletions  = 0;
    uint32_t    content_crc32 = 0;
  };
  std::vector<OutputFile> output_files;

  uint64_t bytes_read    = 0;
  uint64_t bytes_written = 0;
  uint64_t elapsed_us    = 0;

  std::string Serialize() const;
  static bool Deserialize(const Slice& data, CompactionResult* out);
};

}  // namespace ROCKSDB_NAMESPACE

