#pragma once
// ============================================================
//  shared/compaction_spec.h
//  [PRIMARY + WORKER 양쪽 모두 포함]
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
  int         assigned_seg = -1;  // v-multiseg: 이 파일을 보낼/받을 세그먼트
};

struct SnapshotSpec {
  std::vector<SequenceNumber> list;
  SequenceNumber earliest_write_conflict_snapshot = 0;
  SequenceNumber preserve_deletes_seqnum          = 0;
};

// v-gpcut: 출력 레벨+1(grandparent) 파일 경계. RocksDB Compaction::grandparents()
struct GPFileSpec {
  std::string smallest_ikey;
  std::string largest_ikey;
  uint64_t    file_size = 0;
};
static constexpr uint32_t kSpecExtGpCut = 0x54435047u;  // v-gpcut 확장 블록 표식 "GPCT"

struct OutputSpec {
  uint64_t           target_file_size   = 64ULL << 20;
  CompressionType    compression        = kSnappyCompression;
  CompressionOptions compression_opts;
  // v-gpcut: RocksDB CompactionOutputs::ShouldStopBefore 기준값 (0/비어 있으면 워커는 기존 크기 기준만 사용)
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

// v-crcdiag: RocksDB 내부 crc32c에 의존하지 않는 자체 체크섬 함수.
// worker_server.cc / compaction_dispatcher.cc 양쪽에서 이 헤더를 이미
// include하고 있으므로 새 include 없이 바로 쓸 수 있다. 표준 CRC32는
// 아니지만, 양쪽이 완전히 같은 알고리즘을 쓰기만 하면 진단 목적으로는
// 충분하다 (FNV-1a 32bit).
// v-nocrc (2026-11-03): 기본은 계산하지 않고 0 (양쪽 다 0이라 비교는 통과). RC_OUTPUT_CRC=1 이면 계산.
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
  if (!NtbDiagCrcOn()) return 0;  // v-nocrc
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
    // v-crcdiag: worker가 out_bufs[i] 완성 직후 NtbDiagCrc32로 계산해서
    // 채우는 체크섬. primary가 StreamRecvOutputSST로 받은 뒤 같은
    // 알고리즘으로 재계산해서 비교 -> 불일치면 전송 경로 손상,
    // 일치하는데 나중에 sst_dump에서 깨지면 persist/install 이후의
    // 문제라는 걸 가르기 위한 진단용 필드.
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
