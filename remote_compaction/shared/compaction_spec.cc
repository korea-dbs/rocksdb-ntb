// ============================================================
//  shared/compaction_spec.cc
//  [PRIMARY + WORKER 양쪽 모두 컴파일]
// ============================================================
#include "compaction_spec.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// ── 기본 인코딩 헬퍼 ─────────────────────────────────────────────

static void PutBool(std::string* d, bool v) {
  d->push_back(v ? '\x01' : '\x00');
}
static bool GetBool(Slice* s, bool* v) {
  if (s->empty()) return false;
  *v = (s->data()[0] != 0);
  s->remove_prefix(1);
  return true;
}
static void PutStr(std::string* d, const std::string& s) {
  PutLengthPrefixedSlice(d, s);
}
static bool GetStr(Slice* s, std::string* out) {
  Slice tmp;
  if (!GetLengthPrefixedSlice(s, &tmp)) return false;
  *out = tmp.ToString();
  return true;
}
static void PutU32(std::string* d, uint32_t v) { PutFixed32(d, v); }
static bool GetU32(Slice* s, uint32_t* v) {
  if (s->size() < 4) return false;
  *v = DecodeFixed32(s->data());
  s->remove_prefix(4);
  return true;
}
static void PutU64(std::string* d, uint64_t v) { PutFixed64(d, v); }
static bool GetU64(Slice* s, uint64_t* v) {
  if (s->size() < 8) return false;
  *v = DecodeFixed64(s->data());
  s->remove_prefix(8);
  return true;
}

// ── SSTFileSpec ──────────────────────────────────────────────────

static void EncodeSST(std::string* d, const SSTFileSpec& f) {
  PutU64(d, f.file_number);
  PutU64(d, f.file_size);
  PutU64(d, f.smallest_seqno);
  PutU64(d, f.largest_seqno);
  PutStr(d, f.smallest_ikey);
  PutStr(d, f.largest_ikey);
  PutU32(d, static_cast<uint32_t>(f.level));
  PutU32(d, static_cast<uint32_t>(f.assigned_seg));
}
static bool DecodeSST(Slice* s, SSTFileSpec* f) {
  uint32_t lv;
  if (!GetU64(s, &f->file_number))    return false;
  if (!GetU64(s, &f->file_size))      return false;
  if (!GetU64(s, &f->smallest_seqno)) return false;
  if (!GetU64(s, &f->largest_seqno))  return false;
  if (!GetStr(s, &f->smallest_ikey))  return false;
  if (!GetStr(s, &f->largest_ikey))   return false;
  if (!GetU32(s, &lv))                return false;
  f->level = static_cast<int>(lv);
  uint32_t aseg;
  if (!GetU32(s, &aseg))              return false;
  f->assigned_seg = static_cast<int>(aseg);
  return true;
}

// ── CompressionOptions ────────────────────────────────────────────

static void EncodeCmpOpts(std::string* d, const CompressionOptions& o) {
  PutU32(d, static_cast<uint32_t>(o.window_bits));
  PutU32(d, static_cast<uint32_t>(o.level));
  PutU32(d, static_cast<uint32_t>(o.strategy));
  PutU32(d, o.max_dict_bytes);
  PutU32(d, o.zstd_max_train_bytes);
  PutBool(d, o.enabled);
}
static bool DecodeCmpOpts(Slice* s, CompressionOptions* o) {
  uint32_t wb, lv, st, mdb, zmtb;
  if (!GetU32(s, &wb))   return false;
  if (!GetU32(s, &lv))   return false;
  if (!GetU32(s, &st))   return false;
  if (!GetU32(s, &mdb))  return false;
  if (!GetU32(s, &zmtb)) return false;
  if (!GetBool(s, &o->enabled)) return false;
  o->window_bits          = static_cast<int>(wb);
  o->level                = static_cast<int>(lv);
  o->strategy             = static_cast<int>(st);
  o->max_dict_bytes       = mdb;
  o->zstd_max_train_bytes = zmtb;
  return true;
}

// ── CompactionSpec ────────────────────────────────────────────────

std::string CompactionSpec::Serialize() const {
  std::string buf;
  PutU64(&buf, job_id);
  PutU64(&buf, first_output_file_number);
  PutU32(&buf, static_cast<uint32_t>(seg));
  PutU32(&buf, static_cast<uint32_t>(output_level));
  PutBool(&buf, bottommost);
  PutStr(&buf, comparator_name);
  PutStr(&buf, compaction_filter_name);

  PutBool(&buf, has_start);
  if (has_start) PutStr(&buf, start_user_key);
  PutBool(&buf, has_end);
  if (has_end)   PutStr(&buf, end_user_key);

  PutU32(&buf, static_cast<uint32_t>(inputs.size()));
  for (const auto& f : inputs) EncodeSST(&buf, f);

  PutU32(&buf, static_cast<uint32_t>(snapshots.list.size()));
  for (auto sn : snapshots.list) PutU64(&buf, sn);
  PutU64(&buf, snapshots.earliest_write_conflict_snapshot);
  PutU64(&buf, snapshots.preserve_deletes_seqnum);

  PutU64(&buf, output.target_file_size);
  PutU32(&buf, static_cast<uint32_t>(output.compression));
  EncodeCmpOpts(&buf, output.compression_opts);
  // v-gpcut: 확장 블록은 맨 끝에 붙인다 (구버전 Deserialize는 남는 바이트를 무시 -> 서로 호환)
  PutU32(&buf, kSpecExtGpCut);
  PutU64(&buf, output.gp_target_file_size);
  PutU64(&buf, output.gp_max_file_size);
  PutU64(&buf, output.gp_max_compaction_bytes);
  PutU32(&buf, static_cast<uint32_t>(output.grandparents.size()));
  for (const auto& g : output.grandparents) {
    PutStr(&buf, g.smallest_ikey);
    PutStr(&buf, g.largest_ikey);
    PutU64(&buf, g.file_size);
  }
  return buf;
}

bool CompactionSpec::Deserialize(const Slice& data, CompactionSpec* out) {
  Slice s = data;
  uint32_t tmp;
  if (!GetU64(&s, &out->job_id))                   return false;
  if (!GetU64(&s, &out->first_output_file_number)) return false;
  { uint32_t seg_u32 = 0; if (!GetU32(&s, &seg_u32)) return false; out->seg = static_cast<int>(seg_u32); }
  if (!GetU32(&s, &tmp))                           return false;
  out->output_level = static_cast<int>(tmp);
  if (!GetBool(&s, &out->bottommost))             return false;
  if (!GetStr(&s,  &out->comparator_name))        return false;
  if (!GetStr(&s,  &out->compaction_filter_name)) return false;

  if (!GetBool(&s, &out->has_start)) return false;
  if (out->has_start && !GetStr(&s, &out->start_user_key)) return false;
  if (!GetBool(&s, &out->has_end)) return false;
  if (out->has_end   && !GetStr(&s, &out->end_user_key))   return false;

  uint32_t n;
  if (!GetU32(&s, &n)) return false;
  out->inputs.resize(n);
  for (auto& f : out->inputs) if (!DecodeSST(&s, &f)) return false;

  uint32_t nsn;
  if (!GetU32(&s, &nsn)) return false;
  out->snapshots.list.resize(nsn);
  for (auto& sn : out->snapshots.list) if (!GetU64(&s, &sn)) return false;
  if (!GetU64(&s, &out->snapshots.earliest_write_conflict_snapshot)) return false;
  if (!GetU64(&s, &out->snapshots.preserve_deletes_seqnum))          return false;

  if (!GetU64(&s, &out->output.target_file_size)) return false;
  if (!GetU32(&s, &tmp)) return false;
  out->output.compression = static_cast<CompressionType>(tmp);
  if (!DecodeCmpOpts(&s, &out->output.compression_opts)) return false;
  // v-gpcut: 확장 블록 (없으면 기본값 = 기존 크기 기준만)
  if (s.size() >= 4 && DecodeFixed32(s.data()) == kSpecExtGpCut) {
    s.remove_prefix(4);
    if (!GetU64(&s, &out->output.gp_target_file_size))     return false;
    if (!GetU64(&s, &out->output.gp_max_file_size))        return false;
    if (!GetU64(&s, &out->output.gp_max_compaction_bytes)) return false;
    uint32_t ng;
    if (!GetU32(&s, &ng)) return false;
    out->output.grandparents.resize(ng);
    for (auto& g : out->output.grandparents) {
      if (!GetStr(&s, &g.smallest_ikey)) return false;
      if (!GetStr(&s, &g.largest_ikey))  return false;
      if (!GetU64(&s, &g.file_size))     return false;
    }
  }
  return true;
}

// ── CompactionResult ──────────────────────────────────────────────

std::string CompactionResult::Serialize() const {
  std::string buf;
  PutU64(&buf, job_id);
  PutU64(&buf, first_output_file_number);
  PutBool(&buf, success);
  PutStr(&buf, error_msg);

  PutU32(&buf, static_cast<uint32_t>(output_files.size()));
  for (const auto& f : output_files) {
    PutU64(&buf, f.file_number);
    PutU64(&buf, f.file_size);
    PutStr(&buf, f.smallest_ikey);
    PutStr(&buf, f.largest_ikey);
    PutU64(&buf, f.smallest_seqno);
    PutU64(&buf, f.largest_seqno);
    PutU64(&buf, f.num_entries);
    PutU64(&buf, f.num_deletions);
    PutU32(&buf, f.content_crc32);
  }
  PutU64(&buf, bytes_read);
  PutU64(&buf, bytes_written);
  PutU64(&buf, elapsed_us);
  return buf;
}

bool CompactionResult::Deserialize(const Slice& data, CompactionResult* out) {
  Slice s = data;
  if (!GetU64(&s, &out->job_id))                   return false;
  if (!GetU64(&s, &out->first_output_file_number)) return false;
  if (!GetBool(&s, &out->success))                 return false;
  if (!GetStr(&s,  &out->error_msg))               return false;

  uint32_t n;
  if (!GetU32(&s, &n)) return false;
  out->output_files.resize(n);
  for (auto& f : out->output_files) {
    if (!GetU64(&s, &f.file_number))    return false;
    if (!GetU64(&s, &f.file_size))      return false;
    if (!GetStr(&s, &f.smallest_ikey))  return false;
    if (!GetStr(&s, &f.largest_ikey))   return false;
    if (!GetU64(&s, &f.smallest_seqno)) return false;
    if (!GetU64(&s, &f.largest_seqno))  return false;
    if (!GetU64(&s, &f.num_entries))    return false;
    if (!GetU64(&s, &f.num_deletions))  return false;
    if (!GetU32(&s, &f.content_crc32))  return false;
  }
  if (!GetU64(&s, &out->bytes_read))    return false;
  if (!GetU64(&s, &out->bytes_written)) return false;
  if (!GetU64(&s, &out->elapsed_us))    return false;
  return true;
}

}  // namespace ROCKSDB_NAMESPACE
