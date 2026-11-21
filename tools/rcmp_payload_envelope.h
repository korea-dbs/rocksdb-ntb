#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace ROCKSDB_NAMESPACE {

static constexpr uint32_t kRcmpMagic   = 0x504D4352u;
static constexpr uint16_t kRcmpVer     = 1;
static constexpr uint16_t kModePath    = 0;
static constexpr uint16_t kModePayload = 1;

struct RcmpFilePayload {
  std::string name;   // "000042.sst"
  std::string bytes;  // raw file bytes
};

inline void AppendU32(std::string* out, uint32_t v) {
  out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

inline void AppendU64(std::string* out, uint64_t v) {
  out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

inline bool ReadU32(const std::string& in, size_t* off, uint32_t* v) {
  if (*off + sizeof(uint32_t) > in.size()) return false;
  std::memcpy(v, in.data() + *off, sizeof(uint32_t));
  *off += sizeof(uint32_t);
  return true;
}

inline bool ReadU64(const std::string& in, size_t* off, uint64_t* v) {
  if (*off + sizeof(uint64_t) > in.size()) return false;
  std::memcpy(v, in.data() + *off, sizeof(uint64_t));
  *off += sizeof(uint64_t);
  return true;
}

// ── Encode ───────────────────────────────────────────────────
// header(8B) + orig_input_size(8B) + orig_input
//           + file_count(4B)
//           + [name_size(4B) + name + bytes_size(8B) + bytes] * N

inline std::string EncodePayloadEnvelope(
    const std::string& orig_input,
    const std::vector<RcmpFilePayload>& files) {

  std::string out;

  uint32_t magic   = kRcmpMagic;
  uint16_t version = kRcmpVer;
  uint16_t mode    = kModePayload;

  out.append(reinterpret_cast<const char*>(&magic),   sizeof(magic));
  out.append(reinterpret_cast<const char*>(&version), sizeof(version));
  out.append(reinterpret_cast<const char*>(&mode),    sizeof(mode));

  AppendU64(&out, static_cast<uint64_t>(orig_input.size()));
  out.append(orig_input);

  AppendU32(&out, static_cast<uint32_t>(files.size()));

  for (const auto& f : files) {
    AppendU32(&out, static_cast<uint32_t>(f.name.size()));
    out.append(f.name);
    AppendU64(&out, static_cast<uint64_t>(f.bytes.size()));
    out.append(f.bytes);
  }

  return out;
}

// ── Decode ───────────────────────────────────────────────────
// mode=kModePath  → orig_input=in, files empty (not envelope)
// mode=kModePayload → orig_input + files populated

inline bool DecodePayloadEnvelope(
    const std::string& in,
    uint16_t*          mode,
    std::string*       orig_input,
    std::vector<RcmpFilePayload>* files) {

  *mode = kModePath;
  orig_input->clear();
  files->clear();

  // 최소 헤더 크기 체크 (magic+version+mode = 8B)
  if (in.size() < sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint16_t))
    return false;

  size_t off = 0;

  uint32_t magic = 0;
  if (!ReadU32(in, &off, &magic) || magic != kRcmpMagic)
    return false;

  uint16_t ver = 0;
  if (off + sizeof(uint16_t) > in.size()) return false;
  std::memcpy(&ver, in.data() + off, sizeof(uint16_t));
  off += sizeof(uint16_t);

  if (off + sizeof(uint16_t) > in.size()) return false;
  std::memcpy(mode, in.data() + off, sizeof(uint16_t));
  off += sizeof(uint16_t);

  if (*mode != kModePayload)
    return true;   // path mode — orig_input/files 비워둠

  // orig_input
  uint64_t isz = 0;
  if (!ReadU64(in, &off, &isz)) return false;
  if (off + isz > in.size())    return false;
  *orig_input = in.substr(off, isz);
  off += isz;

  // files
  uint32_t fcnt = 0;
  if (!ReadU32(in, &off, &fcnt)) return false;

  for (uint32_t i = 0; i < fcnt; i++) {

    uint32_t nsz = 0;
    if (!ReadU32(in, &off, &nsz))   return false;
    if (off + nsz > in.size())      return false;

    RcmpFilePayload f;
    f.name = in.substr(off, nsz);
    off += nsz;

    uint64_t bsz = 0;
    if (!ReadU64(in, &off, &bsz))   return false;
    if (off + bsz > in.size())      return false;

    f.bytes = in.substr(off, bsz);
    off += bsz;

    files->push_back(std::move(f));
  }

  return true;
}

} // namespace ROCKSDB_NAMESPACE
