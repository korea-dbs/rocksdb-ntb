// ============================================================
//  worker_only/worker_server.cc  (v13 – graceful-shutdown 타임아웃 +
//                          시그널 핸들러 안전성 수정)
//
//  v13 수정 (2026-09-16, worker_server.h v6 참고):
//    - Run()의 종료 시퀀스가 스레드를 무조건 join()해서, DMA 채널이
//      하드웨어적으로 물려 안 리턴되는 스레드가 하나라도 있으면
//      Ctrl+C/SIGTERM을 눌러도 프로세스가 아무 로그 없이 무한정
//      멈춰버리는 문제가 있었음(실제로 채널2 하드웨어 행 상태에서
//      재현됨). RecvLoop/CompactionLoop 스레드 람다가 각자 끝나면
//      recv_thread_done_/compact_thread_done_ 플래그를 세팅하도록
//      바꾸고, Run()은 join() 전에 WaitThreadDone()으로
//      kShutdownJoinTimeoutSec(20초)만 기다린 뒤 — 그 안에 안 끝나면
//      "이 스레드는 하드웨어에 갇혀서 응답 없음, 이 노드는 재부팅
//      필요"라는 로그를 즉시 찍고 join() 대신 detach()해서 나머지
//      정상 스레드들의 종료를 막지 않게 함. (하드웨어에 진짜로 갇힌
//      스레드 자체를 풀어주는 방법은 없음 — 이건 "아무 반응 없이
//      영원히 조용히 멈추는 것"만 고치는 것.)
//    - RecvLoop/CompactionLoop의 대기 루프를
//      `cv.wait(lk, pred)`(외부 notify가 와야만 깨어남)에서
//      `cv.wait_for(lk, 200ms)`를 pred가 참이 될 때까지 도는 방식으로
//      바꿈 — 이제 notify가 전혀 안 와도 최대 200ms 안에 stop_을
//      스스로 재확인함. 이걸 해둔 이유는 시그널 핸들러가 더 이상
//      notify_all()을 호출하지 않기 때문(다음 항목).
//    - worker_main.cc의 OnSignal이 기존엔 Stop()을 직접 호출했는데,
//      Stop()은 내부적으로 condition_variable::notify_all()을
//      호출함 — 이건 async-signal-safe 함수 목록에 없어서 시그널
//      핸들러 안에서 호출하면 안 됨(POSIX 시그널 안전성 규칙 위반).
//      새로 추가한 RequestStop()은 lock-free atomic store 하나만
//      하므로 시그널 핸들러 안에서 호출해도 안전함. Stop()은 그대로
//      남겨서 일반 스레드 컨텍스트(소멸자 등)에서는 여전히 store+
//      notify_all을 둘 다 하도록 유지함(더 빠르게 깨우기 위함).
//
//  v12 수정 (2026-07-25, output 파일 사이에도 primary의 ack을 기다리도록
//  추가) — job=6(output 86개)에서 L1 파일 #380/#404가 키 범위 겹침으로
//  force_consistency_checks에 걸린 사고 원인 규명:
//    - input 쪽은 v7/v22에서 "같은 job의 파일 사이엔 반드시 상대가
//      다 받았다는 ack을 받은 뒤에만 다음 파일을 보낸다"는 순서 보장이
//      있었는데, output 쪽(이 파일이 보내는 방향, worker→primary)엔
//      이 보장이 전혀 없었음 — CompactionLoop가 output 파일들을 그냥
//      순차로 StreamSendOutputSST만 호출했음. output 파일이 많은
//      job(이 사고는 86개)일수록 이 레이스에 걸릴 확률이 높아짐.
//    → primary의 OutputRecvLoop가 output 파일 하나를 완전히 수신할
//      때마다(성공/실패 모두) ctrl 채널로 "output-ack"을 보내주도록
//      바꿨고(compaction_dispatcher.cc v31 참고), 이 파일의
//      CompactionLoop는 각 output 파일을 보낸 뒤 이 ack을 받을 때까지
//      기다렸다가(최대 30초, input 쪽과 동일한 안전장치) 다음 파일을
//      보낸다. DispatcherLoop가 이 새 ack 메시지(tag=0x04)를 받아서
//      대기 중인 CompactionLoop를 깨운다. 상태 변수는 worker_server.h
//      수정을 피하기 위해 이 파일의 익명 네임스페이스에 파일-스코프로
//      둔다(HexDump 등과 같은 패턴).
//    - ntb_channel.cc의 OutputRecvChunk에 file_number 검증도 별도로
//      추가함(primary 쪽 v31) — 겹침이 실제로 생겼을 때 즉시 잡아내는
//      안전장치이고, 이번 ack 대기는 애초에 레이스가 안 생기게 막는
//      근본 수정. 두 개를 같이 적용.
//
//  v11 이력: chunk-ack에 성공/실패 플래그 추가, 실패 시 즉시 fast-fail
//  ack 전송 (아래 원래 v11 헤더 주석 참고).
//
//  v6 이력: per-thread job counter (CompactionLoop 내 total/success/
//  failed 카운트).
//
//  v7 수정 (compaction_dispatcher.cc v22와 짝을 이룸):
//    - job=1처럼 입력 파일이 2개 이상인 job에서, 같은 세그먼트를 파일
//      여러 개가 순차로 공유할 때 primary가 raw consumed 플래그
//      폴링만으로 "이 worker가 첫 파일을 다 받았는지"를 신뢰할 수 없어
//      두 번째 파일이 첫 번째 파일 자리를 덮어써버리는 레이스가 실측됨
//      (FILE_MISMATCH: expect=11 hdr=9, 이 worker의 InputRecvChunk
//      ready-poll이 spin=0으로 즉시 통과했는데 이미 두 번째 파일
//      데이터였음 — 즉 이 worker가 이 세그먼트를 감시하기 시작한
//      시점엔 이미 두 파일 다 전송이 끝나있었다는 뜻).
//    → RecvLoop가 job의 입력 파일을 하나 완전히 수신(StreamRecvSST 성공)
//      할 때마다, 그게 이 job의 마지막 입력이 아니면 ctrl 채널로
//      "chunk-ack"(tag=0x03, job_id+file_number)을 primary에 보내서
//      "이 파일 다 받았으니 다음 파일 보내도 됨"을 명시적으로 알린다.
//      primary는 이 ack을 받을 때까지 다음 파일을 보내지 않는다
//      (compaction_dispatcher.cc의 SendLoop v22 변경 참고).
//    - wire 프로토콜 변경: SendSpecMsg/RecvSpecMsg,
//      SendResultMsg/RecvResultMsg의 페이로드 맨 앞에 1바이트 태그이
//      붙게 됐음 (kMsgTypeSpec=0x01/kMsgTypeResult=0x02/
//      kMsgTypeChunkAck=0x03). DispatcherLoop에서 스펙 수신 시 이
//      태그를 검사+제거하고, RecvLoop/CompactionLoop에서 결과 메시지를
//      보낼 때 태그를 붙인다. compaction_dispatcher.cc와 태그 값이
//      반드시 일치해야 함.
//
//  v10 수정 (checksum-mismatch/all-zero-block 부패 버그 추적):
//    - job=117/118(file=2334/2444, ~67.5MB, 2청크 전송) 등에서 출력이
//      xxh3 체크섬 불일치로 실패했고, "computed" 체크섬이 해당 블록
//      크기의 all-zero 버퍼의 체크섬과 정확히 일치함을 확인함
//      (즉 그 블록 구간이 진짜로 0으로 읽혔다는 뜻). 그런데 worker.log의
//      [HDR]/[RECV_HDR_RAW] 로그가 여러 스레드의 fprintf가 동시에
//      stderr에 섞여써지면서(non-atomic) 깨져 있어 첫 청크(offset 0)가
//      실제로 수신됐는지 로그만으로는 확정할 수 없었음.
//    → (당시) RecvLoop가 StreamRecvSST로 파일 하나를 다 받은 "직후"와
//      OpenInputIterators에서 그 버퍼를 실제로 여는 "직전"에 각각
//      head/tail/경계 hex + leading_zero_run을 찍어 두 시점을 비교함.
//      (v10 결론: 두 시점이 완전히 동일해서 compact_queue_ 대기 중
//      오염은 기각됨. 원인은 ntb_channel.cc의 file_number 미검증
//      (→v27) + consumed 플래그 리셋 누락(→v28)이었음.)
//
//  v11 수정 (2026-07-25, FILE_MISMATCH 발생 시 30초 타임아웃 없이
//  즉시 반응하게 하기 위한 fast-fail 신호 추가):
//    - job=128(file=3277 성공 수신, file=695 대기 중) 케이스 분석 결과:
//      워커가 실제로는 살아서 나중에 정상적으로 파일을 받는데도,
//      primary의 SendLoop이 chunk-ack을 30초 동안 기다리다 먼저
//      타임아웃나서 job을 포기하고 다음 job으로 넘어가버림 → 그 순간
//      세그먼트가 재사용되면서, 뒤늦게 도착한 진짜 ack은 아무도 안
//      기다리고 있어서 허사가 되고, 다음 입력을 기다리던 워커는 그
//      사이 세그먼트를 차지한 엉뚱한 파일을 만나 FILE_MISMATCH가 남.
//    - 근본 원인은 "워커가 파일 수신에 실패했을 때 그 사실을 primary에
//      즉시 알릴 방법이 없었다"는 것.
//    → chunk-ack 페이로드에 1바이트 success 플래그 추가
//      (tag(1)+success(1)+job_id(8)+file_number(8) = 18바이트).
//      StreamRecvSST가 실패하면(주로 FILE_MISMATCH) 그 파일에 대해
//      즉시 "실패" chunk-ack을 보내서, primary의 SendLoop이 30초
//      타임아웃을 기다리지 않고 그 자리에서 바로 반응하게 한다.
//
//  perf 클린업 (2026-07-28): 디버깅 마라톤 중 추가됐던 핫패스(job/파일
//  단위로 매번 찍히는) 로그를 성능평가를 위해 전부 제거. 실제 에러
//  경로(실패 시에만 찍히는 것들)만 남김. 이 로그들에서만 쓰이던
//  HexDump/LeadingZeroRun 헬퍼도 안 쓰이게 되어 함께 제거. 동작 로직은
//  전혀 안 바뀜 — 순수 로그 제거.
// ============================================================
#include <map>  // v-stream
#include "db/compaction/clipping_iterator.h"  // v-wsub
#include <thread>  // v-wsub
#include <algorithm>  // v-wsub
#include "worker_server.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <functional>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "../shared/ntb_mem_file.h"

#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "db/compaction/compaction_iterator.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// v7: ctrl 메시지 종류 태그. compaction_dispatcher.cc(v22)와 반드시
// 동일해야 함.
constexpr char kMsgTypeSpec      = 0x01;
constexpr char kMsgTypeResult    = 0x02;
constexpr char kMsgTypeChunkAck  = 0x03;
constexpr char kMsgTypeJobRegistered = 0x04;  // v-reg
// v12: primary가 output 파일 하나 수신 완료/실패를 알려주는 태그
// (primary->worker, compaction_dispatcher.cc v31과 반드시 일치).
constexpr char kMsgTypeOutputAck = 0x04;

// v12: output-ack 대기 상태. worker_server.h를 건드리지 않기 위해
// 파일-스코프 static으로 둔다.
// DispatcherLoop가 primary로부터 output-ack을 받으면 여기를 세팅+notify
// 하고, CompactionLoop는 output 파일을 하나 보낼 때마다 여기서 대기한다.
std::mutex              g_output_ack_mu[kNumSegments];
std::condition_variable g_output_ack_cv[kNumSegments];
bool                    g_output_ack_ready[kNumSegments]  = {false};
uint64_t                g_output_ack_file[kNumSegments]   = {0};
bool                    g_output_ack_failed[kNumSegments] = {false};

// v-outq (2026-11-03): 출력 전역 큐 + 세그먼트 전담 송신 스레드(OutSendLoop[i]).
//   RC_OUTPUT_ANYSEG=0 이면 예전처럼 잡 세그먼트 고정. 프라이머리도 같은 값이어야 한다.
bool RcOutAnySeg() {
  static const bool on = [] {
    const char* e = getenv("RC_OUTPUT_ANYSEG");
    bool v = !(e != nullptr && atoi(e) == 0);
    fprintf(stderr, "[worker] output segment: %s\n",
            v ? "any free (v-outq)" : "job-fixed");
    return v;
  }();
  return on;
}
struct OutJobDone {
  std::mutex              mu;
  std::condition_variable cv;
  int                     left   = 0;
  bool                    failed = false;
};
struct OutTask {
  uint64_t                                         job_id = 0;
  uint64_t                                         fnum   = 0;
  std::shared_ptr<std::vector<std::vector<char>>>  bufs;
  size_t                                           idx    = 0;
  std::shared_ptr<OutJobDone>                      done;
  std::shared_ptr<std::vector<char>>               one;  // v-stream: Finish 즉시 보내는 파일 하나
};
std::mutex              g_outq_mu;
std::condition_variable g_outq_cv;
std::queue<OutTask>     g_outq;
std::once_flag          g_outq_once;
std::thread*            g_outq_threads = nullptr;  // 일부러 해제 안 함: 종료 때 joinable std::thread 소멸자가 terminate 하지 않게
std::atomic<bool>       g_outq_done[kNumSegments];
std::atomic<int>        g_outq_exited{0};

// v-nowait (2026-11-01): 프라이머리가 등록 ack를 기다리지 않으므로 데이터가
// 작업 명세보다 먼저 올 수 있다. RecvLoop는 등록될 때까지 g_reg_cv로 기다리고,
// DispatcherLoop는 등록을 마칠 때마다 g_reg_epoch를 올리고 깨운다.
std::mutex              g_reg_mu;
std::condition_variable g_reg_cv;
uint64_t                g_reg_epoch = 0;

// 프라이머리와 같은 값으로 맞출 것 (기본 0 = 대기 없음).
bool RcEnvOn(const char* name) {
  const char* e = getenv(name);
  return e != nullptr && atoi(e) != 0;
}
bool RcRegWait() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_REG_WAIT");
    fprintf(stderr, "[worker] reg-ack send: %s\n", v ? "on (v-reg)" : "off (v-nowait)");
    return v;
  }();
  return on;
}
bool RcChunkAckWait() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_CHUNK_ACK_WAIT");
    fprintf(stderr, "[worker] success chunk-ack send: %s\n", v ? "on (v22)" : "off (v-nowait)");
    return v;
  }();
  return on;
}
bool RcOutputAck() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_OUTPUT_ACK");
    fprintf(stderr, "[worker] output-ack wait: %s\n", v ? "on (v12)" : "off (v-nowait)");
    return v;
  }();
  return on;
}

std::string TagMessage(char tag, const std::string& payload) {
  std::string out;
  out.reserve(payload.size() + 1);
  out.push_back(tag);
  out.append(payload);
  return out;
}

// v11: chunk-ack 페이로드에 success 플래그 추가.
// tag(1) + success(1) + job_id(8) + file_number(8) = 18바이트.
// compaction_dispatcher.cc의 CtrlRecvLoop 파싱과 반드시 동일한 순서여야
// 함(success 바이트가 tag 바로 다음, job_id/file_number보다 앞).
std::string MakeChunkAckPayload(uint64_t job_id, uint64_t file_number,
                                 bool success, int seg) {
  // v-ackseg: ack를 받을 세그먼트를 job_seg_map_(responsible_seg)로 역추정하지
  // 않고, 보낸 쪽(worker RecvLoop[seg_idx])이 직접 seg를 실어보낸다.
  std::string out;
  out.reserve(1 + 1 + sizeof(job_id) + sizeof(file_number) + sizeof(uint32_t));
  out.push_back(kMsgTypeChunkAck);
  uint8_t ok = success ? 1 : 0;
  out.push_back(static_cast<char>(ok));
  out.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
  out.append(reinterpret_cast<const char*>(&file_number), sizeof(file_number));
  uint32_t seg_u32 = static_cast<uint32_t>(seg);
  out.append(reinterpret_cast<const char*>(&seg_u32), sizeof(seg_u32));
  return out;
}

}  // namespace

namespace {
// ---- v-wpool (2026-11-09): worker 입력 SST 버퍼 재사용 풀 ----
// 잡마다 입력 버퍼(L0->L1이면 ~1.2GB)를 새로 resize 하면 0 채우기 + 첫 접근 페이지 폴트로
// DispatcherLoop에서 잡당 ~0.5s가 걸렸다(alloc_us). 끝난 잡의 버퍼를 풀에 반납하고,
// 다음 잡은 용량이 충분한 것 중 가장 작은 걸 꺼내 resize(n)로 "줄여" 쓴다
// (줄이기는 할당/0채우기 없음). 반납 때 resize(capacity())로 미리 채워 둔다
// (이미 물리 페이지가 붙어 있어 memset만, 잡이 끝난 뒤라 다음 잡을 막지 않음).
//   RC_WPOOL_MB=N (기본 32768; 0이면 끔 = 기존 동작)
static int64_t WpoolCapBytes() {
  static const int64_t v = [] {
    const char* e = std::getenv("RC_WPOOL_MB");
    long long mb = (e && *e) ? std::atoll(e) : 32768;
    if (mb < 0) mb = 0;
    fprintf(stderr, "[worker] v-wpool: RC_WPOOL_MB=%lld\n", mb);
    return static_cast<int64_t>(mb) << 20;
  }();
  return v;
}
static std::mutex                               g_wpool_mu;
static std::multimap<size_t, std::vector<char>> g_wpool;  // capacity -> buffer
static int64_t                                  g_wpool_bytes = 0;
static std::atomic<uint64_t>                    g_wpool_hit{0}, g_wpool_miss{0};

static void WpoolTake(std::vector<char>* out, size_t n) {
  if (WpoolCapBytes() > 0) {
    std::lock_guard<std::mutex> lk(g_wpool_mu);
    auto it = g_wpool.lower_bound(n);
    if (it != g_wpool.end()) {
      g_wpool_bytes -= static_cast<int64_t>(it->first);
      *out = std::move(it->second);
      g_wpool.erase(it);
      g_wpool_hit.fetch_add(1, std::memory_order_relaxed);
      out->resize(n);  // 반납 때 size==capacity로 맞춰 둬서 여기서는 줄이기만
      return;
    }
  }
  g_wpool_miss.fetch_add(1, std::memory_order_relaxed);
  out->resize(n);
}

static void WpoolGive(std::vector<std::vector<char>>* bufs) {
  const int64_t cap = WpoolCapBytes();
  if (cap > 0) {
    for (auto& b : *bufs) {
      if (b.capacity() == 0) continue;
      b.resize(b.capacity());
      const size_t c = b.capacity();
      std::lock_guard<std::mutex> lk(g_wpool_mu);
      if (g_wpool_bytes + static_cast<int64_t>(c) > cap) continue;  // 상한 초과분은 해제
      g_wpool_bytes += static_cast<int64_t>(c);
      g_wpool.emplace(c, std::move(b));
    }
  }
  bufs->clear();
}

static long long WpoolMb() {
  std::lock_guard<std::mutex> lk(g_wpool_mu);
  return static_cast<long long>(g_wpool_bytes >> 20);
}

struct WpoolReturnGuard {
  std::vector<std::vector<char>>* bufs;
  explicit WpoolReturnGuard(std::vector<std::vector<char>>* b) : bufs(b) {}
  ~WpoolReturnGuard() { if (bufs) WpoolGive(bufs); }
};
// ---- end v-wpool ----

// v-citer (2026-11-01): RocksDB Compaction 객체 없이 CompactionIterator를
// 돌리기 위한 최소 프록시. 11.3 CompactionIterator::CompactionProxy와 1:1.
// KeyNotExistsBeyondOutputLevel은 출력이 bottommost일 때만 true -> 아래
// 레벨에 같은 키가 없다는 게 확실할 때만 삭제 표시를 지울 수 있다.
class NullCompactionProxy : public CompactionIterator::CompactionProxy {
 public:
  NullCompactionProxy(bool bottommost, int start_level, int output_level)
      : bottommost_(bottommost),
        start_level_(start_level),
        output_level_(output_level) {}

  int level() const override { return start_level_; }
  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return bottommost_;
  }
  bool bottommost_level() const override { return bottommost_; }
  int number_levels() const override { return output_level_ + 1; }
  Slice GetLargestUserKey() const override { return Slice(); }
  bool allow_ingest_behind() const override { return false; }
  bool allow_mmap_reads() const override { return false; }
  bool enable_blob_garbage_collection() const override { return false; }
  double blob_garbage_collection_age_cutoff() const override { return 0.0; }
  uint64_t blob_compaction_readahead_size() const override { return 0; }
  const Version* input_version() const override { return nullptr; }
  bool DoesInputReferenceBlobFiles() const override { return false; }
  const Compaction* real_compaction() const override { return nullptr; }
  bool SupportsPerKeyPlacement() const override { return false; }

 private:
  bool bottommost_;
  int  start_level_;
  int  output_level_;
};
}  // namespace

WorkerServer::WorkerServer(const WorkerOptions& opts) : opts_(opts) {}
WorkerServer::~WorkerServer() { Stop(); }

Status WorkerServer::Connect() {
  Status s = channel_.OpenAsWorker(opts_.primary_node_id);
  if (!s.ok()) return s;
  return Status::OK();
}

void WorkerServer::Stop() {
  stop_.store(true, std::memory_order_release);
  for (int i = 0; i < kNumSegments; i++) spec_cv_[i].notify_all();
  compact_cv_.notify_all();
}

// v13: 시그널 핸들러 전용 — lock-free atomic store 하나만 함.
// std::condition_variable::notify_all()이나 그 어떤 락/할당/I/O도
// 하지 않으므로 async-signal-safe함. RecvLoop/CompactionLoop의 대기
// 루프가 notify 없이도 최대 200ms마다 stop_을 스스로 재확인하도록
// 바뀌어 있어서(아래), 이 store 하나만으로 정지가 정상적으로 시작됨.
void WorkerServer::RequestStop() noexcept {
  stop_.store(true, std::memory_order_release);
}

// v13: 스레드 함수가 실제로 리턴했는지를 timeout_sec 동안만 확인.
// 하드웨어에 갇힌 스레드는 이 시간을 늘려도 절대 리턴하지 않으므로,
// 이 값은 "아직 정상적으로 바쁜 스레드"를 잘못 끊지 않을 정도로만
// 넉넉하면 됨(kShutdownJoinTimeoutSec 참고, 현재 20초).
bool WorkerServer::WaitThreadDone(std::atomic<bool>& done_flag,
                                   int timeout_sec) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline) {
    if (done_flag.load(std::memory_order_acquire)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return done_flag.load(std::memory_order_acquire);
}

void WorkerServer::Run() {
  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    recv_thread_done_[seg].store(false, std::memory_order_release);
    recv_threads_[i] = std::thread([this, seg]() {
      RecvLoop(seg);
      // v13: 정상적으로 리턴했다는 걸 알려서, Run()의 종료 시퀀스가
      // join() 대신 무한정 기다리지 않게 한다.
      recv_thread_done_[seg].store(true, std::memory_order_release);
    });
  }

  compact_pool_.reserve(kNumCompactionThreads);
  for (int i = 0; i < kNumCompactionThreads; i++) {
    int tid = i;
    compact_thread_done_[tid].store(false, std::memory_order_release);
    compact_pool_.emplace_back([this, tid]() {
      CompactionLoop(tid);
      compact_thread_done_[tid].store(true, std::memory_order_release);
    });
  }

  DispatcherLoop();

  // ── v13: 종료 시퀀스 — 무조건 join() 하지 않고, 먼저 각 스레드가
  // kShutdownJoinTimeoutSec 안에 실제로 끝났는지 확인한다. 끝났으면
  // join()은 즉시 리턴하므로 안전. 안 끝났으면(=하드웨어에 갇힌 것으로
  // 간주) 명확한 로그를 즉시 남기고 join() 대신 detach()해서, 이 한
  // 스레드 때문에 나머지 스레드들의 정상 종료까지 막히지 않게 한다.
  // (그 스레드 자체를 강제로 깨우는 방법은 없음 — 이건 "아무 로그 없이
  // 영원히 조용히 멈추는 것"만 막는 조치.)
  for (int i = 0; i < kNumSegments; i++) spec_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++) {
    if (WaitThreadDone(recv_thread_done_[i], kShutdownJoinTimeoutSec)) {
      recv_threads_[i].join();
    } else {
      fprintf(stderr,
              "[worker] Run(): RecvLoop[%d] did not stop within %ds — "
              "it is very likely stuck inside a wedged NTB/DMA channel "
              "call that software cannot cancel. Abandoning this thread "
              "(detached) so the rest of shutdown can proceed; this node "
              "will need a reboot to fully clear it.\n",
              i, kShutdownJoinTimeoutSec);
      recv_threads_[i].detach();
    }
  }

  compact_cv_.notify_all();
  for (size_t i = 0; i < compact_pool_.size(); i++) {
    if (WaitThreadDone(compact_thread_done_[i], kShutdownJoinTimeoutSec)) {
      compact_pool_[i].join();
    } else {
      fprintf(stderr,
              "[worker] Run(): CompactionLoop[%zu] did not stop within "
              "%ds — it is very likely stuck inside a wedged NTB/DMA "
              "channel call that software cannot cancel. Abandoning this "
              "thread (detached) so the rest of shutdown can proceed; "
              "this node will need a reboot to fully clear it.\n",
              i, kShutdownJoinTimeoutSec);
      compact_pool_[i].detach();
    }
  }

  fprintf(stderr, "[worker] Run(): shutdown sequence complete.\n");
}

// ── DispatcherLoop ────────────────────────────────────────────────
void WorkerServer::DispatcherLoop() {
  while (!stop_.load(std::memory_order_acquire)) {
    std::string raw;
    Status s = channel_.RecvSpecMsg(&raw);
    if (!s.ok()) {
      if (stop_.load(std::memory_order_acquire)) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    // v12: output-ack(primary -> worker, tag=0x04)은 스펙이 아니므로
    // 먼저 걸러서 처리한다. 페이로드 포맷은 chunk-ack과 동일:
    // tag(1)+success(1)+job_id(8)+file_number(8) = 18바이트.
    if (!raw.empty() && raw[0] == kMsgTypeOutputAck) {
      if (raw.size() < 1 + 1 + sizeof(uint64_t) * 2) {
        fprintf(stderr,
                "[worker] DispatcherLoop: malformed output-ack (size=%zu)\n",
                raw.size());
        continue;
      }
      uint8_t  success_byte   = static_cast<uint8_t>(raw[1]);
      uint64_t ack_job_id     = 0, ack_file_number = 0;
      memcpy(&ack_job_id, raw.data() + 2, sizeof(ack_job_id));
      memcpy(&ack_file_number, raw.data() + 2 + sizeof(ack_job_id),
             sizeof(ack_file_number));
      bool success = (success_byte != 0);
      int seg = 0; { std::lock_guard<std::mutex> lk(job_seg_mu_); auto sit = job_seg_map_.find(ack_job_id); seg = (sit != job_seg_map_.end()) ? sit->second : static_cast<int>(ack_job_id % kNumSegments); }
      {
        std::lock_guard<std::mutex> lk(g_output_ack_mu[seg]);
        g_output_ack_ready[seg]  = true;
        g_output_ack_file[seg]   = ack_file_number;
        g_output_ack_failed[seg] = !success;
      }
      g_output_ack_cv[seg].notify_one();
      continue;
    }

    // v7: 태그 검사 후 스펙 페이로드만 떼어서 역직렬화.
    if (raw.empty() || raw[0] != kMsgTypeSpec) {
      fprintf(stderr,
              "[worker] DispatcherLoop: unexpected ctrl tag (size=%zu)\n",
              raw.size());
      continue;
    }
    Slice spec_payload(raw.data() + 1, raw.size() - 1);

    CompactionSpec spec;
    if (!CompactionSpec::Deserialize(spec_payload, &spec)) {
      fprintf(stderr, "[worker] DispatcherLoop: deserialize failed\n");
      continue;
    }
    uint64_t job_id = spec.job_id;
    int seg = spec.seg;
    { std::lock_guard<std::mutex> lk(job_seg_mu_); job_seg_map_[job_id] = seg; }

    // v-byfile: 입력파일을 assigned_seg별로 묶는 건 그대로지만, 이제
    // "순서가 보장된 큐에 task를 넣는" 게 아니라 세그먼트별
    // pending_recv_에 "이 file_number를 기다리는 수신자"로 등록한다.
    // RecvLoop[seg]가 헤더로 읽은 실제 file_number로 이 map을 찾아
    // 매칭하므로, ctrl 메시지 처리 순서와 실제 데이터 도착 순서가
    // job마다 달라도 안전하다 (바로 이게 FILE_MISMATCH 잔여분의 원인).
    std::map<int, std::vector<size_t>> by_seg;
    for (size_t i = 0; i < spec.inputs.size(); i++) {
      int aseg = spec.inputs[i].assigned_seg;
      if (aseg < 0 || aseg >= kNumSegments) aseg = seg;  // 구버전 primary 호환용 안전장치
      by_seg[aseg].push_back(i);
    }
    if (by_seg.empty()) by_seg[seg];  // 입력 0개인 예외 케이스

    auto ja = std::make_shared<JobAssembly>();
    ja->sst_bufs.resize(spec.inputs.size());
    ja->remaining_files.store(static_cast<int>(spec.inputs.size()),
                               std::memory_order_relaxed);
    auto rc_alloc_t0 = std::chrono::steady_clock::now();  // v-nopad
    for (size_t i = 0; i < spec.inputs.size(); i++) {
      // v-nopad: 청크는 파일 끝을 넘지 않으므로 +kChunkDataSize(90MB) 여유가 필요 없다
      WpoolTake(&ja->sst_bufs[i], spec.inputs[i].file_size);  // v-wpool (was resize)
    }
    fprintf(stderr, "[worker][timing] job=%llu alloc_us=%lld files=%zu\n",
            (unsigned long long)job_id,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - rc_alloc_t0).count(),
            spec.inputs.size());
    fprintf(stderr, "[worker][wpool] job=%llu hit=%llu miss=%llu pool_mb=%lld\n",  // v-wpool
            (unsigned long long)job_id, (unsigned long long)g_wpool_hit.load(),
            (unsigned long long)g_wpool_miss.load(), WpoolMb());
    ja->spec = std::move(spec);

    {
      std::lock_guard<std::mutex> lk(job_assembly_mu_);
      job_assembly_[job_id] = ja;
    }

    // v-filesplit: RecvLoop가 모든 세그먼트의 등록표를 찾으므로, 같은 파일
    // 번호의 옛 등록(실패한 이전 job 등)이 다른 세그먼트 표에 남아 있으면
    // 새 데이터가 죽은 job으로 가지 않도록 먼저 지운다.
    for (size_t fi = 0; fi < ja->spec.inputs.size(); fi++) {
      uint64_t old_fnum = ja->spec.inputs[fi].file_number;
      for (int k = 0; k < kNumSegments; k++) {
        std::lock_guard<std::mutex> lk(pending_mu_[k]);
        pending_recv_[k].erase(old_fnum);
      }
    }

    for (auto& kv : by_seg) {
      int s = kv.first;
      std::lock_guard<std::mutex> lk(pending_mu_[s]);
      for (size_t i : kv.second) {
        uint64_t fnum = ja->spec.inputs[i].file_number;
        PendingRecv pr;
        pr.job_id     = job_id;
        pr.file_index = i;
        pr.ja         = ja;
        pending_recv_[s][fnum] = std::move(pr);
      }
    }

    // v-reg: pending_recv_ 등록을 끝냈다는 ack을 primary로 보낸다.
    // primary의 SendLoop이 이걸 받기 전까지는 실제 입력 데이터를 보내지
    // 않으므로, "데이터가 등록보다 먼저 도착"하는 레이스가 구조적으로
    // 없어진다 (기존엔 RecvLoop가 2초 기다리다 포기하고 버리는 식으로
    // 완화만 했었음).
    {
      std::string reg_payload;
      reg_payload.push_back(kMsgTypeJobRegistered);
      reg_payload.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
      if (RcRegWait())  // v-nowait
        channel_.SendResultMsg(static_cast<int>(job_id % kNumSegments), reg_payload);
    }
    {  // v-nowait: 이 등록을 기다리던 RecvLoop를 깨운다
      std::lock_guard<std::mutex> rlk(g_reg_mu);
      g_reg_epoch++;
    }
    g_reg_cv.notify_all();

    // v-byfile: 입력파일이 0개인 예외 케이스는 어떤 RecvLoop도 이
    // job에 대한 chunk를 받을 일이 없으므로, 여기서 바로 마무리한다.
    if (ja->remaining_files.load(std::memory_order_acquire) <= 0) {
      FinalizeJobAssembly(job_id, ja);
    }
  }
}

// ── RecvLoop ──────────────────────────────────────────────────────
// v-byfile: pending_recv_ 조회가 바로 못 찾아도(ctrl Spec이 아직 처리
// 중일 수 있음) 이 시간 동안은 재시도한다. 30초짜리 하드웨어 타임아웃에
// 비하면 훨씨 짧게 잡아도 충분한 여유임.
static constexpr int kPendingLookupTimeoutMs = 30000;  // v-nowait: 장애 감지용 상한 (정상은 등록 즉시 깨어남)

// v-byfile: RecvLoop의 "이 job 다 받았음" 처리와, 입력파일 0개인 job을
// DispatcherLoop에서 즉시 끝내는 처리가 공유하는 마무리 로직.
void WorkerServer::FinalizeJobAssembly(uint64_t job_id,
                                       std::shared_ptr<JobAssembly> ja) {
  {
    std::lock_guard<std::mutex> lk(job_assembly_mu_);
    job_assembly_.erase(job_id);
  }

  if (ja->failed.load(std::memory_order_relaxed)) {
    CompactionResult err;
    err.job_id    = job_id;
    err.success   = false;
    err.error_msg = "SST receive failed";
    std::lock_guard<std::mutex> out_lk(ack_send_mu_[0]);
    channel_.SendResultMsg(static_cast<int>(job_id % kNumSegments),
                           TagMessage(kMsgTypeResult, err.Serialize()));
    return;
  }

  fprintf(stderr, "[worker][timing] job=%llu enqueue t_us=%lld\n",
          (unsigned long long)job_id,
          (long long)std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count());
  {
    WorkerJob job;
    job.spec      = std::move(ja->spec);
    job.fixed_seg = job.spec.seg;
    job.sst_bufs  = std::move(ja->sst_bufs);
    std::lock_guard<std::mutex> lk(compact_mu_);
    compact_queue_.push(std::move(job));
  }
  compact_cv_.notify_one();
}

void WorkerServer::RecvLoop(int seg_idx) {
  // v-byfile: 큐에서 다음 태스크를 꺼내 그게 다음에 올 파일이라고
  // 가정하지 않는다. 세그먼트에 실제로 도착한 헤더를 먼저 읽어서 그
  // file_number로 pending_recv_[seg_idx]를 찾아 매칭한다.
  while (!stop_.load(std::memory_order_acquire)) {
    uint64_t recv_file = 0, file_offset = 0, chunk_bytes = 0;
    bool is_last = false;
    uint64_t hdr_job = 0;  // v-nowait: 프라이머리가 청크 헤더에 실은 job_id (0 = 미지정)
    Status ps = channel_.InputPeekChunk(seg_idx, &recv_file, &file_offset,
                                        &chunk_bytes, &is_last, &hdr_job);
    if (!ps.ok()) {
      if (stop_.load(std::memory_order_acquire)) break;
      continue;  // ready-flag poll timeout 등 -- 그냥 다시 폴링
    }

    // v-nowait: 등록표에서 file_number로 찾되 job_id까지 맞춘다.
    //   등록 job_id <  헤더 job_id : 옛 잡의 등록 -> 새 등록을 기다림
    //   등록 job_id >  헤더 job_id : 옛 잡의 남은 데이터 -> 버림
    // 아직 등록이 없으면 DispatcherLoop가 등록할 때까지 g_reg_cv로 기다린다
    // (1ms 폴링 없음, 등록되는 즉시 깨어남).
    PendingRecv pr;
    bool found = false;
    bool stale_data = false;
    auto t_wait0 = std::chrono::steady_clock::now();
    for (;;) {
      uint64_t epoch0;
      {
        std::lock_guard<std::mutex> rlk(g_reg_mu);
        epoch0 = g_reg_epoch;
      }
      for (int k = 0; k < kNumSegments && !found && !stale_data; k++) {
        int s2 = (seg_idx + k) % kNumSegments;
        std::lock_guard<std::mutex> lk(pending_mu_[s2]);
        auto it = pending_recv_[s2].find(recv_file);
        if (it == pending_recv_[s2].end()) continue;
        if (hdr_job != 0 && it->second.job_id > hdr_job) { stale_data = true; break; }
        if (hdr_job != 0 && it->second.job_id < hdr_job) continue;
        pr = it->second;
        found = true;
        if (is_last) pending_recv_[s2].erase(it);
      }
      if (found || stale_data) break;
      if (stop_.load(std::memory_order_acquire)) break;
      if (std::chrono::steady_clock::now() - t_wait0 >
          std::chrono::milliseconds(kPendingLookupTimeoutMs)) {
        break;
      }
      std::unique_lock<std::mutex> rlk(g_reg_mu);
      g_reg_cv.wait_for(rlk, std::chrono::milliseconds(100),
                        [epoch0] { return g_reg_epoch != epoch0; });
    }

    if (!found) {
      fprintf(stderr,
              "[worker] RecvLoop[%d]: file_number=%llu hdr_job=%llu %s -- 버림\n",
              seg_idx, (unsigned long long)recv_file,
              (unsigned long long)hdr_job,
              stale_data ? "옛 잡의 남은 데이터"
                         : "도착했는데 기다리는 job을 못 찾음 (상한 초과)");
      channel_.InputConsumeChunk(seg_idx, nullptr, 0);
      if (!stale_data && hdr_job != 0) {
        // 프라이머리가 이 잡을 바로 실패 처리하도록 (v-noack 핸들러)
        std::string ack = MakeChunkAckPayload(hdr_job, recv_file,
                                               /*success=*/false, seg_idx);
        std::lock_guard<std::mutex> out_lk(ack_send_mu_[seg_idx]);
        channel_.SendResultMsg(seg_idx, ack);
      }
      continue;
    }

    char* dest = nullptr;  // v-nopad: 버퍼 범위 검사
    {
      auto& rbuf = pr.ja->sst_bufs[pr.file_index];
      if (file_offset + chunk_bytes <= rbuf.size()) {
        dest = rbuf.data() + file_offset;
      } else {
        fprintf(stderr,
                "[worker] RecvLoop[%d]: v-nopad OVERFLOW job=%llu file=%llu "
                "off=%llu bytes=%llu buf=%zu -- 잡 실패 처리\n",
                seg_idx, (unsigned long long)pr.job_id,
                (unsigned long long)recv_file,
                (unsigned long long)file_offset,
                (unsigned long long)chunk_bytes, rbuf.size());
        pr.ja->failed.store(true, std::memory_order_relaxed);
      }
    }
    channel_.InputConsumeChunk(seg_idx, dest, chunk_bytes);

    if (RcChunkAckWait()) {  // v-nowait: 성공 ack는 프라이머리가 기다릴 때만
      std::string ack = MakeChunkAckPayload(pr.job_id, recv_file,
                                             /*success=*/true, seg_idx);
      std::lock_guard<std::mutex> out_lk(ack_send_mu_[seg_idx]);
      channel_.SendResultMsg(seg_idx, ack);
    }

    fprintf(stderr,
            "[worker][timing] job=%llu seg=%d input_file=%llu idx=%zu "
            "arrive_t_us=%lld\n",
            (unsigned long long)pr.job_id, seg_idx,
            (unsigned long long)recv_file, pr.file_index,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());

    if (is_last) {
      pr.ja->sst_bufs[pr.file_index].resize(
          pr.ja->spec.inputs[pr.file_index].file_size);
      int remaining = pr.ja->remaining_files.fetch_sub(
                           1, std::memory_order_acq_rel) - 1;
      if (remaining <= 0) {
        FinalizeJobAssembly(pr.job_id, pr.ja);
      }
    }
  }
}

// ── v-stream: 출력 SST가 Finish()되면 바로 송신 큐로 (RC_STREAM_PERSIST=2) ──
std::mutex                                      g_sjobs_mu;
std::map<uint64_t, std::shared_ptr<OutJobDone>> g_sjobs;
bool RcStreamGen() {
  static const bool on = [] {
    const char* e = getenv("RC_STREAM_PERSIST");
    bool v = (e != nullptr && atoi(e) == 2) && RcOutAnySeg();
    fprintf(stderr, "[worker] v-stream send-on-finish: %s\n", v ? "on" : "off");
    return v;
  }();
  return on;
}
void RcStreamEmit(uint64_t job_id, uint64_t fnum, std::vector<char>* buf) {
  std::shared_ptr<OutJobDone> d;
  {
    std::lock_guard<std::mutex> g(g_sjobs_mu);
    auto it = g_sjobs.find(job_id);
    if (it == g_sjobs.end()) return;  // 등록 안 된 잡: 기존처럼 결과 뒤에 보냄
    d = it->second;
  }
  OutTask t;
  t.job_id = job_id;
  t.fnum   = fnum;
  t.idx    = 0;
  t.done   = d;
  t.one    = std::make_shared<std::vector<char>>(std::move(*buf));
  {
    std::lock_guard<std::mutex> dl(d->mu);
    d->left++;
  }
  {
    std::lock_guard<std::mutex> ql(g_outq_mu);
    g_outq.push(std::move(t));
  }
  g_outq_cv.notify_all();
}

// ── CompactionLoop (v6: 카운터 추가, v7: 결과 메시지 태그) ────────
void WorkerServer::CompactionLoop(int thread_id) {
  uint64_t total_jobs   = 0;
  uint64_t success_jobs = 0;
  uint64_t failed_jobs  = 0;

  while (!stop_.load(std::memory_order_acquire)) {
    WorkerJob job;
    {
      std::unique_lock<std::mutex> lk(compact_mu_);
      // v13: RecvLoop와 동일하게, notify 없이도 최대 200ms마다 stop_을
      // 스스로 재확인한다.
      while (compact_queue_.empty() &&
             !stop_.load(std::memory_order_acquire)) {
        compact_cv_.wait_for(lk, std::chrono::milliseconds(200));
      }
      if (stop_.load(std::memory_order_acquire) &&
          compact_queue_.empty()) break;
      job = std::move(compact_queue_.front());
      compact_queue_.pop();
    }

    total_jobs++;
    int seg = job.fixed_seg;
    // gyu modify: worker 큐 대기 시간 계측 (dequeue/start 시점)
    fprintf(stderr, "[worker][timing] job=%llu start t_us=%lld\n",
            (unsigned long long)job.spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());

    std::shared_ptr<OutJobDone> sdone;  // v-stream
    if (RcStreamGen()) {
      std::call_once(g_outq_once, [this]() {
                g_outq_threads = new std::thread[kNumSegments];
                for (int i = 0; i < kNumSegments; i++) {
                  g_outq_done[i].store(false, std::memory_order_release);
                  g_outq_threads[i] = std::thread([this, i]() {
                    while (true) {
                      OutTask t;
                      {
                        std::unique_lock<std::mutex> lk(g_outq_mu);
                        // 200ms는 stop_ 재확인용 (RequestStop은 notify를 안 함). 일은 notify로 바로 깬다.
                        while (g_outq.empty() && !stop_.load(std::memory_order_acquire))
                          g_outq_cv.wait_for(lk, std::chrono::milliseconds(200));
                        if (stop_.load(std::memory_order_acquire)) break;
                        t = std::move(g_outq.front());
                        g_outq.pop();
                      }
                      Status ws = channel_.StreamSendOutputSST(t.one ? *t.one : (*t.bufs)[t.idx], t.fnum, i, t.job_id);  // v-stream
                      if (!ws.ok()) {
                        fprintf(stderr,
                                "[worker] OutSendLoop[%d]: StreamSendOutputSST failed "
                                "job=%llu fnum=%llu: %s\n",
                                i, (unsigned long long)t.job_id, (unsigned long long)t.fnum,
                                ws.ToString().c_str());
                      }
                      {
                        std::lock_guard<std::mutex> dlk(t.done->mu);
                        if (!ws.ok()) t.done->failed = true;
                        t.done->left--;
                      }
                      t.done->cv.notify_all();
                    }
                    g_outq_done[i].store(true, std::memory_order_release);
                  });
                }
                fprintf(stderr, "[worker] v-outq: %d output sender threads started\n", kNumSegments);
              });  // v-stream: 송신 스레드를 미리 시작
      sdone = std::make_shared<OutJobDone>();
      std::lock_guard<std::mutex> sg(g_sjobs_mu);
      g_sjobs[job.spec.job_id] = sdone;
    }
    CompactionResult               result;
    std::vector<std::vector<char>> out_bufs;
    Status s = HandleJob(job.spec, job.sst_bufs, &result, &out_bufs);
    WpoolReturnGuard wpool_guard(&job.sst_bufs);  // v-wpool: 이 반복이 끝날 때 입력 버퍼를 풀에 반납
    // gyu modify: HandleJob 소요 시간 계측 (done 시점)
    fprintf(stderr, "[worker][timing] job=%llu handle_done t_us=%lld\n",
            (unsigned long long)job.spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
    if (sdone) {  // v-stream: Finish 즉시 보낸 출력이 모두 나간 뒤 결과를 보낸다
      {
        std::lock_guard<std::mutex> sg(g_sjobs_mu);
        g_sjobs.erase(job.spec.job_id);
      }
      std::unique_lock<std::mutex> sdl(sdone->mu);
      while (sdone->left > 0 && !stop_.load(std::memory_order_acquire))
        sdone->cv.wait_for(sdl, std::chrono::milliseconds(200));
      if (sdone->failed && s.ok()) s = Status::IOError("v-stream: output send failed");
    }

    if (!s.ok()) {
      failed_jobs++;
      fprintf(stderr,
              "[worker] CompactionLoop[%d]: job=%llu failed "
              "(ok=%llu fail=%llu): %s\n",
              thread_id, (unsigned long long)job.spec.job_id,
              (unsigned long long)success_jobs,
              (unsigned long long)failed_jobs,
              s.ToString().c_str());

      // v-diag-off: 기본은 끔 (디스크 용량 문제로 비활성화).
      // 재빌드 없이 다시 보고 싶으면 RC_DIAG_DUMP=1 환경변수로 켠다.
      if (getenv("RC_DIAG_DUMP")) {
      for (size_t vi = 0; vi < job.sst_bufs.size(); vi++) {
        uint64_t vfnum = job.spec.inputs[vi].file_number;
        std::string vpath = std::string(getenv("HOME") ? getenv("HOME") : ".") +
                             "/dma_corrupt_dumps/job" +
                             std::to_string(job.spec.job_id) + "_f" +
                             std::to_string(vfnum) + ".bin";
        FILE* vfp = fopen(vpath.c_str(), "wb");
        if (vfp) {
          fwrite(job.sst_bufs[vi].data(), 1, job.sst_bufs[vi].size(), vfp);
          fclose(vfp);
          fprintf(stderr, "[diag] dumped job=%llu file=%llu size=%zu -> %s\n",
                  (unsigned long long)job.spec.job_id,
                  (unsigned long long)vfnum, job.sst_bufs[vi].size(),
                  vpath.c_str());
        } else {
          fprintf(stderr, "[diag] dump FAILED to open %s\n", vpath.c_str());
        }
      }
      }

      result.job_id    = job.spec.job_id;
      result.success   = false;
      result.error_msg = s.ToString();
      std::lock_guard<std::mutex> out_lk(send_output_mu_[seg]);
      {  // v-ctrlslot: 결과는 잡마다 한 번뿐이라 버리지 않고 성공할 때까지 다시 보낸다
        Status rs_;
        while (!(rs_ = channel_.SendResultMsg(seg, TagMessage(kMsgTypeResult, result.Serialize()))).ok() &&
               !stop_.load(std::memory_order_acquire)) {
          fprintf(stderr, "[worker] Result send failed job=%llu seg=%d: %s -- retry (v-ctrlslot)\n",
                  (unsigned long long)job.spec.job_id, seg, rs_.ToString().c_str());
        }
      }
      continue;
    }

    success_jobs++;

    if (RcOutAnySeg()) {  // v-outq: 결과 먼저, 출력 파일은 전역 큐 -> 빈 세그먼트의 송신 스레드가 가져감
      {  // v-ctrlslot: 결과는 잡마다 한 번뿐이라 버리지 않고 성공할 때까지 다시 보낸다
        Status rs_;
        while (!(rs_ = channel_.SendResultMsg(seg, TagMessage(kMsgTypeResult, result.Serialize()))).ok() &&
               !stop_.load(std::memory_order_acquire)) {
          fprintf(stderr, "[worker] Result send failed job=%llu seg=%d: %s -- retry (v-ctrlslot)\n",
                  (unsigned long long)job.spec.job_id, seg, rs_.ToString().c_str());
        }
      }
      if (!out_bufs.empty() && !sdone) {  // v-stream: 생성 중 이미 보냄
        std::call_once(g_outq_once, [this]() {
          g_outq_threads = new std::thread[kNumSegments];
          for (int i = 0; i < kNumSegments; i++) {
            g_outq_done[i].store(false, std::memory_order_release);
            g_outq_threads[i] = std::thread([this, i]() {
              while (true) {
                OutTask t;
                {
                  std::unique_lock<std::mutex> lk(g_outq_mu);
                  // 200ms는 stop_ 재확인용 (RequestStop은 notify를 안 함). 일은 notify로 바로 깬다.
                  while (g_outq.empty() && !stop_.load(std::memory_order_acquire))
                    g_outq_cv.wait_for(lk, std::chrono::milliseconds(200));
                  if (stop_.load(std::memory_order_acquire)) break;
                  t = std::move(g_outq.front());
                  g_outq.pop();
                }
                Status ws = channel_.StreamSendOutputSST(t.one ? *t.one : (*t.bufs)[t.idx], t.fnum, i, t.job_id);  // v-stream
                if (!ws.ok()) {
                  fprintf(stderr,
                          "[worker] OutSendLoop[%d]: StreamSendOutputSST failed "
                          "job=%llu fnum=%llu: %s\n",
                          i, (unsigned long long)t.job_id, (unsigned long long)t.fnum,
                          ws.ToString().c_str());
                }
                {
                  std::lock_guard<std::mutex> dlk(t.done->mu);
                  if (!ws.ok()) t.done->failed = true;
                  t.done->left--;
                }
                t.done->cv.notify_all();
              }
              g_outq_done[i].store(true, std::memory_order_release);
            });
          }
          fprintf(stderr, "[worker] v-outq: %d output sender threads started\n", kNumSegments);
        });
        auto bufs = std::make_shared<std::vector<std::vector<char>>>(std::move(out_bufs));
        auto done = std::make_shared<OutJobDone>();
        done->left = static_cast<int>(bufs->size());
        {
          std::lock_guard<std::mutex> qlk(g_outq_mu);
          for (size_t i = 0; i < bufs->size(); i++) {
            OutTask t;
            t.job_id = job.spec.job_id;
            t.fnum   = result.output_files[i].file_number;
            t.bufs   = bufs;
            t.idx    = i;
            t.done   = done;
            g_outq.push(std::move(t));
          }
        }
        g_outq_cv.notify_all();
        // 백프레셔: 이 잡의 출력이 다 나갈 때까지 다음 잡을 잡지 않는다 (출력 버퍼 메모리 상한 = 예전과 같음)
        std::unique_lock<std::mutex> dlk(done->mu);
        while (done->left > 0 && !stop_.load(std::memory_order_acquire))
          done->cv.wait_for(dlk, std::chrono::milliseconds(200));
        if (done->failed)
          fprintf(stderr, "[worker] CompactionLoop[%d]: job=%llu some outputs failed to send (v-outq)\n",
                  thread_id, (unsigned long long)job.spec.job_id);
      }
      continue;
    }

    {
      std::lock_guard<std::mutex> out_lk(send_output_mu_[seg]);
      {  // v-ctrlslot: 결과는 잡마다 한 번뿐이라 버리지 않고 성공할 때까지 다시 보낸다
        Status rs_;
        while (!(rs_ = channel_.SendResultMsg(seg, TagMessage(kMsgTypeResult, result.Serialize()))).ok() &&
               !stop_.load(std::memory_order_acquire)) {
          fprintf(stderr, "[worker] Result send failed job=%llu seg=%d: %s -- retry (v-ctrlslot)\n",
                  (unsigned long long)job.spec.job_id, seg, rs_.ToString().c_str());
        }
      }
      for (size_t i = 0; i < out_bufs.size(); i++) {
        uint64_t fnum = result.output_files[i].file_number;
        Status   ws   = channel_.StreamSendOutputSST(out_bufs[i], fnum, seg,
                                                     job.spec.job_id);  // v-stale
        if (!ws.ok()) {
          fprintf(stderr,
                  "[worker] CompactionLoop[%d]: StreamSendOutputSST failed "
                  "job=%llu fnum=%llu: %s\n",
                  thread_id, (unsigned long long)job.spec.job_id,
                  (unsigned long long)fnum, ws.ToString().c_str());
          break;
        }

        // v12: 같은 job의 다음 output 파일을 보내기 전에, primary가
        // 방금 보낸 파일을 완전히 수신했다는 output-ack을 ctrl 채널로
        // 받을 때까지 기다린다(input 쪽 v22 chunk-ack과 대칭). 이게
        // 없으면 output 파일이 많은 job에서 세그먼트를 여러 파일이
        // 순차 공유하다 겹치는 레이스가 생겨 primary 쪽 L1 파일 키
        // 범위 겹침 부패로 이어짐(job=6, output 86개에서 실측).
        if (RcOutputAck()) {  // v-nowait
          std::unique_lock<std::mutex> ack_lk(g_output_ack_mu[seg]);
          uint64_t expect_file = fnum;
          bool got = g_output_ack_cv[seg].wait_for(
              ack_lk, std::chrono::seconds(30),
              [this, seg, expect_file] {
                return (g_output_ack_ready[seg] &&
                        g_output_ack_file[seg] == expect_file) ||
                       stop_.load(std::memory_order_acquire);
              });
          if (stop_.load(std::memory_order_acquire)) break;
          if (!got) {
            fprintf(stderr,
                    "[worker] CompactionLoop[%d]: output-ack TIMEOUT "
                    "job=%llu file=%llu\n",
                    thread_id, (unsigned long long)job.spec.job_id,
                    (unsigned long long)fnum);
            break;
          }
          bool failed = g_output_ack_failed[seg];
          g_output_ack_ready[seg]  = false;
          g_output_ack_failed[seg] = false;
          if (failed) {
            fprintf(stderr,
                    "[worker] CompactionLoop[%d]: output-ack FAILED "
                    "job=%llu file=%llu\n",
                    thread_id, (unsigned long long)job.spec.job_id,
                    (unsigned long long)fnum);
            break;
          }
        }
      }
    }
  }

  // v-outq: 마지막으로 끝나는 CompactionLoop가 출력 송신 스레드를 정리한다
  if (g_outq_exited.fetch_add(1) + 1 == kNumCompactionThreads && g_outq_threads != nullptr) {
    g_outq_cv.notify_all();
    for (int i = 0; i < kNumSegments; i++) {
      if (!g_outq_threads[i].joinable()) continue;
      if (WaitThreadDone(g_outq_done[i], kShutdownJoinTimeoutSec)) {
        g_outq_threads[i].join();
      } else {
        fprintf(stderr, "[worker] OutSendLoop[%d] did not finish in %ds -- detach (v-outq)\n",
                i, kShutdownJoinTimeoutSec);
        g_outq_threads[i].detach();
      }
    }
  }

  fprintf(stderr,
          "[worker] CompactionLoop[%d] exiting: "
          "total=%llu success=%llu failed=%llu\n",
          thread_id,
          (unsigned long long)total_jobs,
          (unsigned long long)success_jobs,
          (unsigned long long)failed_jobs);
}

// ── FindComparator ────────────────────────────────────────────────
const Comparator* WorkerServer::FindComparator(const std::string& name) {
  if (name == BytewiseComparator()->Name()) return BytewiseComparator();
  if (name == ReverseBytewiseComparator()->Name())
    return ReverseBytewiseComparator();
  return nullptr;
}

// ── OpenInputIterators ────────────────────────────────────────────
Status WorkerServer::OpenInputIterators(
    const CompactionSpec&                 spec,
    const Options&                        opts,
    const ReadOptions&                    ro,
    const ImmutableOptions&               iopts,
    const std::vector<std::vector<char>>& sst_bufs,
    std::vector<InternalIterator*>*       iters_out,
    std::vector<TableReader*>*            readers_out) {

  for (size_t i = 0; i < spec.inputs.size(); i++) {
    const auto& f   = spec.inputs[i];
    const auto& buf = sst_bufs[i];

    // MakeMemFileReader: ntb_mem_file.h 헬퍼
    auto file_reader = MakeMemFileReader(
        buf.data(), static_cast<uint64_t>(buf.size()),
        "mem_in_" + std::to_string(f.file_number));

    TableReaderOptions tro(
        iopts,
        /*prefix_extractor=*/nullptr,
        /*compression_manager=*/nullptr,
        EnvOptions(),
        iopts.internal_comparator,
        /*block_protection_bytes_per_key=*/0,
        /*skip_filters=*/false,
        /*immortal=*/false,
        /*force_direct_prefetch=*/false,
        f.level);

    std::unique_ptr<TableReader> reader;
    Status s = opts.table_factory->NewTableReader(
        tro,
        std::move(file_reader),
        static_cast<uint64_t>(buf.size()),
        &reader);
    if (!s.ok()) {
      fprintf(stderr,
              "[worker] OpenInputIterators: NewTableReader failed "
              "file=%llu: %s\n",
              (unsigned long long)f.file_number, s.ToString().c_str());
      return s;
    }

    // v-citer: 범위 삭제(DeleteRange)와 머지 피연산자는 이 워커가 아직
    // 처리하지 않는다. 조용히 데이터를 잃거나 되살리지 않도록 명시적으로
    // 실패시킨다.
    {
      auto props = reader->GetTableProperties();
      if (props && (props->num_range_deletions > 0 ||
                    props->num_merge_operands > 0)) {
        fprintf(stderr,
                "[worker] OpenInputIterators: file=%llu range_dels=%llu "
                "merge_ops=%llu -- not supported by remote worker\n",
                (unsigned long long)f.file_number,
                (unsigned long long)props->num_range_deletions,
                (unsigned long long)props->num_merge_operands);
        return Status::NotSupported(
            "remote worker: input has range deletions or merge operands");
      }
    }

    InternalIterator* iter = reader->NewIterator(
        ro, nullptr, nullptr, false, TableReaderCaller::kCompaction);
    readers_out->push_back(reader.release());
    iters_out->push_back(iter);
  }
  return Status::OK();
}

// ── WriteOutputSSTs ───────────────────────────────────────────────
// v-citer (2026-11-01): MergingIterator 출력을 그대로 쓰지 않고 RocksDB의
// CompactionIterator로 감싸서 정상 컴팩션 의미론을 적용한다: 스냅샷에 안
// 걸린 옛 버전 제거, bottommost에서 삭제 표시 제거와 시퀀스 번호 정리.
// 이전 구현은 병합 결과를 전부 Add하는 정렬 병합이라 버전이 하나도 안
// 지워졌음. 출력 파일은 user key 경계에서만 자른다.
Status WorkerServer::WriteOutputSSTs(
    const CompactionSpec&           spec,
    const Options&                  opts,
    const ReadOptions&              ro,
    const ImmutableOptions&         iopts,
    InternalIterator*               merged_iter,
    CompactionResult*               result_out,
    std::vector<std::vector<char>>* out_bufs) {
  (void)ro;
  (void)iopts;

  // v-gpcut: grandparent 경계 기준 자르기 (RC_GP_CUT=0 이면 기존 크기 기준만)
  static const bool kGpCutEnv = [] {
    const char* e = std::getenv("RC_GP_CUT");
    bool v = !(e && e[0] == '0');
    fprintf(stderr, "[worker] output cut by grandparent boundary: %s (v-gpcut)\n", v ? "on" : "off");
    return v;
  }();
  const bool gp_on = kGpCutEnv && spec.output_level > 0 && !spec.bottommost &&
                     !spec.output.grandparents.empty() && spec.output.gp_target_file_size > 0 &&
                     spec.output.gp_max_file_size > 0;
  // 크기 상한: gp_on이면 RocksDB와 같은 max_output_file_size(보통 target의 2배), 아니면 기존 값
  const uint64_t target_size = gp_on ? spec.output.gp_max_file_size : spec.output.target_file_size;
  uint64_t fn_counter = (spec.first_output_file_number != 0)
                            ? spec.first_output_file_number
                            : 10000;
  const Comparator* ucmp = opts.comparator;

  // ---- v-gpcut: RocksDB CompactionOutputs grandparent 상태 이식 (출력 파일이 바뀌어도 index/gap/seen은 유지) ----
  std::vector<InternalKey> gp_lo, gp_hi;
  std::vector<uint64_t>    gp_sz;
  if (gp_on) {
    for (const auto& g : spec.output.grandparents) {
      gp_lo.emplace_back(); gp_lo.back().DecodeFrom(g.smallest_ikey);
      gp_hi.emplace_back(); gp_hi.back().DecodeFrom(g.largest_ikey);
      gp_sz.push_back(g.file_size);
    }
  }
  size_t   gp_index = 0, gp_switched = 0, gp_cut_cnt = 0, size_cut_cnt = 0;
  bool     gp_gap = true, gp_seen = false;
  uint64_t gp_overlap = 0;
  const uint64_t gp_target = spec.output.gp_target_file_size;
  const uint64_t gp_maxcb  = spec.output.gp_max_compaction_bytes;
  // sstableKeyCompare (db/compaction/compaction.cc) 와 같은 비교
  auto gp_cmp = [&](const Slice& a, const InternalKey& bk) -> int {
    const Slice b = bk.Encode();
    int c = ucmp->CompareWithoutTimestamp(ExtractUserKey(a), ExtractUserKey(b));
    if (c != 0) return c;
    const uint64_t af = ExtractInternalKeyFooter(a), bf = ExtractInternalKeyFooter(b);
    if (af == kRangeTombstoneSentinel) { if (bf != kRangeTombstoneSentinel) return -1; }
    else if (bf == kRangeTombstoneSentinel) { return 1; }
    return 0;
  };
  // GetCurrentKeyGrandparentOverlappedBytes
  auto gp_cur_overlap = [&](const Slice& ik) -> uint64_t {
    if (gp_gap || gp_index >= gp_lo.size()) return 0;
    uint64_t b = gp_sz[gp_index];
    for (int64_t i = static_cast<int64_t>(gp_index) - 1; i >= 0 && gp_cmp(ik, gp_hi[i]) == 0; i--) b += gp_sz[i];
    return b;
  };
  // UpdateGrandparentBoundaryInfo: 이번 key까지 넘은 grandparent 경계 수
  auto gp_update = [&](const Slice& ik) -> size_t {
    size_t crossed = 0;
    if (gp_lo.empty()) return 0;
    while (gp_index < gp_lo.size()) {
      if (gp_gap) {
        if (gp_cmp(ik, gp_lo[gp_index]) < 0) break;
        if (gp_seen) { crossed++; gp_overlap += gp_sz[gp_index]; gp_switched++; }
        gp_gap = false;
      } else {
        const int c = gp_cmp(ik, gp_hi[gp_index]);
        if (c < 0 || (c == 0 && (gp_index == gp_lo.size() - 1 || gp_cmp(ik, gp_lo[gp_index + 1]) < 0))) break;
        if (gp_seen) { crossed++; gp_switched++; }
        gp_gap = true;
        gp_index++;
      }
    }
    if (!gp_seen && !gp_gap) gp_overlap = gp_cur_overlap(ik);
    gp_seen = true;
    return crossed;
  };
  // ShouldStopBefore의 grandparent 규칙 3개 (크기 상한은 아래 target_size 비교가 담당)
  auto gp_should_cut = [&](uint64_t cur, size_t crossed, uint64_t prev_overlap) -> bool {
    if (crossed == 0) return false;
    if (gp_maxcb > 0 && gp_overlap + cur > gp_maxcb) return true;
    const size_t skippable = gp_gap ? 2 : 3;
    if (crossed >= skippable && gp_overlap - prev_overlap > gp_target / 8) return true;
    return cur >= ((gp_target + 99) / 100) * (50 + std::min<size_t>(gp_switched * 5, 40));
  };
  // 파일을 자른 직후 초기화 (AddToOutput: "reset grandparent information")
  auto gp_reset = [&](const Slice& next_ik) { gp_switched = 0; gp_overlap = gp_cur_overlap(next_ik); };

  // ---- CompactionIterator 구성 (CompactionJob과 같은 인자 규칙) ----
  std::vector<SequenceNumber> snaps = spec.snapshots.list;
  std::sort(snaps.begin(), snaps.end());
  const SequenceNumber earliest_snapshot =
      snaps.empty() ? kMaxSequenceNumber : snaps.front();
  SequenceNumber ewc_snapshot = spec.snapshots.earliest_write_conflict_snapshot;
  if (ewc_snapshot == 0) ewc_snapshot = kMaxSequenceNumber;

  int start_level = spec.output_level;
  for (const auto& f : spec.inputs) start_level = std::min(start_level, f.level);

  MergeHelper merge_helper(Env::Default(), ucmp,
                           /*user_merge_operator=*/nullptr,
                           /*compaction_filter=*/nullptr,
                           /*logger=*/nullptr,
                           /*assert_valid_internal_key=*/false,
                           /*latest_snapshot=*/snaps.empty() ? 0 : snaps.back(),
                           /*snapshot_checker=*/nullptr, start_level);

  InternalKeyComparator icmp(ucmp);
  // 범위 삭제가 있는 입력은 OpenInputIterators에서 이미 거부되므로 빈 aggregator.
  CompactionRangeDelAggregator range_del_agg(&icmp, snaps);
  static const std::atomic<bool> kNotCanceled{false};

  merged_iter->SeekToFirst();
  if (!merged_iter->status().ok()) return merged_iter->status();

  CompactionIterator c_iter(
      merged_iter, ucmp, &merge_helper,
      /*last_sequence=*/kMaxSequenceNumber, &snaps, earliest_snapshot,
      ewc_snapshot, /*job_snapshot=*/kMaxSequenceNumber,
      /*snapshot_checker=*/nullptr, Env::Default(),
      /*report_detailed_time=*/false, &range_del_agg,
      /*blob_file_builder=*/nullptr, /*allow_data_in_errors=*/false,
      /*enforce_single_del_contracts=*/true, kNotCanceled,
      std::make_unique<NullCompactionProxy>(spec.bottommost, start_level,
                                            spec.output_level),
      /*must_count_input_entries=*/false);
  c_iter.SeekToFirst();
  if (gp_on && c_iter.Valid()) gp_update(c_iter.key());  // v-gpcut: 첫 key도 RocksDB처럼 상태에 반영

  uint64_t    total_out_entries = 0;
  std::string last_user_key;

  while (c_iter.Valid()) {
    out_bufs->emplace_back();
    std::vector<char>& cur_buf = out_bufs->back();

    // MakeMemFileWriter: ntb_mem_file.h 헬퍼
    auto writer = MakeMemFileWriter(
        &cur_buf, "mem_out_" + std::to_string(fn_counter));

    // output용 Options에 factory 설정
    BlockBasedTableOptions bbto;
    Options local_opts = opts;
    local_opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
    ImmutableOptions local_iopts(local_opts);
    MutableCFOptions mopts(local_opts);
    ReadOptions  tbo_ro;
    WriteOptions tbo_wo;

    static InternalTblPropCollFactories empty_factories;
    TableBuilderOptions tbo(
        local_iopts,
        mopts,
        tbo_ro,
        tbo_wo,
        local_iopts.internal_comparator,
        &empty_factories,
        spec.output.compression,
        spec.output.compression_opts,
        /*column_family_id=*/0,
        /*column_family_name=*/"default",
        /*level=*/spec.output_level,
        /*newest_key_time=*/0,
        /*is_bottommost=*/spec.bottommost,
        TableFileCreationReason::kCompaction,
        /*oldest_key_time=*/0,
        /*file_creation_time=*/0,
        /*db_id=*/"",
        /*db_session_id=*/"",
        target_size,
        fn_counter);

    std::unique_ptr<TableBuilder> builder(
        local_opts.table_factory->NewTableBuilder(tbo, writer.get()));

    bool        has_entries = false;
    InternalKey smallest, largest;
    uint64_t    num_entries   = 0;
    uint64_t    num_deletions = 0;
    SequenceNumber min_seqno  = kMaxSequenceNumber;
    SequenceNumber max_seqno  = 0;

    while (c_iter.Valid()) {
      const Slice ikey = c_iter.key();
      const Slice val  = c_iter.value();

      ParsedInternalKey pikey;
      if (ParseInternalKey(ikey, &pikey, false).ok()) {
        if (pikey.type == kTypeDeletion ||
            pikey.type == kTypeSingleDeletion) num_deletions++;
        if (pikey.sequence < min_seqno) min_seqno = pikey.sequence;
        if (pikey.sequence > max_seqno) max_seqno = pikey.sequence;
      }

      if (!has_entries) { smallest.DecodeFrom(ikey); has_entries = true; }
      largest.DecodeFrom(ikey);
      builder->Add(ikey, val);
      num_entries++;

      const Slice uk = ExtractUserKey(ikey);
      last_user_key.assign(uk.data(), uk.size());
      c_iter.Next();

      // v-gpcut: 다음 key 기준으로 grandparent 상태 갱신 (RocksDB ShouldStopBefore와 같은 시점, key당 1번)
      const uint64_t gp_prev_overlap = gp_overlap;
      const size_t   gp_crossed = (gp_on && c_iter.Valid()) ? gp_update(c_iter.key()) : 0;
      // 목표 크기에 도달해도 같은 user key의 다른 버전 사이에서는 자르지 않는다.
      const bool next_diff_key = c_iter.Valid() &&
          ucmp->Compare(ExtractUserKey(c_iter.key()), Slice(last_user_key)) != 0;
      if (next_diff_key) {
        const uint64_t cur = builder->FileSize();
        const bool by_size = cur >= target_size;
        const bool by_gp   = !by_size && gp_on && gp_should_cut(cur, gp_crossed, gp_prev_overlap);
        if (by_size || by_gp) {
          if (gp_on) { gp_reset(c_iter.key()); if (by_gp) gp_cut_cnt++; else size_cut_cnt++; }
          break;
        }
      }
    }

    if (!c_iter.status().ok()) {
      builder->Abandon();
      return c_iter.status();
    }
    if (!has_entries) {
      builder->Abandon();
      out_bufs->pop_back();
      break;
    }

    Status s = builder->Finish();
    if (!s.ok()) return s;

    IOOptions io_opts;
    IOStatus ios = writer->Sync(io_opts, /*use_fsync=*/false);
    if (!ios.ok()) return ios;

    CompactionResult::OutputFile of;
    of.file_number    = fn_counter++;
    of.file_size      = static_cast<uint64_t>(cur_buf.size());
    of.smallest_ikey  = smallest.Encode().ToString();
    of.largest_ikey   = largest.Encode().ToString();
    of.smallest_seqno = (min_seqno == kMaxSequenceNumber) ? 0 : min_seqno;
    of.largest_seqno  = max_seqno;
    of.num_entries    = num_entries;
    of.num_deletions  = num_deletions;
    // v-crcdiag: 전송 직전 완성된 cur_buf의 체크섬을 계산해서 메타데이터에
    // 실어 보낸다. primary가 받은 뒤 같은 알고리즘(NtbDiagCrc32)으로
    // 재계산해서 비교한다.
    of.content_crc32 = NtbDiagCrc32(cur_buf.data(), cur_buf.size());
    fprintf(stderr,
            "[diag][crc] WriteOutputSSTs: file=%llu size=%zu crc32=%u\n",
            (unsigned long long)of.file_number, cur_buf.size(),
            of.content_crc32);

    total_out_entries += num_entries;
    result_out->output_files.push_back(std::move(of));
    result_out->bytes_written += cur_buf.size();
    if (RcStreamGen()) {  // v-stream: 만들자마자 송신 (writer를 먼저 닫아 기존과 같은 순서로)
      writer.reset();
      RcStreamEmit(spec.job_id, result_out->output_files.back().file_number, &cur_buf);
    }
  }

  if (!c_iter.status().ok()) return c_iter.status();
  if (gp_on) {  // v-gpcut
    fprintf(stderr, "[worker][gpcut] job=%llu level=%d grandparents=%zu out_files=%zu cut_by_gp=%zu cut_by_size=%zu\n",
            (unsigned long long)spec.job_id, spec.output_level, gp_lo.size(), out_bufs->size(), gp_cut_cnt, size_cut_cnt);
  }

  const CompactionIterationStats& st = c_iter.iter_stats();
  fprintf(stderr,
          "[worker][citer] job=%llu in_records=%llu out_records=%llu "
          "drop_hidden=%llu drop_obsolete=%llu out_files=%zu bottommost=%d "
          "snaps=%zu\n",
          (unsigned long long)spec.job_id,
          (unsigned long long)st.num_input_records,
          (unsigned long long)total_out_entries,
          (unsigned long long)st.num_record_drop_hidden,
          (unsigned long long)st.num_record_drop_obsolete,
          out_bufs->size(), spec.bottommost ? 1 : 0, snaps.size());

  return merged_iter->status();
}

// ── HandleJob ─────────────────────────────────────────────────────
Status WorkerServer::HandleJob(
    const CompactionSpec&                 spec,
    const std::vector<std::vector<char>>& sst_bufs,
    CompactionResult*                     result_out,
    std::vector<std::vector<char>>*       out_bufs) {
  auto t_compact_start = std::chrono::steady_clock::now();

  const Comparator* ucmp = FindComparator(spec.comparator_name);
  if (!ucmp)
    return Status::InvalidArgument(
        "unknown comparator: " + spec.comparator_name);

  Options          opts;
  opts.comparator = ucmp;
  opts.env        = Env::Default();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;  // v13: 캐시 키가 파일 고유 ID 기반이 아니라서
  // job 내 입력 파일들끼리 블록 캐시가 충돌할 수 있음 (job=23 1890번 파일
  // 키 역행 부패의 원인으로 추정). 단발성 순차 읽기라 캐시 자체가 불필요.
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ImmutableOptions iopts(opts);
  ReadOptions      ro;

  std::vector<InternalIterator*> iters;
  std::vector<TableReader*>      readers;

  Status s = OpenInputIterators(spec, opts, ro, iopts, sst_bufs,
                                &iters, &readers);
  if (!s.ok()) {
    for (auto* r : readers) delete r;
    for (auto* it : iters)  delete it;
    return s;
  }

  // ---- v-wsub (2026-11-09): 잡 하나를 user key 범위 K개로 나눠 K개 스레드로 컴팩션 ----
  // RocksDB subcompaction과 같은 방식: 범위마다 독립 입력 iterator + ClippingIterator [start, end)
  // + 독립 출력 상태(WriteOutputSSTs 1회). 결과는 범위 순서로 이어 붙이고
  // 출력 파일 번호는 단일 스레드와 같은 연속 번호로 다시 매긴다.
  //   RC_WSUB=K        (기본 1 = 기존 동작)
  //   RC_WSUB_MIN_MB=M (기본 64; 입력 합이 M MB 미만인 잡은 나누지 않음)
  static const int kWsub = [] {
    const char* e = std::getenv("RC_WSUB");
    int v = (e && *e) ? std::atoi(e) : 1;
    if (v < 1) v = 1;
    if (v > 64) v = 64;
    fprintf(stderr, "[worker] v-wsub: RC_WSUB=%d\n", v);
    return v;
  }();
  static const uint64_t kWsubMinBytes = [] {
    const char* e = std::getenv("RC_WSUB_MIN_MB");
    long v = (e && *e) ? std::atol(e) : 64;
    if (v < 0) v = 0;
    fprintf(stderr, "[worker] v-wsub: RC_WSUB_MIN_MB=%ld\n", v);
    return static_cast<uint64_t>(v) << 20;
  }();
  static const uint64_t kWsubMinPiece = [] {  // v-wsub-minpiece: 조각 최소 크기 (0 = job의 출력 SST 크기 자동)
    const char* e = std::getenv("RC_WSUB_MIN_PIECE_MB");
    long v = (e && *e) ? std::atol(e) : 0;
    if (v < 0) v = 0;
    fprintf(stderr, "[worker] v-wsub-minpiece: RC_WSUB_MIN_PIECE_MB=%ld (0=auto: job target file size)\n", v);
    return static_cast<uint64_t>(v) << 20;
  }();

  result_out->job_id     = spec.job_id;
  result_out->success    = true;
  result_out->bytes_read = 0;
  for (const auto& buf : sst_bufs) result_out->bytes_read += buf.size();

  // 경계 후보 = 입력 파일들의 첫 user key. 출력 레벨 입력 파일이 있으면 그것만 쓴다
  // (서로 안 겹치고 크기가 비슷해서 균등 분할에 적합; 랜덤 키 L0->L1에서는 L1 파일들).
  std::vector<std::string> wsub_bounds;
  static const bool kWsubL0Only = [] {  // v-wsub-l0: 1이면 출력 L1(=L0->L1) job만 분할
    const char* e = std::getenv("RC_WSUB_L0ONLY"); bool v = e && *e == '1';
    fprintf(stderr, "[worker] v-wsub-l0: RC_WSUB_L0ONLY=%d\n", v ? 1 : 0); return v; }();
  if (kWsub > 1 && result_out->bytes_read >= kWsubMinBytes &&
      (!kWsubL0Only || spec.output_level == 1)) {  // v-wsub-l0
    // v-wsub2: 입력 파일 전체의 index key anchor(user key, 그 구간 바이트)를 모아
    // 바이트 누적 기준으로 K등분 (RocksDB GenSubcompactionBoundaries와 같은 방식).
    // v-wsub1은 출력 레벨 파일 시작 키만 써서 범위가 크게 치우쳤음 (첫 범위가 대부분 차지).
    struct WsubAnc { std::string k; uint64_t sz; };
    std::vector<WsubAnc> anc;
    uint64_t anc_total = 0;
    uint64_t wsub_piece_min = 0;  // v-wsub-minpiece
    for (auto* r : readers) {
      std::vector<TableReader::Anchor> av;
      Status as = r->ApproximateKeyAnchors(ro, av);
      if (!as.ok()) continue;
      for (auto& x : av) {
        anc_total += static_cast<uint64_t>(x.range_size);
        anc.push_back({std::move(x.user_key), static_cast<uint64_t>(x.range_size)});
      }
    }
    std::sort(anc.begin(), anc.end(),
              [&](const WsubAnc& x, const WsubAnc& y) { return ucmp->Compare(x.k, y.k) < 0; });
    if (anc_total > 0 && anc.size() > 1) {
      // v-wsub-minpiece: RocksDB GenSubcompactionBoundaries와 같게 조각 크기 >= 출력 SST 크기
      const uint64_t piece_min = kWsubMinPiece > 0 ? kWsubMinPiece
          : (spec.output.gp_target_file_size > 0 ? spec.output.gp_target_file_size : spec.output.target_file_size);
      wsub_piece_min = piece_min;
      const uint64_t step = std::max<uint64_t>(anc_total / static_cast<uint64_t>(kWsub), piece_min);
      uint64_t acc = 0, next = step;
      for (size_t i = 0; i + 1 < anc.size(); i++) {
        acc += anc[i].sz;
        if (acc >= next) {
          if (wsub_bounds.empty() || ucmp->Compare(wsub_bounds.back(), anc[i].k) < 0)
            wsub_bounds.push_back(anc[i].k);
          while (next <= acc) next += step;
          if (wsub_bounds.size() + 1 >= static_cast<size_t>(kWsub)) break;
        }
      }
    }
    fprintf(stderr, "[worker][wsub2] job=%llu anchors=%zu bounds=%zu pieces=%zu in_mb=%.1f piece_min_mb=%.1f\n",  // v-wsub-minpiece
            (unsigned long long)spec.job_id, anc.size(), wsub_bounds.size(), wsub_bounds.size() + 1,
            anc_total / 1048576.0, wsub_piece_min / 1048576.0);
  }

  if (wsub_bounds.empty()) {
    // 기존 경로 (단일 스레드)
    Arena             arena;
    InternalIterator* merged = NewMergingIterator(
        &iopts.internal_comparator,
        iters.data(), static_cast<int>(iters.size()), &arena);
    s = WriteOutputSSTs(spec, opts, ro, iopts, merged, result_out, out_bufs);
  } else {
    const size_t   K       = wsub_bounds.size() + 1;
    const uint64_t fn_base = (spec.first_output_file_number != 0)
                                 ? spec.first_output_file_number : 10000;
    std::vector<CompactionResult>               sub_res(K);
    std::vector<std::vector<std::vector<char>>> sub_bufs(K);
    std::vector<Status>                         sub_st(K);
    std::vector<uint64_t>                       sub_ms(K, 0);

    auto run_sub = [&](size_t k) {
      const auto t0 = std::chrono::steady_clock::now();
      CompactionSpec sspec = spec;
      // 범위끼리 임시 파일 번호가 겹치지 않게만 띄워 둔다 (아래에서 연속 번호로 다시 매김)
      sspec.first_output_file_number = fn_base + static_cast<uint64_t>(k) * 100000;
      std::vector<InternalIterator*> it_k;
      std::vector<TableReader*>      rd_k;
      Status st = OpenInputIterators(sspec, opts, ro, iopts, sst_bufs, &it_k, &rd_k);
      if (st.ok()) {
        Arena             arena_k;  // merged_k는 Arena 할당: 기존 경로처럼 delete 하지 않음
        InternalIterator* merged_k = NewMergingIterator(
            &iopts.internal_comparator,
            it_k.data(), static_cast<int>(it_k.size()), &arena_k);
        // (user key, kMaxSequenceNumber)는 그 user key의 모든 버전보다 앞이므로
        // [start, end)가 user key 경계로 잘린다 (compaction_job.cc 서브컴팩션과 같은 방식).
        InternalKey  lo_ik, hi_ik;
        Slice        lo_s, hi_s;
        const Slice* lo = nullptr;
        const Slice* hi = nullptr;
        if (k > 0) {
          lo_ik.Set(wsub_bounds[k - 1], kMaxSequenceNumber, kValueTypeForSeek);
          lo_s = lo_ik.Encode();
          lo = &lo_s;
        }
        if (k + 1 < K) {
          hi_ik.Set(wsub_bounds[k], kMaxSequenceNumber, kValueTypeForSeek);
          hi_s = hi_ik.Encode();
          hi = &hi_s;
        }
        ClippingIterator clip(merged_k, lo, hi, &iopts.internal_comparator);
        sub_res[k].job_id  = spec.job_id;
        sub_res[k].success = true;
        st = WriteOutputSSTs(sspec, opts, ro, iopts, &clip, &sub_res[k], &sub_bufs[k]);
        if (st.ok() && sub_bufs[k].size() != sub_res[k].output_files.size())
          st = Status::Corruption("v-wsub: out_bufs/output_files size mismatch");
      }
      for (auto* it : it_k) delete it;
      for (auto* r : rd_k)  delete r;
      sub_st[k] = st;
      sub_ms[k] = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count());
    };

    std::vector<std::thread> ths;
    ths.reserve(K - 1);
    for (size_t k = 1; k < K; k++) ths.emplace_back(run_sub, k);
    run_sub(0);
    for (auto& t : ths) t.join();

    for (size_t k = 0; k < K && s.ok(); k++) {
      if (!sub_st[k].ok()) s = sub_st[k];
    }
    if (s.ok()) {
      uint64_t fn = fn_base;
      for (size_t k = 0; k < K; k++) {
        for (size_t j = 0; j < sub_res[k].output_files.size(); j++) {
          CompactionResult::OutputFile of = std::move(sub_res[k].output_files[j]);
          if (RcStreamGen()) fn++; else of.file_number = fn++;  // v-stream: 이미 보낸 번호 유지
          result_out->output_files.push_back(std::move(of));
          out_bufs->push_back(std::move(sub_bufs[k][j]));
        }
        result_out->bytes_written += sub_res[k].bytes_written;
      }
    }
    std::string ms_str;
    for (size_t k = 0; k < K; k++) {
      if (k) ms_str += ",";
      ms_str += std::to_string(sub_ms[k]) + "/" + std::to_string(sub_res[k].output_files.size());
    }
    fprintf(stderr,
            "[worker][wsub] job=%llu out_level=%d K=%zu in_mb=%.1f out_files=%zu sub_ms/files=%s ok=%d\n",
            (unsigned long long)spec.job_id, spec.output_level, K,
            result_out->bytes_read / 1048576.0, result_out->output_files.size(),
            ms_str.c_str(), s.ok() ? 1 : 0);
  }
  result_out->elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t_compact_start).count();

  // merged는 Arena 할당이므로 delete 하지 않음 - Arena 소멸 시 자동 해제
  for (auto* it : iters)  delete it;
  for (auto* r : readers) delete r;

  if (!s.ok()) {
    result_out->success   = false;
    result_out->error_msg = s.ToString();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
