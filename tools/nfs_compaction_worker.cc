// tools/nfs_compaction_worker.cc
//
// 잘 동작하던 원본 코드 기반 + 잡별 디렉토리 구조
//
// 원본과 달라진 부분만:
//   PollThread(): 서브디렉토리 순회 → <job_id>/req 감지
//   WorkerThread(): 파일 경로가 job_dir/ 안으로 변경
//   CleanupThread(): <job_id>/installed 감지 → job_dir 통째로 삭제
//
// Usage:
//   nfs_compaction_worker <shared_dir> <worker_name> <threads> <primary_db>
//

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"

namespace fs = std::filesystem;
using namespace ROCKSDB_NAMESPACE;

std::atomic<long long> g_total_sst_read_us{0};

static std::atomic<long long> g_total_cpu_ms{0};
static std::atomic<long long> g_total_meta_ms{0};

// Job 구조체: job_dir 포함
struct Job {
  std::string id;
  fs::path    job_dir;  // <shared>/rcmp_jobs/<worker>/<job_id>/
};

static std::queue<Job>         g_queue;
static std::set<std::string>   g_inflight;
static std::mutex              g_mu;
static std::condition_variable g_cv;
static std::atomic<bool>       g_stop{false};
static const size_t            MAX_QUEUE = 1024;

// ----------------------------------------------------------------------------
// 파일 헬퍼 (원본과 동일)
// ----------------------------------------------------------------------------
static bool ReadFile(const fs::path& p, std::string* out) {
  std::ifstream in(p, std::ios::binary);
  if (!in.good()) return false;
  *out = std::string(std::istreambuf_iterator<char>(in),
                     std::istreambuf_iterator<char>());
  return true;
}

static bool WriteFileAtomic(const fs::path& p, const std::string& s) {
  fs::path tmp = p;
  tmp += ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out.good()) return false;
    out.write(s.data(), static_cast<std::streamsize>(s.size()));
    out.flush();
    if (!out.good()) return false;
  }
  std::error_code ec;
  fs::rename(tmp, p, ec);
  if (ec) { fs::remove(tmp, ec); return false; }
  return true;
}

static void SafeRemove(const fs::path& p) {
  std::error_code ec; fs::remove(p, ec);
}

static void SafeRemoveAll(const fs::path& p) {
  std::error_code ec; fs::remove_all(p, ec);
}

// ----------------------------------------------------------------------------
// PollThread
// 원본: dir/*.req 파일 스캔
// 변경: dir/ 아래 서브디렉토리 순회 → <job_id>/req 확인
// ----------------------------------------------------------------------------
static void PollThread(const std::string& shared, const std::string& worker) {
  fs::path worker_root = fs::path(shared) / "rcmp_jobs" / worker;
  std::cout << "[poll] watching " << worker_root << std::endl;

  while (!g_stop.load()) {
    try {
      std::error_code ec;
      if (!fs::exists(worker_root, ec)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }

      for (const auto& entry : fs::directory_iterator(worker_root, ec)) {
        if (!entry.is_directory(ec)) continue;

        std::string job_id  = entry.path().filename().string();
        fs::path    job_dir = entry.path();
        fs::path    req     = job_dir / "req";
        fs::path    meta    = job_dir / "meta";

        if (!fs::exists(req, ec) || !fs::exists(meta, ec)) continue;

        // claim: req → running (원본과 동일한 atomic rename)
        fs::path running = job_dir / "running";
        std::error_code rec;
        fs::rename(req, running, rec);
        if (rec) continue;

        {
          std::lock_guard<std::mutex> lk(g_mu);
          if (g_inflight.count(job_id)) continue;
          if (g_queue.size() > MAX_QUEUE) continue;
          g_queue.push({job_id, job_dir});
          g_inflight.insert(job_id);
        }
        g_cv.notify_one();
        std::cout << "[poll] queued job " << job_id << std::endl;
      }
    } catch (...) {}

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

// ----------------------------------------------------------------------------
// CleanupThread
// 원본: dir/<id>.installed 감지 → 개별 파일 삭제
// 변경: dir/<id>/installed 감지 → job_dir 통째로 삭제
// ----------------------------------------------------------------------------
static void CleanupThread(const std::string& shared,
                          const std::string& worker,
                          const std::string& secondary_root) {
  fs::path worker_root = fs::path(shared) / "rcmp_jobs" / worker;

  while (!g_stop.load()) {
    try {
      std::error_code ec;
      if (!fs::exists(worker_root, ec)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        continue;
      }

      for (const auto& entry : fs::directory_iterator(worker_root, ec)) {
        if (!entry.is_directory(ec)) continue;

        fs::path job_dir   = entry.path();
        fs::path installed = job_dir / "installed";
        if (!fs::exists(installed, ec)) continue;

        std::string job_id = entry.path().filename().string();
        std::cout << "[cleanup] gc job " << job_id << std::endl;

        // job_dir 통째로 삭제
        SafeRemoveAll(job_dir);
        // SST 임시 디렉토리도 삭제
        SafeRemoveAll(fs::path(secondary_root) / job_id);

        {
          std::lock_guard<std::mutex> lk(g_mu);
          g_inflight.erase(job_id);
        }
      }
    } catch (...) {}

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

// ----------------------------------------------------------------------------
// WorkerThread
// 원본과 동일한 로직, 파일 경로만 job_dir/ 안으로 변경
// ----------------------------------------------------------------------------
static void WorkerThread(const std::string& shared,
                         const std::string& worker,
                         const std::string& primary_db,
                         const std::string& secondary_root,
                         int tid) {
  while (!g_stop.load()) {
    Job job;
    {
      std::unique_lock<std::mutex> lk(g_mu);
      g_cv.wait(lk, [] { return g_stop.load() || !g_queue.empty(); });
      if (g_stop.load() && g_queue.empty()) return;
      job = g_queue.front();
      g_queue.pop();
    }

    std::cout << "[worker " << tid << "] start " << job.id << std::endl;

    struct rusage ru_start;
    getrusage(RUSAGE_THREAD, &ru_start);

    std::string meta;
    long meta_ts = 0;
    if (!ReadFile(job.job_dir / "meta", &meta)) {
      std::cerr << "[meta fail] " << job.id << std::endl;
      WriteFileAtomic(job.job_dir / "fail", "meta read failed");
      std::lock_guard<std::mutex> lk(g_mu);
      g_inflight.erase(job.id);
      continue;
    }

    if (meta.rfind("TS:", 0) == 0) {
      size_t pos = meta.find('\n');
      if (pos != std::string::npos) {
        meta_ts = std::stol(meta.substr(3, pos - 3));
        meta    = meta.substr(pos + 1);
      }
    }

    auto now = std::chrono::system_clock::now();
    long read_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now.time_since_epoch()).count();
    long meta_ms = read_ts - meta_ts;

    fs::path secondary = fs::path(secondary_root) / job.id;
    std::error_code ec;
    fs::remove_all(secondary, ec);
    fs::create_directories(secondary, ec);

    CompactionServiceOptionsOverride opts;
    BlockBasedTableOptions table_opts;
    opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
    opts.comparator    = BytewiseComparator();

    std::string output;
    Status s = DB::OpenAndCompact(
        primary_db, secondary.string(), meta, &output, opts);

    struct rusage ru_end;
    getrusage(RUSAGE_THREAD, &ru_end);

    long cpu_us =
        (ru_end.ru_utime.tv_sec  - ru_start.ru_utime.tv_sec)  * 1000000L
      + (ru_end.ru_utime.tv_usec - ru_start.ru_utime.tv_usec)
      + (ru_end.ru_stime.tv_sec  - ru_start.ru_stime.tv_sec)  * 1000000L
      + (ru_end.ru_stime.tv_usec - ru_start.ru_stime.tv_usec);
    long cpu_ms = cpu_us / 1000;

    g_total_cpu_ms.fetch_add(cpu_ms,   std::memory_order_relaxed);
    g_total_meta_ms.fetch_add(meta_ms, std::memory_order_relaxed);

    // result 먼저, done 나중 (원본과 동일한 순서)
    if (!s.ok()) {
      WriteFileAtomic(job.job_dir / "fail", s.ToString());
      SafeRemove(job.job_dir / "running");
      std::cerr << "[worker " << tid << "] FAIL " << job.id
                << " : " << s.ToString() << std::endl;
    } else {
      WriteFileAtomic(job.job_dir / "result", output);
      WriteFileAtomic(job.job_dir / "done", "ok\n");
      SafeRemove(job.job_dir / "running");
    }

    std::cout << "[RCMP_LATENCY] job=" << job.id
              << " cpu_ms=" << cpu_ms
              << " meta_ms=" << meta_ms << std::endl;
    std::cout << "[RCMP_TOTAL] cpu_ms_sum=" << g_total_cpu_ms.load()
              << " meta_ms_sum=" << g_total_meta_ms.load() << std::endl;
    std::cout << "[SST_READ_TOTAL_MS] "
              << g_total_sst_read_us.load() / 1000 << std::endl;

    {
      std::lock_guard<std::mutex> lk(g_mu);
      g_inflight.erase(job.id);
    }
  }
}

// ----------------------------------------------------------------------------
// main (원본과 동일)
// ----------------------------------------------------------------------------
int main(int argc, char** argv) {
  if (argc < 5) {
    std::cerr << "usage: nfs_compaction_worker <shared_dir> <worker_name> "
                 "<threads> <primary_db>\n"
                 "  e.g. nfs_compaction_worker /mnt/shared_nvme srv4 4 "
                 "/mnt/shared_nvme/db_bench\n";
    return 1;
  }

  std::string shared     = argv[1];
  std::string worker     = argv[2];
  int         threads    = std::stoi(argv[3]);
  std::string primary_db = argv[4];
  std::string secondary_root = shared + "/rcmp_work";

  std::error_code ec;
  fs::create_directories(secondary_root, ec);
  fs::create_directories(fs::path(shared) / "rcmp_jobs" / worker, ec);

  std::cout << "[main] shared="      << shared
            << " worker="            << worker
            << " threads="           << threads
            << " primary_db="        << primary_db
            << " secondary_root="    << secondary_root << std::endl;

  std::thread poll(PollThread, shared, worker);
  std::thread cleanup(CleanupThread, shared, worker, secondary_root);

  std::vector<std::thread> ws;
  ws.reserve(threads);
  for (int i = 0; i < threads; i++)
    ws.emplace_back(WorkerThread, shared, worker, primary_db,
                    secondary_root, i);

  poll.join();
  cleanup.join();
  for (auto& t : ws) t.join();
  return 0;
}

