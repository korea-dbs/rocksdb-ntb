// nfs_compaction_worker: the worker side of the file-based baselines (NFS and
// GFS2); see tools/nfs_compaction_service.h for the job directory protocol.
//
//   nfs_compaction_worker <shared_dir> <worker_name> <threads> <primary_db>
//                         [secondary_root]
//
// A poll thread picks up new jobs under <shared_dir>/rcmp_jobs/<worker_name>;
// <threads> worker threads each run DB::OpenAndCompact() on <primary_db> as a
// secondary instance under <secondary_root>/<job id> (default
// <shared_dir>/rcmp_work), write the result, and mark the job done or failed.
//
// Environment: RC_WSPLIT, RC_WSPLIT_MIN_MB, RC_WSPLIT_L0ONLY split one job
// into key-range subcompactions (see db/db_impl/db_impl_secondary.cc and
// db/compaction/compaction_service_job.cc).
//
// Per job it prints [RCMP_LATENCY] (CPU, wall time, SST read/write time and
// bytes, metadata time; "open_*" fields cover only the DB open phase), and
// the DB opens print [RCMP_IO], [RCMP_CPUPH] and [RCMP_IOPH].
#include <sys/resource.h>

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

#include "nfs_io_timing_fs.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"

namespace ROCKSDB_NAMESPACE {
extern void (*g_rcmp_phase_hook)();  // db/db_impl/db_impl_secondary.cc
}

namespace fs = std::filesystem;
using namespace ROCKSDB_NAMESPACE;

namespace {

// I/O counters of this thread at the end of the DB open phase.
thread_local nfsio::IoAcc rc_ph;
void RcPhaseHook() { rc_ph = nfsio::SnapshotTls(); }

std::atomic<long long> g_total_cpu_ms{0};
std::atomic<long long> g_total_wall_ms{0};
std::atomic<long long> g_total_io_ms{0};
std::atomic<long long> g_total_meta_ms{0};
std::atomic<long long> g_jobs_completed{0};
std::atomic<int> g_inflight{0};

struct Job {
  std::string id;
  fs::path meta;
};

std::queue<Job> g_queue;
std::set<std::string> g_seen;
std::mutex g_mu;
std::condition_variable g_cv;
std::atomic<bool> g_stop{false};
constexpr size_t kMaxQueue = 1024;

bool ReadFile(const fs::path& p, std::string* out) {
  std::ifstream in(p, std::ios::binary);
  if (!in.good()) return false;
  *out = std::string(std::istreambuf_iterator<char>(in),
                     std::istreambuf_iterator<char>());
  return true;
}

bool WriteFileAtomic(const fs::path& p, const std::string& s) {
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
  if (ec) {
    fs::remove(tmp, ec);
    return false;
  }
  return true;
}

void SafeRemove(const fs::path& p) {
  std::error_code ec;
  fs::remove(p, ec);
}

// Claims a job (writes "running") once its req and meta exist and it has no
// running/done/fail marker yet.
void PollThread(const std::string& shared, const std::string& worker) {
  fs::path worker_root = fs::path(shared) / "rcmp_jobs" / worker;
  std::cout << "[poll] watching " << worker_root << std::endl;

  while (!g_stop.load()) {
    try {
      std::error_code ec;
      if (!fs::exists(worker_root, ec)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      for (const auto& job_entry : fs::directory_iterator(worker_root, ec)) {
        if (!job_entry.is_directory()) continue;
        std::string job_id = job_entry.path().filename().string();
        {
          std::lock_guard<std::mutex> lk(g_mu);
          if (g_seen.count(job_id)) continue;
          if (g_queue.size() >= kMaxQueue) continue;
        }

        fs::path job_dir = job_entry.path();
        fs::path run_f = job_dir / "running";
        std::error_code ec2;
        if (!fs::exists(job_dir / "req", ec2)) continue;
        if (!fs::exists(job_dir / "meta", ec2)) continue;
        if (fs::exists(run_f, ec2)) continue;
        if (fs::exists(job_dir / "done", ec2)) continue;
        if (fs::exists(job_dir / "fail", ec2)) continue;
        if (!WriteFileAtomic(run_f, "running\n")) continue;

        {
          std::lock_guard<std::mutex> lk(g_mu);
          if (g_seen.count(job_id)) {
            SafeRemove(run_f);
            continue;
          }
          g_seen.insert(job_id);
          g_queue.push({job_id, job_dir / "meta"});
        }
        g_cv.notify_one();
        std::cout << "[poll] queued job=" << job_id
                  << " queue_depth=" << g_queue.size()
                  << " inflight=" << g_inflight.load() << std::endl;
      }
    } catch (...) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void StatsThread() {
  long long last_completed = 0;
  auto last_time = std::chrono::steady_clock::now();
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    long long now_completed = g_jobs_completed.load();
    auto now_time = std::chrono::steady_clock::now();
    double elapsed_sec =
        std::chrono::duration<double>(now_time - last_time).count();
    std::cout << "[STATS]"
              << " throughput=" << (now_completed - last_completed) / elapsed_sec
              << " jobs/sec"
              << " inflight=" << g_inflight.load()
              << " completed=" << now_completed
              << " cpu_ms_sum=" << g_total_cpu_ms.load()
              << " wall_ms_sum=" << g_total_wall_ms.load()
              << " io_ms_sum=" << g_total_io_ms.load()
              << " meta_ms_sum=" << g_total_meta_ms.load() << std::endl;
    last_completed = now_completed;
    last_time = now_time;
  }
}

void WorkerThread(const std::string& shared, const std::string& worker,
                  const std::string& primary_db,
                  const std::string& secondary_root, int tid) {
  fs::path worker_root = fs::path(shared) / "rcmp_jobs" / worker;
  static std::shared_ptr<Env> timing_env(NewCompositeEnv(
      std::make_shared<nfsio::TimingFS>(FileSystem::Default())));

  while (!g_stop.load()) {
    Job job;
    {
      std::unique_lock<std::mutex> lk(g_mu);
      g_cv.wait(lk, [] { return g_stop.load() || !g_queue.empty(); });
      if (g_stop.load() && g_queue.empty()) return;
      job = g_queue.front();
      g_queue.pop();
    }

    g_inflight.fetch_add(1, std::memory_order_relaxed);
    std::cout << "[worker " << tid << "] start job=" << job.id
              << " inflight=" << g_inflight.load() << std::endl;

    fs::path job_dir = worker_root / job.id;
    fs::path result_p = job_dir / "result";
    fs::path done_p = job_dir / "done";
    fs::path fail_p = job_dir / "fail";

    struct rusage ru_start;
    getrusage(RUSAGE_THREAD, &ru_start);

    std::string meta;
    long meta_ts = 0;
    if (!ReadFile(job.meta, &meta)) {
      std::cerr << "[worker " << tid << "] meta read fail job=" << job.id
                << std::endl;
      WriteFileAtomic(fail_p, "meta read failed");
      g_inflight.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }
    // The primary prefixes the input with "TS:<ms>\n" (its write time).
    if (meta.rfind("TS:", 0) == 0) {
      size_t pos = meta.find('\n');
      if (pos != std::string::npos) {
        meta_ts = std::stol(meta.substr(3, pos - 3));
        meta = meta.substr(pos + 1);
      }
    }
    long meta_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count() -
                   meta_ts;

    fs::path secondary = fs::path(secondary_root) / job.id;
    std::error_code ec;
    fs::remove_all(secondary, ec);
    fs::create_directories(secondary, ec);

    CompactionServiceOptionsOverride opts;
    BlockBasedTableOptions table_opts;
    opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
    opts.comparator = BytewiseComparator();
    opts.env = timing_env.get();
    nfsio::ResetTls();
    rc_ph = nfsio::IoAcc();
    g_rcmp_phase_hook = &RcPhaseHook;

    auto t_compact_start = std::chrono::steady_clock::now();
    std::string output;
    Status s = DB::OpenAndCompact(primary_db, secondary.string(), meta,
                                  &output, opts);
    auto t_compact_end = std::chrono::steady_clock::now();
    nfsio::IoAcc io_snap = nfsio::SnapshotTls();

    struct rusage ru_end;
    getrusage(RUSAGE_THREAD, &ru_end);
    long wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       t_compact_end - t_compact_start)
                       .count();
    long cpu_us =
        (ru_end.ru_utime.tv_sec - ru_start.ru_utime.tv_sec) * 1000000L +
        (ru_end.ru_utime.tv_usec - ru_start.ru_utime.tv_usec) +
        (ru_end.ru_stime.tv_sec - ru_start.ru_stime.tv_sec) * 1000000L +
        (ru_end.ru_stime.tv_usec - ru_start.ru_stime.tv_usec);
    long cpu_ms = cpu_us / 1000;
    long io_ms = wall_ms - cpu_ms;
    if (io_ms < 0) io_ms = 0;

    g_total_cpu_ms.fetch_add(cpu_ms, std::memory_order_relaxed);
    g_total_wall_ms.fetch_add(wall_ms, std::memory_order_relaxed);
    g_total_io_ms.fetch_add(io_ms, std::memory_order_relaxed);
    g_total_meta_ms.fetch_add(meta_ms, std::memory_order_relaxed);

    if (!s.ok()) {
      WriteFileAtomic(fail_p, s.ToString());
      std::cerr << "[worker " << tid << "] FAIL job=" << job.id << " : "
                << s.ToString() << std::endl;
    } else {
      WriteFileAtomic(result_p, output);
      WriteFileAtomic(done_p, "ok\n");
    }

    g_jobs_completed.fetch_add(1, std::memory_order_relaxed);
    g_inflight.fetch_sub(1, std::memory_order_relaxed);

    std::cout << "[RCMP_LATENCY] job=" << job.id << " cpu_ms=" << cpu_ms
              << " wall_ms=" << wall_ms << " io_ms=" << io_ms
              << " meta_ms=" << meta_ms
              << " open_sst_read_ms=" << (rc_ph.sst_read_ns / 1e6)
              << " open_sst_read_mb=" << (rc_ph.sst_read_bytes / 1048576.0)
              << " open_sst_read_ops=" << rc_ph.sst_read_ops
              << " in_read_ms=" << (io_snap.sst_read_ns / 1000000)
              << " in_read_mb=" << (io_snap.sst_read_bytes / 1048576)
              << " in_read_ops=" << io_snap.sst_read_ops
              << " out_write_ms=" << (io_snap.sst_write_ns / 1000000)
              << " out_sync_ms=" << (io_snap.sst_sync_ns / 1000000)
              << " out_write_mb=" << (io_snap.sst_write_bytes / 1048576)
              << " other_io_ms=" << (io_snap.other_ns / 1000000)
              << " m_stat_open_ms=" << (rc_ph.m_stat_ns / 1000000)
              << " m_stat_open_n=" << rc_ph.m_stat_n
              << " m_list_open_ms=" << (rc_ph.m_list_ns / 1000000)
              << " m_list_open_n=" << rc_ph.m_list_n
              << " m_dir_open_ms=" << (rc_ph.m_dir_ns / 1000000)
              << " m_dir_open_n=" << rc_ph.m_dir_n
              << " m_stat_ms=" << (io_snap.m_stat_ns / 1000000)
              << " m_stat_n=" << io_snap.m_stat_n
              << " m_list_ms=" << (io_snap.m_list_ns / 1000000)
              << " m_list_n=" << io_snap.m_list_n
              << " m_dir_ms=" << (io_snap.m_dir_ns / 1000000)
              << " m_dir_n=" << io_snap.m_dir_n
              << " inflight=" << g_inflight.load() << std::endl;
    std::cout << "[RCMP_TOTAL]"
              << " cpu_ms_sum=" << g_total_cpu_ms.load()
              << " wall_ms_sum=" << g_total_wall_ms.load()
              << " io_ms_sum=" << g_total_io_ms.load()
              << " meta_ms_sum=" << g_total_meta_ms.load()
              << " completed=" << g_jobs_completed.load() << std::endl;
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 5) {
    std::cerr << "usage: nfs_compaction_worker <shared_dir> <worker_name> "
                 "<threads> <primary_db> [secondary_root]\n";
    return 1;
  }
  std::string shared = argv[1];
  std::string worker = argv[2];
  int threads = std::stoi(argv[3]);
  std::string primary_db = argv[4];
  std::string secondary_root = argc > 5 ? argv[5] : shared + "/rcmp_work";

  std::error_code ec;
  fs::create_directories(secondary_root, ec);
  fs::create_directories(fs::path(shared) / "rcmp_jobs" / worker, ec);
  std::cout << "[main] shared=" << shared << " worker=" << worker
            << " threads=" << threads << " primary_db=" << primary_db
            << " secondary_root=" << secondary_root << std::endl;

  std::thread poll(PollThread, shared, worker);
  std::thread stats(StatsThread);
  std::vector<std::thread> ws;
  ws.reserve(threads);
  for (int i = 0; i < threads; i++)
    ws.emplace_back(WorkerThread, shared, worker, primary_db, secondary_root,
                    i);

  poll.join();
  stats.join();
  for (auto& t : ws) t.join();
  return 0;
}
