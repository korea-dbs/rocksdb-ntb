// worker_main: the NTB-Native compaction worker process.
//
//   worker_main --primary_node=<id> [options]
//
// Options (both --name=value and --name value are accepted):
//   --primary_node=N                 SISCI node id of the primary (required)
//   --threads=N                      compaction threads (default 16)
//   --subcompaction=none|l0|all      split jobs into key ranges compacted in
//                                    parallel: never, only L0->L1 jobs, or
//                                    every job (default none)
//   --max_subcompactions=N           ranges per job (default 8)
//   --subcompaction_min_mb=N         do not split jobs with less input
//                                    (default 64)
//   --subcompaction_min_range_mb=N   minimum input per range; 0 = the job's
//                                    target output file size (default 0)
//   --input_pool_mb=N                input buffers kept for reuse (default
//                                    32768; 0 disables the pool)
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "worker_server.h"

using namespace ROCKSDB_NAMESPACE;

static WorkerServer* g_server = nullptr;

static void OnSignal(int) {
  if (g_server) g_server->RequestStop();  // async-signal-safe
}

static void Usage(const char* prog) {
  fprintf(stderr,
          "usage: %s --primary_node=N [--threads=16] "
          "[--subcompaction=none|l0|all] [--max_subcompactions=8] "
          "[--subcompaction_min_mb=64] [--subcompaction_min_range_mb=0] "
          "[--input_pool_mb=32768]\n",
          prog);
}

int main(int argc, char** argv) {
  WorkerOptions opts;
  bool have_node = false;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      Usage(argv[0]);
      return 0;
    }
    if (arg.rfind("--", 0) != 0) {
      Usage(argv[0]);
      return 1;
    }
    std::string name, value;
    const size_t eq = arg.find('=');
    if (eq != std::string::npos) {
      name = arg.substr(2, eq - 2);
      value = arg.substr(eq + 1);
    } else if (i + 1 < argc) {
      name = arg.substr(2);
      value = argv[++i];
    } else {
      Usage(argv[0]);
      return 1;
    }
    for (auto& c : name)
      if (c == '-') c = '_';

    if (name == "primary_node") {
      opts.primary_node_id = static_cast<uint32_t>(std::atoi(value.c_str()));
      have_node = true;
    } else if (name == "threads") {
      opts.compaction_threads = std::atoi(value.c_str());
    } else if (name == "subcompaction") {
      if (value == "none") {
        opts.subcompaction = WorkerSubcompaction::kNone;
      } else if (value == "l0") {
        opts.subcompaction = WorkerSubcompaction::kL0;
      } else if (value == "all") {
        opts.subcompaction = WorkerSubcompaction::kAll;
      } else {
        Usage(argv[0]);
        return 1;
      }
    } else if (name == "max_subcompactions") {
      opts.max_subcompactions = std::atoi(value.c_str());
    } else if (name == "subcompaction_min_mb") {
      opts.subcompaction_min_input_bytes = std::strtoull(value.c_str(), nullptr, 10) << 20;
    } else if (name == "subcompaction_min_range_mb") {
      opts.subcompaction_min_range_bytes = std::strtoull(value.c_str(), nullptr, 10) << 20;
    } else if (name == "input_pool_mb") {
      opts.input_pool_bytes = std::strtoull(value.c_str(), nullptr, 10) << 20;
    } else {
      fprintf(stderr, "unknown option --%s\n", name.c_str());
      Usage(argv[0]);
      return 1;
    }
  }
  if (!have_node) {
    Usage(argv[0]);
    return 1;
  }

  // Poll loops on the worker yield instead of sleeping.
  SetNtbWaitMode(NtbWaitMode::kYield);

  fprintf(stderr, "[worker] primary node id = %u\n", opts.primary_node_id);
  WorkerServer server(opts);
  g_server = &server;
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  Status s = server.Connect();
  if (!s.ok()) {
    fprintf(stderr, "[worker] connect failed: %s\n", s.ToString().c_str());
    return 1;
  }
  server.Run();
  return 0;
}
