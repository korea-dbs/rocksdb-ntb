# NTB-Native Remote Compaction for RocksDB

This repository contains the prototype and the baselines used in our paper.

A primary RocksDB instance offloads compaction jobs to a worker host over a
PCIe non-transparent bridge (NTB). The primary DMAs the input SSTs and a
job-local compaction specification into the worker's memory; the worker
merges the inputs in memory, without opening the database and without a
shared file system, and streams the output SSTs back by DMA. The primary
writes them to its own disk and installs them.

## Branches

| Branch     | Host             | Contents |
|------------|------------------|----------|
| `main`     | primary          | NTB-Native: RocksDB with the remote compaction dispatcher and install path (`db_bench`) |
| `worker`   | worker           | NTB-Native: compaction worker (`worker_main`) |
| `baseline` | primary + worker | Baselines: local RocksDB, and file-based remote compaction over NFS or GFS2 (`db_bench`, `tools/nfs_compaction_worker`) |

All branches start from an unmodified snapshot of RocksDB 11.3.0
(facebook/rocksdb@3daabe2db), followed by one housekeeping commit (upstream
CI and agent files removed, README replaced). Compare a branch with its second commit to see our
changes.

## Layout

```
remote_compaction/              (main, worker)
  shared/         job spec encoding and the NTB channel (SISCI); identical on both branches
  primary_only/   dispatcher and remote compaction job (main)
  worker_only/    worker server, in-memory SST files, entry point (worker)
tools/                          (baseline)
  nfs_compaction_service.*   primary side: RocksDB CompactionService over a shared directory
  nfs_compaction_worker.cc   worker side: runs DB::OpenAndCompact() per job
```

Changes to RocksDB itself:

- `main`: `db/db_impl/db_impl_compaction_flush.cc` (routing and asynchronous
  install), `db/db_impl/db_impl.{h,cc}`, `db/compaction/compaction.h` and
  `compaction_picker.cc` (key ranges of in-flight remote jobs),
  `tools/db_bench_tool.cc` (`--allow_fallocate`).
- `worker`: `table/block_based/block_prefetcher.cc` (no prefetch buffer when
  readahead is 0).
- `baseline`: `db/compaction/compaction_service_job.cc` and
  `db/db_impl/db_impl_secondary.cc` (worker-side subcompactions,
  instrumentation), `db/compaction/compaction.cc` (`RC_SUB_MIN_MB`),
  `db/compaction/compaction_job.cc` (install time), `db/table_cache.cc` and
  `table/format.cc` (unique-ID and file-size checks disabled),
  `tools/db_bench_tool.cc`.

## Requirements

- Two x86-64 Linux hosts with Dolphin PXH830 NTB adapters (PCIe Gen3 x16),
  connected through an MXS824 PCIe switch; we used Ubuntu 24.04.
- NTB-Native and GFS2: Dolphin eXpressWare with the SISCI API under
  `/opt/DIS` (override with `-DSISCI_ROOT=...`); not included here. DMA uses
  the host's Intel CBDMA channels through the Dolphin driver (8 channels),
  and the driver must preallocate enough NTB memory for 8 x 90 MiB segments
  per direction; see the comment at the top of
  `remote_compaction/shared/ntb_channel.h`.
- NFS: an Ethernet link between the hosts (we used 10 GbE).
- RocksDB build dependencies: gflags, zstd, snappy, lz4, zlib, liburing.

## Build

```
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DPORTABLE=0 -DFAIL_ON_WARNINGS=OFF \
  -DROCKSDB_BUILD_SHARED=OFF -DWITH_GFLAGS=ON -DWITH_ZSTD=ON -DWITH_SNAPPY=ON \
  -DWITH_LZ4=ON -DWITH_ZLIB=ON -DWITH_LIBURING=ON -DWITH_TESTS=OFF \
  -DWITH_TOOLS=ON -DWITH_BENCHMARK_TOOLS=ON
make -j"$(nproc)" db_bench                # main (primary), baseline (primary)
make -j"$(nproc)" worker_main             # worker (worker)
make -j"$(nproc)" nfs_compaction_worker   # baseline (worker)
```

## Workloads

Two workloads, both `overwrite` on an existing database for 600 s with four
writer threads, the primary's `db_bench` pinned to four cores:

| | CPU-bound | I/O-bound |
|---|---|---|
| keys / value size | 300 M / 16 B | 50 M / 1 KiB |
| write buffer | 16 MiB | 32 MiB |
| L1 target size / SST size | 16 MiB / 4 MiB | 64 MiB / 16 MiB |
| compaction threads (primary and worker) | 8 | 16 |
| minimum job input for subcompactions | 16 MiB | 64 MiB |

### Base databases

Each run starts from a fresh copy of a base database. We built the bases
once with `db_bench` of the `baseline` branch:

```
# CPU-bound base
./db_bench --benchmarks=fillseq,waitforcompaction --num=300000000 --threads=1 \
  --key_size=16 --value_size=16 --compression_type=zstd \
  --write_buffer_size=16777216 --max_write_buffer_number=10 \
  --level0_file_num_compaction_trigger=2 --max_bytes_for_level_base=16777216 \
  --target_file_size_base=4194304 --max_background_jobs=16 \
  --use_direct_reads=true --use_direct_io_for_flush_and_compaction=true \
  --db=<cpu base dir>

# I/O-bound base: same, with
#   --num=50000000 --value_size=1024
#   --max_bytes_for_level_base=67108864 --target_file_size_base=16777216
```

Before each run: `rm -rf <db dir> && cp -a <base dir> <db dir>`, and
`fstrim` the data SSD.

### db_bench flags

```
COMMON="--benchmarks=overwrite,stats --use_existing_db=1 --duration=600 \
  --threads=4 --key_size=16 --compression_type=zstd \
  --max_write_buffer_number=10 --level0_file_num_compaction_trigger=2 \
  --level0_slowdown_writes_trigger=30 --level0_stop_writes_trigger=45 \
  --max_background_jobs=8 --max_background_flushes=2 \
  --use_direct_reads=true --use_direct_io_for_flush_and_compaction=true \
  --enable_pipelined_write=true --skip_stats_update_on_db_open=true \
  --allow_fallocate=false --histogram=1 --statistics=1 --report_bg_io_stats=true"

CPU="--num=300000000 --writes=300000000 --value_size=16 \
  --write_buffer_size=16777216 --max_bytes_for_level_base=16777216 \
  --target_file_size_base=4194304 --max_background_compactions=8"

IO="--num=50000000 --writes=50000000 --value_size=1024 \
  --write_buffer_size=33554432 --max_bytes_for_level_base=67108864 \
  --target_file_size_base=16777216 --max_background_compactions=16"
```

Below, `$W` is `$CPU` or `$IO`, `T` is the worker thread count (8 or 16) and
`M` the minimum job input for subcompactions in MiB.

## Preliminary Results

Throughput (ops/s) of `db_bench overwrite` for 600 s on the CPU-bound and I/O-bound
workloads described above. NFS and NTB run RocksDB remote compaction as released
(NTB: PCIe NTB with GFS2); NTB-Native is this repository.
`nosub`: no subcompactions, `subL0`: L0→L1 only, `suball`: every level.

| System | CPU nosub | CPU subL0 | CPU suball | I/O nosub | I/O subL0 | I/O suball |
|---|---|---|---|---|---|---|
| Local | 276,831 | 290,041 | 243,378 | 54,832 | 68,790 | 65,441 |
| NFS | 330,705 | 330,825 | 327,713 | 52,659 | 51,355 | 53,186 |
| NTB | 369,284 | 327,235 | 312,669 | 49,111 | 52,927 | 56,152 |
| NTB-Native | 414,455 | 449,816 | 442,956 | 73,283 | 136,014 | 129,768 |

## Running the systems

Each system is run without subcompactions, with subcompactions only for
L0->L1 compactions, and with subcompactions for every compaction.

### Local (`baseline`)

```
taskset -c 0-3 ./db_bench $COMMON $W --db=<db dir>
# subcompactions for L0->L1:        add --subcompactions=8
# subcompactions for every level:   add --subcompactions=8 and set RC_SUB_MIN_MB=M
```

### NTB-Native (`main` on the primary, `worker` on the worker)

```
# primary
RC_WORKER_NODE=<SISCI node id of the worker> taskset -c 0-3 ./db_bench $COMMON $W --db=<db dir>

# worker, started right after db_bench
./worker_main --primary_node=<SISCI node id of the primary> --threads=T
# subcompactions for L0->L1:        add --subcompaction=l0  --subcompaction_min_mb=M
# subcompactions for every level:   add --subcompaction=all --subcompaction_min_mb=M
```

Offloaded compactions: L0->L1, L1->L2, and every compaction whose output
level is L3 or deeper. Intra-L0 compactions and compactions whose inputs
contain range deletions or need a merge operator stay local.

| `db_bench` environment | Default | Meaning |
|---|---|---|
| `RC_WORKER_NODE` | 4 | SISCI node id of the worker |
| `RC_REMOTE` | 1 | 0 runs every compaction locally |

| `worker_main` option | Default | Meaning |
|---|---|---|
| `--primary_node=N` | (required) | SISCI node id of the primary |
| `--threads=N` | 16 | compaction threads |
| `--subcompaction=none\|l0\|all` | none | split a job into key ranges compacted in parallel: never, only L0->L1 jobs, or every job |
| `--max_subcompactions=N` | 8 | ranges per job |
| `--subcompaction_min_mb=N` | 64 | jobs with less input are not split |
| `--subcompaction_min_range_mb=N` | 0 | minimum input per range; 0 = the job's target output file size |
| `--input_pool_mb=N` | 32768 | input buffers kept for reuse between jobs; 0 disables reuse |

The worker retries the connection to the primary for up to five minutes.
Per job, the primary prints `[timing]` lines and the worker
`[worker][timing]` lines; the time breakdowns in the paper come from them.


### NFS and GFS2 (`baseline` on both hosts)

The primary's database directory and a job directory are on storage that
both hosts can access (see Setup). `<shared>` is the job directory and
`<db dir>` the database directory, as seen by each host.

```
# primary
RCMP_SHARED_DIR=<shared> RCMP_WORKERS=w0 RCMP_MIN_LEVEL=1 RCMP_POLL_MS=1 \
  taskset -c 0-3 ./db_bench $COMMON $W --db=<db dir>
# offload only L0->L1:              also set RCMP_MAX_LEVEL=1

# worker
./tools/nfs_compaction_worker <shared> w0 T <db dir>
# subcompactions for L0->L1:        RC_WSPLIT=8 RC_WSPLIT_MIN_MB=M RC_WSPLIT_L0ONLY=1 ./tools/nfs_compaction_worker ...
# subcompactions for every level:   RC_WSPLIT=8 RC_WSPLIT_MIN_MB=M ./tools/nfs_compaction_worker ...
```

| Environment (primary) | Default | Meaning |
|---|---|---|
| `RCMP_SHARED_DIR` | (unset) | job directory; unset = no remote compaction (local RocksDB) |
| `RCMP_WORKERS` | worker | worker name(s), comma-separated; must match the worker's second argument |
| `RCMP_MIN_LEVEL` / `RCMP_MAX_LEVEL` | 1 / 99 | offload compactions whose output level is in this range |
| `RCMP_POLL_MS` | 1 | polling interval for finished jobs |

| Environment (worker) | Default | Meaning |
|---|---|---|
| `RC_WSPLIT` | 0 | > 1: split a job into up to this many key-range subcompactions |
| `RC_WSPLIT_MIN_MB` | 64 | jobs with less input are not split |
| `RC_WSPLIT_L0ONLY` | 0 | 1: split only jobs that start at L0 |

Per job, the worker prints `[RCMP_LATENCY]` (CPU, wall, read, write and
metadata time) and `[RCMP_IOPH]` (DB open vs. compaction I/O); the primary
prints `[RCMP_WAIT]`, `[RCMP_RENAME]` and `[RCMP_INSTALL]`.

## Setup

### NTB-Native

Install eXpressWare on both hosts and give each host a SISCI node id (ours:
primary 8, worker 4). We set the number of CBDMA channels used by the PX
driver to 8 (`intel_dma_num_channels=8` in `dis_px.conf`).

### NFS

The data SSD is an ext4 file system on the primary, mounted at `/mnt/nvme`.

```
# primary: /etc/exports
/mnt/nvme/nfs_db  <worker address>(rw,sync)
sudo exportfs -ra

# worker
sudo mkdir -p /mnt/nvme/nfs_db
sudo mount -t nfs4 <primary address>:/mnt/nvme/nfs_db /mnt/nvme/nfs_db
```

With this layout, `<shared>` is `/mnt/nvme/nfs_db` and `<db dir>` is a
directory under it, on both hosts.

### GFS2 over SmartIO

The primary's NVMe SSD is shared with the worker through Dolphin SmartIO and
formatted with GFS2 (DLM locking, two journals).

```
# primary (owner of the SSD), SmartIO NVMe drivers from eXpressWare
sudo insmod dis_nvme_manager.ko
sudo insmod dis_nvme.ko
sudo /opt/DIS/sbin/smartio_tool add <PCI address of the SSD>
sudo /opt/DIS/sbin/smartio_tool available <PCI address of the SSD>   # answer y
sudo /opt/DIS/sbin/smartio_tool connect <id>                         # we used 4

# worker
sudo insmod dis_nvme.ko            # the shared SSD appears as /dev/dis<...>n1

# both hosts: two-node corosync cluster (cluster name "mycluster") and DLM
sudo modprobe dlm
sudo systemctl start corosync && sudo systemctl start dlm

# once, on one host
sudo mkfs.gfs2 -p lock_dlm -t mycluster:gfs2nvme -j 2 /dev/dis<...>n1

# both hosts
sudo mount -t gfs2 /dev/dis<...>n1 /mnt/nvme
```

With this layout, `<shared>` is `/mnt/nvme` and `<db dir>` a directory
under it, on both hosts.

## License

This code is derived from RocksDB and is distributed under the same terms:
GPLv2 (`COPYING`) or Apache 2.0 (`LICENSE.Apache`), at your option.
