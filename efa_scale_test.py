#!/usr/bin/env python3
"""
EFA RDMA scale test — long-running workload against an ElastiCache cluster
(or standalone server) to shake out bugs under sustained load, saturate EFA
bandwidth, provoke race conditions, and validate DRAM pool scaling toward the
384 GiB EFA registration limit.

Requires:
  - valkey Python package (pip install valkey)
  - fabric_target binary (compiled from tests/harness/fabric_target.rs)
  - EFA drivers + libfabric on the client instance

Usage (cluster mode):
    python3 efa_scale_test.py \\
        --cluster-endpoint clustercfg.xxx.euw1devo.cache.amazonaws.com \\
        --tls --cluster-mode \\
        --duration 3600 --concurrency 8 --client-id 1

Usage (standalone mode):
    python3 efa_scale_test.py \\
        --host <server-ip> --port 6379 \\
        --duration 300 --concurrency 4
"""

import argparse
import json
import logging
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import valkey

# ─── Constants ────────────────────────────────────────────────────────────────

FABRIC_TARGET = os.environ.get(
    "FABRIC_TARGET", "/mnt/nvme0/cargo-target/release/fabric_target"
)

OBJECT_SIZES = [4096, 65536, 1048576, 10485760, 52428800]  # 4KB → 50MB

WORKLOAD_RATIOS = {
    "write_heavy":       (0.90, 0.10),   # (SET%, GET%)
    "read_heavy":        (0.10, 0.90),
    "balanced":          (0.50, 0.50),
    "burst_write":       (0.95, 0.05),
    "staging_pressure":  (1.00, 0.00),   # 100% SET to saturate NVMe staging buffers
}

# EFA device can register ~384 GiB in 4 KiB pages. Use 380 GiB safety margin.
EFA_REGISTRATION_LIMIT_BYTES = 380 * 1024 * 1024 * 1024

HEALTH_PULSE_INTERVAL = 30  # seconds

# ─── Logging ──────────────────────────────────────────────────────────────────

log = logging.getLogger("efa_scale")


def setup_logging(logfile, max_size_mb, backups):
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03dZ [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fmt.converter = time.gmtime
    log.setLevel(logging.DEBUG)
    # Rotating file handler
    fh = RotatingFileHandler(
        logfile, maxBytes=max_size_mb * 1024 * 1024, backupCount=backups
    )
    fh.setFormatter(fmt)
    log.addHandler(fh)
    # Stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)


# ─── Graceful Shutdown ────────────────────────────────────────────────────────


class GracefulShutdown:
    def __init__(self):
        self.shutdown_requested = threading.Event()
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame):
        log.warning("Shutdown requested (signal %d), finishing in-flight ops...", signum)
        self.shutdown_requested.set()

    def should_continue(self):
        return not self.shutdown_requested.is_set()


# ─── fabric_target helpers ────────────────────────────────────────────────────


# ─── fabric_target helpers ────────────────────────────────────────────────────
#
# Each RDMA transfer requires a registered memory region on the client for the
# server to read from (SET) or write into (GET). fabric_target is a one-shot
# Rust binary that opens an EFA endpoint, allocates and registers a buffer via
# fi_mr_reg, and advertises its rkey/addr/len. A new process + endpoint +
# registration is created per operation because fabric_target does not yet
# support persistent mode. This is the main throughput bottleneck: a production
# client (e.g. GPU inference) would hold long-lived RDMA sessions with
# pre-registered memory (see vdma's FabricService + registered_arena pattern).


def start_fabric_target(server_fabric_addr, size, read_mode=False, num_regions=1):
    """Launch fabric_target --efa --size=<size> and parse its advertisement.

    Returns (proc, advertisements, source_crc) where advertisements is a list
    of (addr_hex, rkey, remote_addr, length) tuples (one per region).
    source_crc is the CRC of the prefilled buffer (read_mode only), else None.
    """
    cmd = [FABRIC_TARGET, "--efa", f"--size={size}", server_fabric_addr]
    if read_mode:
        cmd.insert(1, "--read")
    if num_regions > 1:
        # Split the buffer into num_regions randomly-sized regions.
        # Generate random cut points, sort them, and derive region sizes.
        cuts = sorted(random.randint(1, size - 1) for _ in range(num_regions - 1))
        # Deduplicate: if two cuts land on the same byte, a region would be 0.
        cuts = sorted(set(cuts))
        # If dedup reduced the count, fall back to fewer regions.
        boundaries = [0] + cuts + [size]
        parts = [str(boundaries[i + 1] - boundaries[i]) for i in range(len(boundaries) - 1)]
        cmd.insert(1, f"--split={','.join(parts)}")
        num_regions = len(parts)  # actual count after dedup
    env = dict(
        os.environ,
        LD_LIBRARY_PATH="/opt/amazon/efa/lib64:"
        + os.environ.get("LD_LIBRARY_PATH", ""),
    )
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    # Read one advertisement line per region.
    advertisements = []
    for _ in range(num_regions):
        line = proc.stdout.readline()
        if not line.startswith("advertisement:"):
            rest = proc.stdout.read()
            proc.kill()
            raise RuntimeError(f"fabric_target failed to advertise: {line}{rest}")
        parts = line.split()
        advertisements.append((
            parts[1],       # addr_hex (same for all regions)
            int(parts[2]),  # rkey
            int(parts[3]),  # remote_addr
            int(parts[4]),  # length
        ))
    source_crc = None
    if read_mode:
        crc_line = proc.stdout.readline()
        m = re.search(r"\bcrc (0x[0-9a-fA-F]+)\b", crc_line)
        if m:
            source_crc = int(m.group(1), 16)
    return proc, advertisements, source_crc


def parse_client_crc(output):
    """Extract the CRC that fabric_target computed over the bytes in its buffer."""
    m = re.search(r"\bcrc (0x[0-9a-fA-F]+)\b", output)
    return int(m.group(1), 16) if m else None


def kill_target(proc):
    """Terminate fabric_target and drain its output."""
    try:
        proc.kill()
        proc.wait(timeout=5)
    except Exception:
        pass


# ─── Cluster Topology ─────────────────────────────────────────────────────────


def discover_cluster(endpoint, port, tls):
    """Discover shard primaries via CLUSTER NODES."""
    conn = valkey.Valkey(
        host=endpoint, port=port, ssl=tls,
        ssl_cert_reqs=None if tls else None,
        decode_responses=True, socket_timeout=10,
    )
    nodes_raw = conn.execute_command("CLUSTER", "NODES")
    shards = {}
    for line in nodes_raw.strip().split("\n"):
        parts = line.split()
        if len(parts) < 9:
            continue
        node_id, addr, flags = parts[0], parts[1], parts[2]
        if "master" not in flags or "fail" in flags:
            continue
        host_port = addr.split("@")[0]
        host, node_port = host_port.rsplit(":", 1)
        slots = parts[8:]
        shard_id = slots[0].split("-")[0] if slots else node_id[:8]
        shards[shard_id] = {
            "host": host,
            "port": int(node_port),
            "slots": slots,
            "node_id": node_id,
        }
    conn.close()
    log.info("Discovered %d shard(s)", len(shards))
    for sid, info in shards.items():
        log.info(
            "  shard=%s node=%s:%d slots=%s",
            sid, info["host"], info["port"], ",".join(info["slots"]),
        )
    return shards


def connect_standalone(host, port, tls):
    """Single-node mode: one 'shard' covering all slots."""
    return {
        "0": {
            "host": host,
            "port": port,
            "slots": ["0-16383"],
            "node_id": "standalone",
        }
    }


# ─── Hash-Tagged Key Routing ─────────────────────────────────────────────────

# CRC16-CCITT lookup table for Valkey cluster hash slot computation.
_CRC16_TABLE = None


def _crc16_init():
    global _CRC16_TABLE
    _CRC16_TABLE = []
    for i in range(256):
        crc = i << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
        _CRC16_TABLE.append(crc & 0xFFFF)


def crc16(data: bytes) -> int:
    if _CRC16_TABLE is None:
        _crc16_init()
    crc = 0
    for b in data:
        crc = ((_CRC16_TABLE[((crc >> 8) ^ b) & 0xFF]) ^ (crc << 8)) & 0xFFFF
    return crc


def compute_shard_hash_tags(shards):
    """Find a short hash tag for each shard that maps to its slot range."""
    tags = {}
    for shard_id, info in shards.items():
        slot_range = info["slots"][0]
        if "-" in slot_range:
            start, end = map(int, slot_range.split("-"))
        else:
            start = end = int(slot_range)
        for i in range(200000):
            tag = f"s{shard_id}_{i}"
            slot = crc16(tag.encode()) % 16384
            if start <= slot <= end:
                tags[shard_id] = tag
                log.info("  shard=%s hash_tag={%s} → slot %d", shard_id, tag, slot)
                break
        else:
            raise RuntimeError(
                f"Could not find hash tag for shard {shard_id} slots {slot_range}"
            )
    return tags


# ─── Per-Shard RDMA Session Setup ────────────────────────────────────────────


def setup_shard_connections(shards, tls):
    """Create a Valkey connection to each shard primary."""
    connections = {}
    for shard_id, info in shards.items():
        conn = valkey.Valkey(
            host=info["host"], port=info["port"],
            ssl=tls, ssl_cert_reqs=None if tls else None,
            decode_responses=False, socket_timeout=30,
        )
        conn.ping()
        connections[shard_id] = conn
        log.info(
            "  shard=%s connected to %s:%d", shard_id, info["host"], info["port"]
        )
    return connections


def discover_server_fabric_addr(shard_info, tls):
    """Discover the server's fabric address via RDMA_HELLO.

    Starts a throwaway fabric_target (without the server address — it can't do
    transfers, but it opens an EFA endpoint and advertises its own address).
    Sends BLOB.RDMA_HELLO with that address on a throwaway connection. The
    server returns its fabric address(es). Kills the throwaway target and
    connection — they can't be reused for transfers.
    """
    env = dict(
        os.environ,
        LD_LIBRARY_PATH="/opt/amazon/efa/lib64:"
        + os.environ.get("LD_LIBRARY_PATH", ""),
    )
    # Start fabric_target without the server address positional — just to get
    # a valid EFA endpoint address for HELLO.
    cmd = [FABRIC_TARGET, "--efa", f"--size={DEFAULT_BUFFER_LEN}"]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    try:
        line = proc.stdout.readline()
        if not line.startswith("advertisement:"):
            rest = proc.stdout.read()
            raise RuntimeError(
                f"fabric_target discovery failed to advertise: {line}{rest}"
            )
        client_addr = line.split()[1]
        # HELLO on a throwaway connection
        conn = valkey.Valkey(
            host=shard_info["host"], port=shard_info["port"],
            ssl=tls, ssl_cert_reqs=None if tls else None,
            decode_responses=False, socket_timeout=10,
        )
        hello_reply = conn.execute_command("BLOB.RDMA_HELLO", client_addr)
        conn.close()
        if isinstance(hello_reply, list) and len(hello_reply) > 0:
            # Return the first server fabric address (hex-encoded bytes)
            addr = hello_reply[0]
            if isinstance(addr, bytes):
                return addr.decode("ascii")
            return str(addr)
        raise RuntimeError(f"RDMA_HELLO returned unexpected reply: {hello_reply!r}")
    finally:
        kill_target(proc)

# Default buffer size for the discovery handshake (matches fabric_target default).
DEFAULT_BUFFER_LEN = 4096


# ─── Startup Verification ────────────────────────────────────────────────────


def verify_cluster_state(connections, shards):
    """Read INFO from each shard and log config. Abort on fatal misconfig."""
    cluster_info = {}
    for shard_id, conn in connections.items():
        info = {}
        # INFO memory for maxmemory
        try:
            mem = conn.info("memory")
            info["used_memory"] = mem.get("used_memory", 0)
            info["maxmemory"] = mem.get("maxmemory", 0)
        except Exception as e:
            log.warning("shard=%s INFO memory failed: %s", shard_id, e)
        # INFO largeobj for module config
        try:
            lo = conn.info("largeobj")
            info["segment_size"] = lo.get("largeobj_dram_segment_size_bytes", 0)
            info["efa_registered"] = lo.get("largeobj_dram_rdma_registered_segments",
                                            lo.get("largeobj_efa_registered_segments", 0))
            info["dram_segments"] = lo.get("largeobj_dram_segments", 0)
            info["dram_capacity"] = lo.get("largeobj_dram_capacity_bytes", 0)
            info["dram_pool_exhausted"] = lo.get("largeobj_dram_pool_exhausted", 0)
        except Exception as e:
            log.warning("shard=%s INFO largeobj failed: %s", shard_id, e)
        # Read max-object-size via CONFIG GET (may fail on managed clusters)
        info["max_object_size"] = 0
        try:
            cfg = conn.execute_command("CONFIG", "GET", "largeobj.max-object-size")
            if cfg and len(cfg) >= 2:
                info["max_object_size"] = int(cfg[1])
        except Exception:
            # Managed cluster may block CONFIG. Estimate from segment_size.
            info["max_object_size"] = info.get("segment_size", 0)
        cluster_info[shard_id] = info
        log.info(
            "shard=%s maxmemory=%s segment_size=%s efa_registered=%s "
            "max_object_size=%s dram_segments=%s",
            shard_id,
            _human_bytes(info.get("maxmemory", 0)),
            _human_bytes(info.get("segment_size", 0)),
            info.get("efa_registered", "?"),
            _human_bytes(info.get("max_object_size", 0)),
            info.get("dram_segments", "?"),
        )
        if info.get("maxmemory", 0) == 0:
            log.warning(
                "shard=%s has maxmemory=0 (unbounded). "
                "DRAM pool will grow without limit.",
                shard_id,
            )
    return cluster_info


def filter_object_sizes(sizes, max_object_size):
    """Remove sizes that exceed the server's max-object-size."""
    if max_object_size <= 0:
        return sizes
    filtered = [s for s in sizes if s <= max_object_size]
    if len(filtered) < len(sizes):
        removed = [s for s in sizes if s > max_object_size]
        log.warning(
            "Filtered object sizes exceeding max-object-size=%s: %s",
            _human_bytes(max_object_size),
            [_human_bytes(s) for s in removed],
        )
    return filtered


# ─── Health Monitor ───────────────────────────────────────────────────────────


class HealthMonitor:
    """Background thread polling INFO on each shard every HEALTH_PULSE_INTERVAL."""

    def __init__(self, connections, shards, shutdown, segment_size, all_stats):
        self.connections = connections
        self.shards = shards
        self.shutdown = shutdown
        self.segment_size = segment_size
        self.all_stats = all_stats
        self.prev_samples = {}
        self.prev_ops_snapshot = 0
        self.prev_ops_time = time.monotonic()
        self.eviction_stall_count = defaultdict(int)
        self.peak_throughput_gbps = 0.0
        self.peak_registered_segments = 0
        self.thread = threading.Thread(
            target=self._run, name="health-monitor", daemon=True
        )

    def start(self):
        self.thread.start()

    def _run(self):
        while self.shutdown.should_continue():
            self._pulse()
            # Sleep in small increments so shutdown is responsive
            for _ in range(HEALTH_PULSE_INTERVAL * 10):
                if not self.shutdown.should_continue():
                    return
                time.sleep(0.1)

    def _pulse(self):
        for shard_id, conn in self.connections.items():
            try:
                self._poll_shard(shard_id, conn)
            except Exception as e:
                log.warning("[health] shard=%s poll failed: %s", shard_id, e)
        # Periodic ops/s snapshot from worker stats
        now = time.monotonic()
        total_ops = sum(ws.ops for ws in self.all_stats)
        dt = now - self.prev_ops_time
        if dt > 0:
            ops_per_sec = (total_ops - self.prev_ops_snapshot) / dt
            log.info(
                "[perf] ops_total=%d interval_ops/s=%.0f",
                total_ops, ops_per_sec,
            )
        self.prev_ops_snapshot = total_ops
        self.prev_ops_time = now

    def _poll_shard(self, shard_id, conn):
        node = f"{self.shards[shard_id]['host']}:{self.shards[shard_id]['port']}"
        mem = conn.info("memory")
        lo = conn.info("largeobj")
        used = mem.get("used_memory", 0)
        maxmem = mem.get("maxmemory", 0)
        evictions = lo.get("largeobj_evictions", 0)
        pending = lo.get("largeobj_pending_evictions", 0)
        exhausted = lo.get("largeobj_dram_pool_exhausted", 0)
        staging_exhausted = lo.get("largeobj_nvme_buffer_exhausted",
                                   lo.get("largeobj_disk_staging_buffer_exhausted", 0))
        registered = lo.get("largeobj_dram_rdma_registered_segments",
                            lo.get("largeobj_efa_registered_segments", 0))
        dram_segs = lo.get("largeobj_dram_segments", 0)
        dram_util = lo.get("largeobj_dram_utilization_pct", 0)
        expands = lo.get("largeobj_dram_scaling_expands", 0)
        shrinks = lo.get("largeobj_dram_scaling_shrinks", 0)
        efa_read_errs = lo.get("largeobj_rdma_read_errors",
                               lo.get("largeobj_efa_read_errors", 0))
        efa_write_errs = lo.get("largeobj_rdma_write_errors",
                                lo.get("largeobj_efa_write_errors", 0))
        # RDMA byte counters (may not exist on older builds)
        rdma_read = lo.get("largeobj_rdma_read_bytes", 0)
        rdma_write = lo.get("largeobj_rdma_write_bytes", 0)
        # Throughput from RDMA byte counter diffs
        throughput_gbps = 0.0
        prev = self.prev_samples.get(shard_id)
        if prev:
            dt = time.monotonic() - prev["time"]
            if dt > 0:
                total_delta = (
                    (rdma_read - prev.get("rdma_read", 0))
                    + (rdma_write - prev.get("rdma_write", 0))
                )
                throughput_gbps = (total_delta * 8) / (dt * 1e9)
        self.prev_samples[shard_id] = {
            "time": time.monotonic(),
            "rdma_read": rdma_read,
            "rdma_write": rdma_write,
            "evictions": evictions,
            "used_memory": used,
        }
        # Registration pressure check
        reg_bytes = registered * self.segment_size if self.segment_size else 0
        reg_pct = (reg_bytes / EFA_REGISTRATION_LIMIT_BYTES) * 100 if reg_bytes else 0
        reg_tag = ""
        if reg_pct > 90:
            reg_tag = " CRITICAL_REG_PRESSURE"
        elif reg_pct > 70:
            reg_tag = " HIGH_REG_PRESSURE"
        if registered > self.peak_registered_segments:
            self.peak_registered_segments = registered
        if throughput_gbps > self.peak_throughput_gbps:
            self.peak_throughput_gbps = throughput_gbps
        # Eviction stall detection
        stall_tag = ""
        if maxmem > 0 and used > maxmem * 0.95:
            if prev and evictions == prev.get("evictions", 0):
                self.eviction_stall_count[shard_id] += 1
                if self.eviction_stall_count[shard_id] >= 3:
                    stall_tag = " EVICTION_STALL"
            else:
                self.eviction_stall_count[shard_id] = 0
        log.info(
            "[health] shard=%s node=%s used=%s maxmem=%s evictions=%d "
            "pending=%d exhausted=%d staging_exhausted=%d dram_segs=%d "
            "util=%d%% expand=%d shrink=%d efa_reg=%d/%s(%.0f%%) "
            "efa_errs=r%d/w%d throughput=%.1fGbps%s%s",
            shard_id, node, _human_bytes(used), _human_bytes(maxmem),
            evictions, pending, exhausted, staging_exhausted, dram_segs,
            dram_util, expands, shrinks, registered,
            _human_bytes(reg_bytes), reg_pct,
            efa_read_errs, efa_write_errs, throughput_gbps,
            reg_tag, stall_tag,
        )


# ─── Worker ───────────────────────────────────────────────────────────────────


class WorkerStats:
    """Thread-safe per-worker statistics."""

    def __init__(self):
        self.lock = threading.Lock()
        self.ops = 0
        self.gets = 0
        self.sets = 0
        self.errors = 0
        self.crc_mismatches = 0
        self.staging_exhausted = 0
        self.latencies = []  # (op_type, latency_ms)
        self.latencies_by_size = defaultdict(list)  # size -> [latency_ms]
        self.latencies_by_op = defaultdict(list)    # "GET"/"SET" -> [latency_ms]

    def record(self, op_type, latency_ms, obj_size=0, error=False, crc_mismatch=False,
               staging_exhausted=False):
        with self.lock:
            self.ops += 1
            if op_type == "GET":
                self.gets += 1
            else:
                self.sets += 1
            if error:
                self.errors += 1
            if crc_mismatch:
                self.crc_mismatches += 1
            if staging_exhausted:
                self.staging_exhausted += 1
            # Cap stored latencies to avoid unbounded memory growth on long runs.
            # Keep all if < 100K, then sample 1-in-10.
            if len(self.latencies) < 100_000 or random.random() < 0.1:
                self.latencies.append((op_type, latency_ms))
            if obj_size > 0:
                size_lats = self.latencies_by_size[obj_size]
                if len(size_lats) < 50_000 or random.random() < 0.1:
                    size_lats.append(latency_ms)
            # Per-op-type latencies for separate GET vs SET reporting
            op_lats = self.latencies_by_op[op_type]
            if len(op_lats) < 100_000 or random.random() < 0.1:
                op_lats.append(latency_ms)

    def snapshot(self):
        with self.lock:
            return {
                "ops": self.ops,
                "gets": self.gets,
                "sets": self.sets,
                "errors": self.errors,
                "crc_mismatches": self.crc_mismatches,
                "staging_exhausted": self.staging_exhausted,
                "latencies": list(self.latencies),
                "latencies_by_size": {
                    k: list(v) for k, v in self.latencies_by_size.items()
                },
                "latencies_by_op": {
                    k: list(v) for k, v in self.latencies_by_op.items()
                },
            }


def worker_loop(
    worker_id, shard_id, shard_info, hash_tag, server_fabric_addr,
    tls, object_sizes, workload, shutdown, stats,
    multi_region,
):
    """Main worker loop: pick op, execute RDMA transfer, verify, log."""
    node = f"{shard_info['host']}:{shard_info['port']}"
    set_ratio, _ = WORKLOAD_RATIOS[workload]
    key_counter = 0
    # Pool of keys this worker has SET, keyed by size. GETs pick from here.
    written_keys = defaultdict(list)
    MAX_KEY_POOL = 200  # Cap per size to bound memory
    while shutdown.should_continue():
        obj_size = random.choice(object_sizes)
        is_set = random.random() < set_ratio
        # For GET: only attempt if we have previously SET a key of this size.
        # Otherwise fall back to SET (builds up the key pool).
        if not is_set and not written_keys[obj_size]:
            is_set = True
        key_counter += 1
        key = f"{{{hash_tag}}}obj_{worker_id}_{key_counter}"
        try:
            if is_set:
                _do_rdma_set(
                    worker_id, shard_id, shard_info, node, key, obj_size,
                    server_fabric_addr, tls, stats, multi_region=multi_region,
                )
                pool = written_keys[obj_size]
                pool.append(key)
                if len(pool) > MAX_KEY_POOL:
                    pool.pop(0)
            else:
                get_key = random.choice(written_keys[obj_size])
                _do_rdma_get(
                    worker_id, shard_id, shard_info, node, get_key, obj_size,
                    server_fabric_addr, tls, stats, multi_region=multi_region,
                )
        except Exception as e:
            is_staging = ("staging buffer pool exhausted" in str(e)
                         or "NVMe staging buffer" in str(e))
            stats.record(
                "SET" if is_set else "GET", 0, obj_size=obj_size,
                error=True, staging_exhausted=is_staging,
            )
            log.error(
                "[%s] shard=%s node=%s key=%s error=%s",
                worker_id, shard_id, node, key, e,
            )


def _do_rdma_set(worker_id, shard_id, shard_info, node, key, obj_size,
                 server_fabric_addr, tls, stats, multi_region=False):
    """Execute one RDMA SET: spawn fabric_target (read mode), HELLO, SET, verify CRC."""
    # Randomly split across 1-5 regions when multi_region is enabled and size allows.
    # Each region must be at least 1 byte; skip split for tiny objects.
    num_regions = random.randint(1, 5) if (multi_region and obj_size >= 5) else 1
    conn = valkey.Valkey(
        host=shard_info["host"], port=shard_info["port"],
        ssl=tls, ssl_cert_reqs=None if tls else None,
        decode_responses=False, socket_timeout=60,
    )
    proc = None
    try:
        proc, advertisements, source_crc = start_fabric_target(
            server_fabric_addr, obj_size, read_mode=True, num_regions=num_regions,
        )
        client_addr = advertisements[0][0]  # same for all regions
        # Establish RDMA session
        hello_reply = conn.execute_command("BLOB.RDMA_HELLO", client_addr)
        if not isinstance(hello_reply, list) or len(hello_reply) == 0:
            raise RuntimeError(f"RDMA_HELLO unexpected reply: {hello_reply!r}")
        # Build RDMA SET args: key total_len [rkey addr len] per region
        set_args = ["BLOB.RDMA_SET", key, str(obj_size)]
        for _, rkey, remote_addr, length in advertisements:
            set_args.extend([str(rkey), str(remote_addr), str(length)])
        # RDMA SET
        t0 = time.monotonic()
        set_reply = conn.execute_command(*set_args)
        lat_ms = (time.monotonic() - t0) * 1000
        if set_reply != b"OK":
            stats.record("SET", lat_ms, obj_size=obj_size, error=True)
            log.error(
                "[%s] shard=%s node=%s RDMA_SET key=%s size=%s reply=%r",
                worker_id, shard_id, node, key, _human_bytes(obj_size), set_reply,
            )
            return
        # CRC verification via BLOB.INFO
        crc_match = True
        server_crc = None
        try:
            server_crc = conn.execute_command("BLOB.INFO", key, "CRC")
        except Exception:
            pass
        if source_crc is not None and server_crc is not None:
            crc_match = source_crc == server_crc
        stats.record("SET", lat_ms, obj_size=obj_size, crc_mismatch=not crc_match)
        if not crc_match:
            log.error(
                "[%s] shard=%s node=%s RDMA_SET CRC MISMATCH key=%s "
                "client=0x%08x server=0x%08x",
                worker_id, shard_id, node, key, source_crc or 0, server_crc or 0,
            )
            _capture_error_context(conn, key, "CRC mismatch", worker_id, node, shard_id)
        else:
            log.debug(
                "[%s] shard=%s node=%s RDMA_SET key=%s size=%s lat_ms=%.1f OK",
                worker_id, shard_id, node, key, _human_bytes(obj_size), lat_ms,
            )
    finally:
        if proc:
            kill_target(proc)
        try:
            conn.close()
        except Exception:
            pass


def _do_rdma_get(worker_id, shard_id, shard_info, node, key, obj_size,
                 server_fabric_addr, tls, stats, multi_region=False):
    """Execute one RDMA GET: first ensure key exists (TCP SET), then RDMA GET + CRC verify."""
    num_regions = random.randint(1, 5) if (multi_region and obj_size >= 5) else 1
    conn = valkey.Valkey(
        host=shard_info["host"], port=shard_info["port"],
        ssl=tls, ssl_cert_reqs=None if tls else None,
        decode_responses=False, socket_timeout=60,
    )
    proc = None
    try:
        # The key was previously SET by this worker (from written_keys pool),
        # so it exists with the correct size. No TCP prep needed.
        proc, advertisements, _ = start_fabric_target(
            server_fabric_addr, obj_size, read_mode=False, num_regions=num_regions,
        )
        client_addr = advertisements[0][0]
        hello_reply = conn.execute_command("BLOB.RDMA_HELLO", client_addr)
        if not isinstance(hello_reply, list) or len(hello_reply) == 0:
            raise RuntimeError(f"RDMA_HELLO unexpected reply: {hello_reply!r}")
        # Build RDMA GET args: key [rkey addr len] per region
        get_args = ["BLOB.RDMA_GET", key]
        for _, rkey, remote_addr, length in advertisements:
            get_args.extend([str(rkey), str(remote_addr), str(length)])
        t0 = time.monotonic()
        get_reply = conn.execute_command(*get_args)
        lat_ms = (time.monotonic() - t0) * 1000
        if not isinstance(get_reply, list) or len(get_reply) != 2:
            # Key may not exist (nil reply)
            if get_reply is None:
                stats.record("GET", lat_ms, obj_size=obj_size)
                log.debug(
                    "[%s] shard=%s node=%s RDMA_GET key=%s nil (key not found)",
                    worker_id, shard_id, node, key,
                )
                return
            stats.record("GET", lat_ms, obj_size=obj_size, error=True)
            log.error(
                "[%s] shard=%s node=%s RDMA_GET key=%s unexpected reply=%r",
                worker_id, shard_id, node, key, get_reply,
            )
            return
        server_len = get_reply[0]
        server_crc = get_reply[1]
        # Wait briefly for fabric_target to receive bytes and compute CRC
        time.sleep(min(2, obj_size / (50 * 1024 * 1024) + 0.5))
        proc.terminate()
        try:
            stdout, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, _ = proc.communicate(timeout=5)
        client_crc = parse_client_crc(stdout)
        crc_match = True
        if client_crc is not None and server_crc is not None:
            crc_match = client_crc == server_crc
        verified = "payload verified" in (stdout or "")
        stats.record("GET", lat_ms, obj_size=obj_size, crc_mismatch=not crc_match)
        if not crc_match:
            log.error(
                "[%s] shard=%s node=%s RDMA_GET CRC MISMATCH key=%s "
                "client=0x%08x server=0x%08x",
                worker_id, shard_id, node, key,
                client_crc or 0, server_crc or 0,
            )
            _capture_error_context(conn, key, "CRC mismatch", worker_id, node, shard_id)
        elif not verified and client_crc is not None:
            log.warning(
                "[%s] shard=%s node=%s RDMA_GET key=%s CRC OK but payload not verified",
                worker_id, shard_id, node, key,
            )
        else:
            log.debug(
                "[%s] shard=%s node=%s RDMA_GET key=%s size=%s lat_ms=%.1f OK",
                worker_id, shard_id, node, key, _human_bytes(obj_size), lat_ms,
            )
    finally:
        if proc:
            kill_target(proc)
        try:
            conn.close()
        except Exception:
            pass


def _capture_error_context(conn, key, error, worker_id, node, shard_id):
    """Capture server-side state on failure for correlation."""
    ctx = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "worker": worker_id,
        "node": node,
        "shard": shard_id,
        "key": key,
        "error": error,
    }
    try:
        ctx["blob_info"] = conn.execute_command("BLOB.INFO", key)
    except Exception:
        ctx["blob_info"] = None
    try:
        lo = conn.info("largeobj")
        ctx["info_largeobj"] = {
            k: v for k, v in lo.items()
            if isinstance(k, str) and k.startswith("largeobj_")
        }
    except Exception:
        ctx["info_largeobj"] = None
    try:
        mem = conn.info("memory")
        ctx["used_memory"] = mem.get("used_memory")
        ctx["maxmemory"] = mem.get("maxmemory")
    except Exception:
        pass
    log.error("[error_context] %s", json.dumps(ctx, default=str))


# ─── Race Provocation Tests ──────────────────────────────────────────────────


def run_race_tests(shard_id, shard_info, hash_tag, server_fabric_addr, tls, shutdown):
    """Run race provocation tests against one shard. All transfers use RDMA."""
    node = f"{shard_info['host']}:{shard_info['port']}"
    log.info("[race] Starting race tests on shard=%s node=%s", shard_id, node)
    OBJ_SIZE = 4096

    def make_conn():
        return valkey.Valkey(
            host=shard_info["host"], port=shard_info["port"],
            ssl=tls, ssl_cert_reqs=None if tls else None,
            decode_responses=False, socket_timeout=30,
        )

    def rdma_set(key, size=OBJ_SIZE):
        """One-shot RDMA SET: conn + fabric_target + HELLO + SET + teardown."""
        conn = make_conn()
        proc, adverts, source_crc = start_fabric_target(
            server_fabric_addr, size, read_mode=True,
        )
        try:
            conn.execute_command("BLOB.RDMA_HELLO", adverts[0][0])
            args = ["BLOB.RDMA_SET", key, str(size)]
            for _, rkey, raddr, rlen in adverts:
                args.extend([str(rkey), str(raddr), str(rlen)])
            reply = conn.execute_command(*args)
            return reply == b"OK", source_crc
        finally:
            kill_target(proc)
            conn.close()

    def rdma_get_crc(key, size=OBJ_SIZE):
        """One-shot RDMA GET: returns (server_crc, client_crc) or (None, None)."""
        conn = make_conn()
        proc, adverts, _ = start_fabric_target(
            server_fabric_addr, size, read_mode=False,
        )
        try:
            conn.execute_command("BLOB.RDMA_HELLO", adverts[0][0])
            args = ["BLOB.RDMA_GET", key]
            for _, rkey, raddr, rlen in adverts:
                args.extend([str(rkey), str(raddr), str(rlen)])
            reply = conn.execute_command(*args)
            if not isinstance(reply, list) or len(reply) != 2:
                return None, None
            server_crc = reply[1]
            time.sleep(0.5)
            proc.terminate()
            stdout, _ = proc.communicate(timeout=10)
            client_crc = parse_client_crc(stdout)
            return server_crc, client_crc
        finally:
            kill_target(proc)
            conn.close()

    passed = 0
    failed = 0

    def check(label, condition, detail=""):
        nonlocal passed, failed
        if condition:
            passed += 1
            log.info("[race] ✓ %s", label)
        else:
            failed += 1
            msg = f"[race] ✗ {label}"
            if detail:
                msg += f" — {detail}"
            log.error(msg)

    # ── Concurrent GET same key ──
    if shutdown.should_continue():
        log.info("[race] ── Concurrent GET same key ──")
        key = f"{{{hash_tag}}}race_cget"
        ok, _ = rdma_set(key)
        check("Setup SET for concurrent GET", ok)
        errors = []
        crcs = [None] * 8

        def concurrent_get(idx):
            try:
                s_crc, c_crc = rdma_get_crc(key)
                crcs[idx] = (s_crc, c_crc)
            except Exception as e:
                errors.append(f"thread-{idx}: {e}")

        threads = [threading.Thread(target=concurrent_get, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        valid = [c for c in crcs if c is not None and c[0] is not None]
        crc_ok = all(s == c for s, c in valid) if valid else False
        check(
            f"Concurrent GET: {len(valid)}/8 got data, CRCs match",
            len(valid) == 8 and crc_ok,
            f"errors={errors[:3]}" if errors else
            f"crc_mismatches={sum(1 for s, c in valid if s != c)}",
        )
        make_conn().execute_command("DEL", key)

    # ── Concurrent SET same key ──
    if shutdown.should_continue():
        log.info("[race] ── Concurrent SET same key ──")
        key = f"{{{hash_tag}}}race_cset"
        errors = []

        def concurrent_set(idx):
            try:
                ok, _ = rdma_set(key)
                if not ok:
                    errors.append(f"thread-{idx}: SET returned not OK")
            except Exception as e:
                errors.append(f"thread-{idx}: {e}")

        threads = [threading.Thread(target=concurrent_set, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        # Verify key exists — last writer wins
        s_crc, c_crc = rdma_get_crc(key)
        check(
            "Concurrent SET: key readable after race",
            s_crc is not None,
            f"GET returned nil" if s_crc is None else "",
        )
        check("Concurrent SET: no errors", len(errors) == 0, "; ".join(errors[:3]))
        make_conn().execute_command("DEL", key)

    # ── GET during SET (same key) ──
    if shutdown.should_continue():
        log.info("[race] ── GET during SET (same key) ──")
        key = f"{{{hash_tag}}}race_gds"
        # Initial SET
        rdma_set(key)
        get_results = []
        set_errors = []
        get_errors = []

        def setter():
            try:
                rdma_set(key, size=65536)  # Overwrite with larger object
            except Exception as e:
                set_errors.append(str(e))

        def getter():
            for _ in range(5):
                try:
                    s_crc, _ = rdma_get_crc(key)
                    get_results.append("ok" if s_crc is not None else "nil")
                except Exception as e:
                    get_results.append(f"err:{e}")

        gt = threading.Thread(target=getter)
        st = threading.Thread(target=setter)
        gt.start()
        time.sleep(0.01)
        st.start()
        gt.join(timeout=120)
        st.join(timeout=60)
        # Every GET should succeed or return nil — never crash
        all_valid = all(r in ("ok", "nil") for r in get_results)
        check(
            f"GET during SET: no corruption ({len(get_results)} GETs)",
            all_valid and len(set_errors) == 0,
            f"results={get_results} set_errors={set_errors}",
        )
        make_conn().execute_command("DEL", key)

    # ── Buffer reuse stress ──
    if shutdown.should_continue():
        log.info("[race] ── Buffer reuse stress ──")
        errors = []
        rounds = 20
        for i in range(rounds):
            key = f"{{{hash_tag}}}race_buf_{i}"
            try:
                ok, src_crc = rdma_set(key)
                if not ok:
                    errors.append(f"round {i}: SET failed")
                    continue
                s_crc, c_crc = rdma_get_crc(key)
                if s_crc is not None and c_crc is not None and s_crc != c_crc:
                    errors.append(
                        f"round {i}: CRC mismatch server=0x{s_crc:08x} client=0x{c_crc:08x}"
                    )
                make_conn().execute_command("DEL", key)
            except Exception as e:
                errors.append(f"round {i}: {e}")
        check(
            f"Buffer reuse: {rounds} RDMA SET/GET/DEL rounds",
            len(errors) == 0,
            "; ".join(errors[:5]),
        )

    log.info("[race] Race tests complete: %d passed, %d failed", passed, failed)
    return passed, failed


# ─── Results & Summary ────────────────────────────────────────────────────────


def compute_summary(all_stats, duration, num_shards):
    """Aggregate per-worker stats into a final summary."""
    total_ops = 0
    total_gets = 0
    total_sets = 0
    total_errors = 0
    total_crc = 0
    total_staging_exhausted = 0
    all_latencies = []
    merged_by_size = defaultdict(list)
    merged_by_op = defaultdict(list)
    for ws in all_stats:
        snap = ws.snapshot()
        total_ops += snap["ops"]
        total_gets += snap["gets"]
        total_sets += snap["sets"]
        total_errors += snap["errors"]
        total_crc += snap["crc_mismatches"]
        total_staging_exhausted += snap["staging_exhausted"]
        all_latencies.extend(snap["latencies"])
        for size, lats in snap["latencies_by_size"].items():
            merged_by_size[size].extend(lats)
        for op, lats in snap["latencies_by_op"].items():
            merged_by_op[op].extend(lats)
    lat_values = sorted(lat for _, lat in all_latencies)
    p50 = lat_values[len(lat_values) // 2] if lat_values else 0
    p99 = lat_values[int(len(lat_values) * 0.99)] if lat_values else 0
    p_max = lat_values[-1] if lat_values else 0
    # Per-size breakdown
    by_size = {}
    for size in sorted(merged_by_size.keys()):
        lats = sorted(merged_by_size[size])
        count = len(lats)
        by_size[size] = {
            "ops": count,
            "p50_ms": round(lats[count // 2], 2) if lats else 0,
            "p99_ms": round(lats[int(count * 0.99)], 2) if lats else 0,
            "max_ms": round(lats[-1], 2) if lats else 0,
        }
    # Per-op breakdown
    by_op = {}
    for op in ("GET", "SET"):
        lats = sorted(merged_by_op.get(op, []))
        count = len(lats)
        by_op[op] = {
            "ops": count,
            "p50_ms": round(lats[count // 2], 2) if lats else 0,
            "p99_ms": round(lats[int(count * 0.99)], 2) if lats else 0,
            "max_ms": round(lats[-1], 2) if lats else 0,
        }
    return {
        "duration_s": duration,
        "shards": num_shards,
        "total_ops": total_ops,
        "gets": total_gets,
        "sets": total_sets,
        "errors": total_errors,
        "crc_mismatches": total_crc,
        "staging_exhausted": total_staging_exhausted,
        "latency_p50_ms": round(p50, 2),
        "latency_p99_ms": round(p99, 2),
        "latency_max_ms": round(p_max, 2),
        "by_size": by_size,
        "by_op": by_op,
    }


def print_summary(summary, race_results=None):
    """Print human-readable final summary."""
    print(f"  Duration:     {summary['duration_s']}s")
    if summary.get("workload"):
        print(f"  Workload:     {summary['workload']} "
              f"(SET {summary['set_pct']:.0f}% / GET {summary['get_pct']:.0f}%)")
    print(f"  Shards:       {summary['shards']}")
    print(f"  Total ops:    {summary['total_ops']:,}")
    print(f"    RDMA_GET:   {summary['gets']:,}")
    print(f"    RDMA_SET:   {summary['sets']:,}")
    print(f"  Errors:       {summary['errors']:,}")
    print(f"  CRC mismatch: {summary['crc_mismatches']:,}")
    print(f"  Staging exhausted: {summary['staging_exhausted']:,}")
    print(f"  Latency p50/p99/max: {summary['latency_p50_ms']:.1f}ms / "
          f"{summary['latency_p99_ms']:.1f}ms / {summary['latency_max_ms']:.1f}ms")
    if summary.get("by_op"):
        for op in ("GET", "SET"):
            d = summary["by_op"].get(op, {})
            if d.get("ops", 0) > 0:
                print(f"    {op} p50/p99/max: {d['p50_ms']:.1f}ms / "
                      f"{d['p99_ms']:.1f}ms / {d['max_ms']:.1f}ms ({d['ops']:,} ops)")
    if summary.get("by_size"):
        print(f"  {'─' * 56}")
        print(f"  {'Size':>10s}  {'Ops':>8s}  {'p50':>8s}  {'p99':>8s}  {'max':>8s}")
        for size, data in summary["by_size"].items():
            print(f"  {_human_bytes(size):>10s}  {data['ops']:>8,}  "
                  f"{data['p50_ms']:>7.1f}ms  {data['p99_ms']:>7.1f}ms  "
                  f"{data['max_ms']:>7.1f}ms")
    if summary.get("throughput_gbps"):
        print(f"  RDMA throughput (server): {summary['throughput_gbps']:.1f} Gbps "
              f"({_human_bytes(summary.get('rdma_bytes', 0))} transferred)")
    if summary.get("client_throughput_gbps"):
        print(f"  RDMA throughput (client EFA): {summary['client_throughput_gbps']:.1f} Gbps "
              f"({_human_bytes(summary.get('client_efa_bytes', 0))} transferred)")
    if summary.get("peak_throughput_gbps"):
        print(f"  Peak RDMA throughput: {summary['peak_throughput_gbps']:.1f} Gbps")
    if summary.get("peak_efa_registered_segments"):
        reg_bytes = summary.get("peak_efa_registered_bytes", 0)
        reg_pct = (reg_bytes / EFA_REGISTRATION_LIMIT_BYTES * 100) if reg_bytes else 0
        print(f"  Peak EFA registered: {summary['peak_efa_registered_segments']} segments "
              f"({_human_bytes(reg_bytes)}, {reg_pct:.0f}% of 380GiB limit)")
    if race_results:
        rp, rf = race_results
        print(f"  Race tests:   {rp} passed, {rf} failed")
    print(f"{'═' * 60}\n")


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _human_bytes(n):
    if n is None or n == 0:
        return "0"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.0f}{unit}" if n == int(n) else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def snapshot_rdma_bytes(connections):
    """Read rdma_read_bytes + rdma_write_bytes from each shard (server-side).
    Returns 0 if the counters are not present (older deployed builds)."""
    total = 0
    for conn in connections.values():
        try:
            lo = conn.info("largeobj")
            total += lo.get("largeobj_rdma_read_bytes", 0)
            total += lo.get("largeobj_rdma_write_bytes", 0)
        except Exception:
            pass
    return total


def snapshot_client_efa_bytes():
    """Read RDMA byte counters from the local EFA device via sysfs.

    The client is the passive RDMA target: the server fi_read pulls from our
    memory (SET path) and fi_write pushes into it (GET path). The matching
    sysfs counters on the client side are:
      rdma_read_resp_bytes  — bytes the server read FROM us (SET)
      rdma_write_recv_bytes — bytes the server wrote INTO us (GET)
    """
    total = 0
    import glob as _glob
    for counter in ("rdma_read_resp_bytes", "rdma_write_recv_bytes"):
        for path in _glob.glob(f"/sys/class/infiniband/*/ports/1/hw_counters/{counter}"):
            try:
                with open(path) as f:
                    total += int(f.read().strip())
            except Exception:
                pass
    return total


# ─── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="EFA RDMA scale test for valkey-large-object"
    )
    # Connection
    conn_group = parser.add_argument_group("connection")
    conn_group.add_argument("--cluster-endpoint", help="ElastiCache config endpoint")
    conn_group.add_argument("--host", default="127.0.0.1", help="Standalone host")
    conn_group.add_argument("--port", type=int, default=6379)
    conn_group.add_argument("--tls", action="store_true", help="Enable TLS")
    conn_group.add_argument("--cluster-mode", action="store_true")
    conn_group.add_argument(
        "--server-fabric-addr",
        help="Server fabric address hex (required for standalone RDMA)",
    )
    # Workload
    wl_group = parser.add_argument_group("workload")
    wl_group.add_argument("--duration", type=int, default=3600, help="Seconds to run")
    wl_group.add_argument(
        "--concurrency", type=int, default=8, help="Workers per shard"
    )
    wl_group.add_argument("--client-id", type=int, default=1, help="Client instance ID")
    wl_group.add_argument(
        "--workload", nargs="+", default=["balanced"],
        choices=list(WORKLOAD_RATIOS.keys()) + ["all"],
        help="Workload phase(s) to run in order (default: balanced). "
             "Use 'all' to run every workload sequentially.",
    )
    wl_group.add_argument(
        "--object-sizes", help="Comma-separated object sizes in bytes",
    )
    wl_group.add_argument(
        "--skip-race-tests", action="store_true", help="Skip race provocation tests",
    )
    wl_group.add_argument(
        "--multi-region", action="store_true",
        help="Split large objects across multiple memory regions (exercises "
             "multi-triple rkey/addr/len code path in RDMA_GET/SET)",
    )
    # Logging
    log_group = parser.add_argument_group("logging")
    log_group.add_argument("--logfile", default="efa_scale_test.log")
    log_group.add_argument("--max-log-size", type=int, default=50, help="MB")
    log_group.add_argument("--log-backups", type=int, default=5)
    log_group.add_argument("--results-file", default="results.json")

    args = parser.parse_args()

    # ── Setup logging ──
    setup_logging(args.logfile, args.max_log_size, args.log_backups)
    shutdown = GracefulShutdown()

    log.info("EFA Scale Test starting")
    log.info("  client_id=%d duration=%ds concurrency=%d workload=%s",
             args.client_id, args.duration, args.concurrency, args.workload)

    # ── Resolve workload phases ──
    if "all" in args.workload:
        workload_phases = list(WORKLOAD_RATIOS.keys())
    else:
        workload_phases = args.workload
    phase_duration = args.duration // len(workload_phases)
    log.info("  phases=%s (%ds each)", workload_phases, phase_duration)

    # ── Discover topology ──
    endpoint = args.cluster_endpoint or args.host
    if args.cluster_mode:
        shards = discover_cluster(endpoint, args.port, args.tls)
    else:
        shards = connect_standalone(args.host, args.port, args.tls)

    if not shards:
        log.error("No shards discovered — aborting")
        sys.exit(1)

    # ── Connect to each shard ──
    connections = setup_shard_connections(shards, args.tls)

    # ── Verify cluster state ──
    cluster_info = verify_cluster_state(connections, shards)
    # Get segment_size from the first shard
    first_info = next(iter(cluster_info.values()))
    segment_size = first_info.get("segment_size", 0)
    max_obj_size = first_info.get("max_object_size", 0)

    # ── Compute hash tags ──
    hash_tags = compute_shard_hash_tags(shards)

    # ── Filter object sizes ──
    if args.object_sizes:
        object_sizes = [int(s) for s in args.object_sizes.split(",")]
    else:
        object_sizes = list(OBJECT_SIZES)
    object_sizes = filter_object_sizes(object_sizes, max_obj_size)
    if not object_sizes:
        log.error("No valid object sizes after filtering — aborting")
        sys.exit(1)
    log.info("Object sizes: %s", [_human_bytes(s) for s in object_sizes])

    # ── Resolve server fabric address ──
    # For RDMA, we need each shard's fabric address for fabric_target's positional arg.
    # Each shard node has its own EFA device with a unique fabric address.
    # Discovery: start a throwaway fabric_target, do RDMA_HELLO per shard, get server addr.
    server_fabric_addrs = {}  # shard_id -> fabric_addr
    if args.server_fabric_addr:
        # Manual override: use the same address for all shards (standalone or single-shard).
        for shard_id in shards:
            server_fabric_addrs[shard_id] = args.server_fabric_addr
    else:
        for shard_id, shard_info in shards.items():
            try:
                addr = discover_server_fabric_addr(shard_info, args.tls)
                server_fabric_addrs[shard_id] = addr
                log.info(
                    "  shard=%s fabric address: %s...", shard_id, addr[:16]
                )
            except Exception as e:
                log.error("shard=%s fabric address discovery failed: %s", shard_id, e)
    if not server_fabric_addrs:
        log.error(
            "Could not determine any server fabric address. Pass --server-fabric-addr "
            "or ensure EFA is available on both client and server."
        )
        sys.exit(1)
    if len(server_fabric_addrs) < len(shards):
        log.warning(
            "Fabric address discovered for %d/%d shards — workers on missing shards "
            "will fail RDMA ops",
            len(server_fabric_addrs), len(shards),
        )

    # ── Flush existing keys for a clean start ──
    for shard_id, conn in connections.items():
        try:
            conn.execute_command("FLUSHALL")
            log.info("shard=%s FLUSHALL OK", shard_id)
        except Exception as e:
            log.warning("shard=%s FLUSHALL failed: %s", shard_id, e)

    # ── Start health monitor ──
    # all_stats is populated below when workers are created; the monitor reads
    # it on each pulse, so it sees 0 ops until workers start.
    all_stats = []
    health = HealthMonitor(connections, shards, shutdown, segment_size, all_stats)
    health.start()

    # ── Run race tests (before workload, to catch bugs early) ──
    race_results = None
    if not args.skip_race_tests:
        first_shard = next(iter(shards.keys()))
        race_results = run_race_tests(
            first_shard, shards[first_shard], hash_tags[first_shard],
            server_fabric_addrs[first_shard], args.tls, shutdown,
        )

    # ── Run workload phases ──
    phase_summaries = []
    overall_start = time.monotonic()
    overall_rdma_before = snapshot_rdma_bytes(connections)
    overall_client_efa_before = snapshot_client_efa_bytes()

    for phase_idx, workload_name in enumerate(workload_phases):
        if not shutdown.should_continue():
            break
        set_pct, get_pct = WORKLOAD_RATIOS[workload_name]
        log.info(
            "═══ Phase %d/%d: %s (SET %.0f%% / GET %.0f%%, %ds) ═══",
            phase_idx + 1, len(workload_phases), workload_name,
            set_pct * 100, get_pct * 100, phase_duration,
        )
        print(f"\n{'═' * 60}")
        print(f"  Phase {phase_idx + 1}/{len(workload_phases)}: {workload_name} "
              f"(SET {set_pct*100:.0f}% / GET {get_pct*100:.0f}%, {phase_duration}s)")
        print(f"{'═' * 60}")

        rdma_bytes_before = snapshot_rdma_bytes(connections)
        client_efa_before = snapshot_client_efa_bytes()
        phase_stats = []
        phase_shutdown = GracefulShutdown()
        worker_threads = []
        for shard_id, shard_info in shards.items():
            shard_fabric_addr = server_fabric_addrs.get(shard_id)
            if not shard_fabric_addr:
                continue
            for i in range(args.concurrency):
                wid = f"c{args.client_id}-s{shard_id}-w{i}"
                ws = WorkerStats()
                phase_stats.append(ws)
                all_stats.append(ws)
                t = threading.Thread(
                    target=worker_loop,
                    name=wid,
                    args=(
                        wid, shard_id, shard_info, hash_tags[shard_id],
                        shard_fabric_addr, args.tls, object_sizes,
                        workload_name, phase_shutdown, ws,
                        args.multi_region,
                    ),
                    daemon=True,
                )
                worker_threads.append(t)

        log.info(
            "Spawning %d workers (%d per shard × %d shards)",
            len(worker_threads), args.concurrency, len(shards),
        )
        phase_start = time.monotonic()
        for t in worker_threads:
            t.start()

        # Wait for phase duration or global shutdown
        phase_deadline = phase_start + phase_duration
        while time.monotonic() < phase_deadline and shutdown.should_continue():
            time.sleep(1)

        # Signal phase workers to stop
        phase_shutdown.shutdown_requested.set()
        for t in worker_threads:
            t.join(timeout=30)

        phase_elapsed = time.monotonic() - phase_start
        rdma_bytes_after = snapshot_rdma_bytes(connections)
        client_efa_after = snapshot_client_efa_bytes()
        rdma_bytes_delta = rdma_bytes_after - rdma_bytes_before
        client_efa_delta = client_efa_after - client_efa_before
        phase_throughput_gbps = (rdma_bytes_delta * 8) / (phase_elapsed * 1e9) if phase_elapsed > 0 else 0.0
        client_throughput_gbps = (client_efa_delta * 8) / (phase_elapsed * 1e9) if phase_elapsed > 0 else 0.0
        phase_summary = compute_summary(phase_stats, round(phase_elapsed), len(shards))
        phase_summary["workload"] = workload_name
        phase_summary["set_pct"] = set_pct * 100
        phase_summary["get_pct"] = get_pct * 100
        phase_summary["rdma_bytes"] = rdma_bytes_delta
        phase_summary["throughput_gbps"] = round(phase_throughput_gbps, 1)
        phase_summary["client_efa_bytes"] = client_efa_delta
        phase_summary["client_throughput_gbps"] = round(client_throughput_gbps, 1)
        phase_summaries.append(phase_summary)
        print_summary(phase_summary)
        log.info("Phase %d/%d complete: %s", phase_idx + 1, len(workload_phases), workload_name)

    overall_elapsed = time.monotonic() - overall_start

    # ── Signal global shutdown ──
    shutdown.shutdown_requested.set()

    # ── Aggregate summary ──
    overall_rdma_after = snapshot_rdma_bytes(connections)
    overall_client_efa_after = snapshot_client_efa_bytes()
    overall_rdma_delta = overall_rdma_after - overall_rdma_before
    overall_client_efa_delta = overall_client_efa_after - overall_client_efa_before
    overall_throughput_gbps = (overall_rdma_delta * 8) / (overall_elapsed * 1e9) if overall_elapsed > 0 else 0.0
    overall_client_throughput_gbps = (overall_client_efa_delta * 8) / (overall_elapsed * 1e9) if overall_elapsed > 0 else 0.0
    aggregate = compute_summary(all_stats, round(overall_elapsed), len(shards))
    aggregate["rdma_bytes"] = overall_rdma_delta
    aggregate["throughput_gbps"] = round(overall_throughput_gbps, 1)
    aggregate["client_efa_bytes"] = overall_client_efa_delta
    aggregate["client_throughput_gbps"] = round(overall_client_throughput_gbps, 1)
    aggregate["peak_throughput_gbps"] = round(health.peak_throughput_gbps, 1)
    aggregate["peak_efa_registered_segments"] = health.peak_registered_segments
    aggregate["peak_efa_registered_bytes"] = (
        health.peak_registered_segments * segment_size if segment_size else 0
    )
    print(f"\n{'═' * 60}")
    print("  AGGREGATE (all phases)")
    print(f"{'═' * 60}")
    print_summary(aggregate, race_results)

    # ── Write results JSON ──
    results = {
        "client_id": args.client_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "duration": args.duration,
            "phase_duration": phase_duration,
            "concurrency": args.concurrency,
            "workload_phases": workload_phases,
            "object_sizes": [_human_bytes(s) for s in object_sizes],
            "multi_region": args.multi_region,
            "shards": len(shards),
        },
        "phases": phase_summaries,
        "aggregate": aggregate,
        "race_tests": {"passed": race_results[0], "failed": race_results[1]}
        if race_results else None,
    }
    with open(args.results_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("Results written to %s", args.results_file)

    # Exit with failure if any errors or CRC mismatches
    if aggregate["errors"] > 0 or aggregate["crc_mismatches"] > 0:
        sys.exit(1)
    if race_results and race_results[1] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
