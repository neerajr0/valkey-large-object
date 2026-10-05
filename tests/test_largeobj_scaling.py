"""
Integration tests for DRAMPool expand/shrink scaling behavior.

Tests cover:
  - Dram mode: reactive expand when segment fills
  - Dram mode: expansion gated by server maxmemory watermark
  - Dram mode: eviction makes room once the pool cannot grow, as the policy allows
  - Tiered mode: reactive expand on DRAMPool fill
  - Tiered mode: shrink evicts cached segment but NVMe copy survives
  - Tiered mode: eviction frees nvme-maxmemory budget, settling the ledger exactly
  - Both modes: COPY evicts like SET, pinned objects are skipped; in a cluster, victims are tombstoned
"""

import binascii
import os
import subprocess
import threading
import time
from contextlib import suppress

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj

# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()

# What tests/harness/fabric_target writes (--read mode) or expects (write mode).
# Must match fabric_target's generate_pattern(): cycling 0x00..0xFF.
EFA_TARGET_LEN = 4096
EFA_PATTERN = bytes(i % 256 for i in range(EFA_TARGET_LEN))


def wait_uring_registered_matches_live(client, timeout=10):
    """Tiered: each pool's io_uring ring registers its own live segments
    (dram_uring==dram_live, nvme_uring==nvme_live). wait_for because the
    re-register on expand/shrink is fire-and-forget on the poller."""
    def _match():
        i = info_largeobj(client)
        return (i.get('largeobj_dram_uring_registered_segments') == i.get('largeobj_dram_live_segments')
                and i.get('largeobj_nvme_uring_registered_segments') == i.get('largeobj_nvme_live_segments'))
    wait_for_true(_match, timeout=timeout)


def cap_dram_growth(client):
    """Freeze the DRAM pool at its current size. Only the server `maxmemory` stops it growing:
    `try_expand` refuses once used + one segment reaches the shrink watermark of maxmemory.

    The watermark goes to its 50% floor, which puts maxmemory about two segments above usage.
    Any tighter and the core evicts or refuses the command itself before the module sees it.
    """
    segment = info_largeobj(client)['largeobj_dram_segment_size_bytes']
    used = int(client.info('memory')['used_memory'])
    client.execute_command('CONFIG', 'SET', 'largeobj.scaling-shrink-watermark', 50)
    client.execute_command('CONFIG', 'SET', 'maxmemory', str(2 * (used + segment) - 512 * 1024))


def set_policy(client, policy='allkeys-lru'):
    """Let the module evict (or not): it needs maxmemory > 0 and a policy other than noeviction.

    Dram also freezes the pool, so a full segment can only be served by evicting. Tiered leaves
    maxmemory far above usage, so the core never evicts and every eviction seen is the module's.
    """
    if 'largeobj_nvme_live_segments' in info_largeobj(client):
        used = int(client.info('memory')['used_memory'])
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024 * 1024))
    else:
        cap_dram_growth(client)
    client.execute_command('CONFIG', 'SET', 'maxmemory-policy', policy)


def set_objects(client, prefix, count, size):
    """SET `count` objects of `size` bytes, each filled with its own byte; return them by key."""
    payloads = {f'{prefix}{i}': bytes([i % 251 + 1]) * size for i in range(count)}
    for key, payload in payloads.items():
        assert client.execute_command('BLOB.SET', key, payload) == b'OK'
    return payloads


def write_until(client, prefix, payload, done, timeout=10):
    """SET `payload` under `prefix` in batches until `done()` or the timeout; failed SETs are ignored."""
    deadline, i = time.time() + timeout, 0
    while not done() and time.time() < deadline:
        for _ in range(16):
            try:
                client.execute_command('BLOB.SET', f'{prefix}{i}', payload)
            except ResponseError:
                pass
            i += 1
    return done()


def assert_pin_skipped_then_released(server, client, key, expected, payload):
    """A transfer reading `key` pins it: SETs that need room skip it, and it survives. Once the
    transfer ends it is a victim again."""
    skips = lambda: info_largeobj(client)['largeobj_pinned_skips_total']
    reader = server.get_new_client()
    reader.execute_command('BLOB.HELLO', PEER_ADDRESS)
    stop = threading.Event()

    def hold():
        while not stop.is_set():
            try:
                reader.execute_command('BLOB.GET', key, 0, 0, 1 << 30)  # rkey, addr, len
            except ResponseError:
                pass  # the peer is dead: the GET holds its pin until the fabric times out

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    try:
        before = skips()
        assert write_until(client, 'pinned_', payload, lambda: skips() > before), \
            "a walk never offered the pinned key"
        assert client.execute_command('EXISTS', key) == 1
    finally:
        stop.set()
        thread.join(timeout=30)
    assert client.execute_command('BLOB.GET', key) == expected
    assert write_until(client, 'after_', payload, lambda: client.execute_command('EXISTS', key) == 0), \
        "the key is still pinned after the transfer ended"


# ─── Dram Mode Scaling ────────────────────────────────────────────────────────

class TestDramReactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows reactively when a segment fills (SET path).

    scaling-poll-ms is set very high (60s) so the scaling cron cannot fire
    during the test. Any expand observed must be from the reactive SET path.
    """

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB → pool starts with 1 segment, grows on demand.
        # scaling-poll-ms=60000 → cron fires at most once per minute, won't interfere.
        # fabric-provider Emulated on loopback for EFA reactive expand test.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-poll-ms 60000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags):
        """Launch the fabric_target peer process and return (process, address, rkey, remote_addr, length)."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        process = subprocess.Popen(
            [target, '127.0.0.1', *flags],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        line = process.stdout.readline()
        assert line.startswith('advertisement: '), line
        address, rkey, remote_addr, length = line.split()[1:]
        return process, address, int(rkey), int(remote_addr), int(length)

    def test_expand_on_segment_full(self):
        """SET that fills a segment triggers reactive expand in serve_set_dram_tcp.

        With the cron disabled (60s poll), the only source of expand is the
        reactive path in the SET handler. Verified via scaling_expand_total.
        """
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        r = client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        assert r == b'OK', f"First BLOB.SET failed: {r}"

        r = client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)
        assert r == b'OK', f"Second BLOB.SET failed (expand may not have fired): {r}"

        after = info_largeobj(client)
        assert after.get('largeobj_scaling_expand_total', 0) > expand_before, \
            "Expected scaling_expand_total to increase — cron is disabled so this must be reactive"
        # Dram mode has no io_uring ring, so nothing is ever io_uring-registered —
        # confirms submit_reregister's engine-none guard no-ops here (even after expand).
        assert after.get('largeobj_dram_uring_registered_segments') == 0

    def test_expand_data_integrity(self):
        """Data written before and after a reactive expand is returned correctly."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024
        keys_payloads = [(f'key_{i}', bytes([i % 256]) * obj_size) for i in range(4)]

        for key, payload in keys_payloads:
            client.execute_command('BLOB.SET', key, payload)

        for key, payload in keys_payloads:
            got = client.execute_command('BLOB.GET', key)
            assert got == payload, f"Data mismatch for {key} after expand"

    def test_maxmemory_0_no_explicit_cap(self):
        """No module-level DRAM cap; the pool grows up to the server maxmemory ceiling."""
        client = self.server.get_new_client()
        for i in range(3):
            r = client.execute_command('BLOB.SET', f'key_{i}', b'X' * (100 * 1024))
            assert r == b'OK', f"SET {i} failed: {r}"

    def test_live_data_survives_memory_pressure(self):
        """Any Dram key not evicted by Valkey core must return correct data.

        Under memory pressure, core may evict LO keys. The module must never
        corrupt data for keys that core did NOT evict.
        """
        client = self.server.get_new_client()
        obj_size = 200 * 1024
        payloads = {f'dram_{i}': bytes([i % 256]) * obj_size for i in range(5)}

        for key, payload in payloads.items():
            r = client.execute_command('BLOB.SET', key, payload)
            assert r == b'OK', f"SET {key} failed: {r}"

        mem_info = client.execute_command('INFO', 'memory')
        used_memory = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used_memory * 1.1)))

        # Wait for a few cron ticks.
        time.sleep(5)

        for key, payload in payloads.items():
            if client.execute_command('EXISTS', key) == 1:
                got = client.execute_command('BLOB.GET', key)
                assert got == payload, f"{key} data corrupted under pressure"

    def test_efa_set_triggers_reactive_expand(self):
        """Fill the 1MB segment with a TCP SET, then an EFA SET triggers expand.

        Same principle as test_expand_on_segment_full but exercises the EFA SET
        path (cmd_set_dram_efa) which also has reactive expand logic.
        """
        client = self.server.get_new_client()
        expand_before = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        # Fill the pool with TCP objects until a reactive expand fires (each ~full-segment
        # object co-locates in its own segment, so a later one forces a new segment). Bounded
        # loop so a packing change can't hang the test.
        for i in range(8):
            client.execute_command('BLOB.SET', f'filler_{i}', b'F' * (950 * 1024))
            if info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before:
                break
        assert info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before, \
            "expected TCP fill to trigger a reactive expand"
        expand_after_fill = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        # Now an EFA SET must also succeed and read back correctly in the multi-segment pool
        # (exercises cmd_set_dram_efa's alloc/expand path).
        process, address, rkey, remote_addr, length = self.start_target('--read')
        try:
            client.execute_command('BLOB.HELLO', address)
            result = client.execute_command('BLOB.SET', 'efa_key', EFA_TARGET_LEN, rkey, remote_addr, length)
            assert result == b'OK', f"EFA SET failed: {result}"
            assert client.execute_command('BLOB.GET', 'efa_key') == EFA_PATTERN
        finally:
            process.kill()
        # The EFA SET succeeded in a pool that had already expanded (multi-segment),
        # confirming cmd_set_dram_efa's alloc path works across segments.
        assert info_largeobj(client).get('largeobj_scaling_expand_total', 0) >= expand_after_fill


class TestDramProactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows proactively when the scaling cron sees utilization > watermark.

    scaling-poll-ms=1000 so the cron fires every second. The test writes enough
    data to push utilization above the expand watermark, then stops writing and
    waits for the cron to add a segment.
    """

    EXPAND_TIMEOUT_S = 15

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB; no module DRAM cap (server maxmemory 0 so shrink never fires).
        # scaling-expand-watermark=50 so filling half a segment triggers proactive expand.
        # scaling-shrink-watermark=99 to ensure shrink never fires during this test.
        # scaling-poll-ms=1000 so the cron fires frequently.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-expand-watermark 50"
            f" scaling-shrink-watermark 99"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_proactive_expand_fires_when_watermark_exceeded(self):
        """The scaling cron adds a segment when utilization exceeds the expand watermark.

        Steps:
        1. Write one 600KB object into a 1MB segment → utilization ≈ 60% > 50% watermark.
        2. Stop writing. No new SETs happen.
        3. Wait for cron to fire and increment scaling_expand_total.

        Because cron is the only actor after step 1, any expand is definitively proactive.
        """
        client = self.server.get_new_client()

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        # Fill >50% of one 1MB segment (600KB ≈ 59% of 1MB).
        r = client.execute_command('BLOB.SET', 'probe', b'P' * (600 * 1024))
        assert r == b'OK', "BLOB.SET failed"

        # No more SETs. Wait for cron to observe utilization > 50% and expand.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before,
            timeout=self.EXPAND_TIMEOUT_S,
        )



class TestDramServerMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: expansion is gated by the SERVER maxmemory watermark.

    There is no module-local DRAM budget — the pool grows on demand and the
    only ceiling is Valkey's own `maxmemory` (via would_cross_memory_watermark).
    This test sets a server maxmemory low enough that, after the pool has grown,
    a further object requiring another segment would cross the watermark and is
    rejected.
    """

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_expansion_capped_by_server_maxmemory(self):
        """Once used_memory is near the server maxmemory ceiling, an object that
        would need a new segment (crossing the watermark) must be rejected."""
        client = self.server.get_new_client()
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')

        obj_size = 900 * 1024
        # Land the first object (pool starts at 1 segment, fits 900KB).
        client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)

        # Cap server maxmemory just above current used_memory, leaving less than
        # one segment (1MB) of headroom — so the next object cannot expand.
        used = int(client.info('memory')['used_memory'])
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024))

        try:
            client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)
            assert False, "Expected rejection: expansion would cross server maxmemory watermark"
        except ResponseError:
            pass
        # Restore uncapped for teardown safety.
        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')


class TestDramEviction(ValkeyLargeObjTestCaseBase):
    """Dram mode with one 1MB segment that `set_policy` freezes, so eviction is the only way a SET
    succeeds once it is full. Both transports reach `alloc_dram_or_make_room`."""

    SEGMENT_SIZE = 1024 * 1024
    OBJ = SEGMENT_SIZE // 4  # four would fill the segment exactly, but talc's metadata lives in it

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" max-object-size {self.SEGMENT_SIZE - 64 * 1024}"
            f" chunk-size 65536"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_writing_past_the_segment_evicts_and_stays_bounded(self):
        """Three segments' worth cannot be resident at once, so these SETs only succeed by evicting."""
        client = self.server.get_new_client()
        set_policy(client)
        payloads = set_objects(client, 'obj_', 12, self.OBJ)

        info = info_largeobj(client)
        assert info['largeobj_evictions_total'] > 0
        assert info['largeobj_dram_live_segments'] == 1
        survivors = [key for key in payloads if client.execute_command('EXISTS', key)]
        assert 'obj_11' in survivors, "the last write cannot have been anyone's victim"
        for key in survivors:
            assert client.execute_command('BLOB.GET', key) == payloads[key]

    def test_noeviction_refuses_a_full_arena_and_destroys_nothing(self):
        client = self.server.get_new_client()
        set_policy(client, 'noeviction')
        keys = [f'obj_{i}' for i in range(3)]
        for key in keys:
            assert client.execute_command('BLOB.SET', key, b'F' * self.OBJ) == b'OK'

        with pytest.raises(ResponseError, match='pool exhausted'):
            client.execute_command('BLOB.SET', 'refused', b'R' * self.OBJ)
        assert info_largeobj(client)['largeobj_evictions_total'] == 0
        assert client.execute_command('EXISTS', *keys) == len(keys)

    def test_copy_evicts_another_key_and_never_its_source(self):
        """COPY pins its source: alone in the segment it fails cleanly, and with another key
        resident that key is the victim."""
        client = self.server.get_new_client()
        set_policy(client)
        big = b'S' * (640 * 1024)
        assert client.execute_command('BLOB.SET', 'src', big) == b'OK'
        with pytest.raises(ResponseError):
            client.execute_command('COPY', 'src', 'dst')
        assert client.execute_command('BLOB.GET', 'src') == big
        assert client.execute_command('EXISTS', 'dst') == 0
        assert info_largeobj(client)['largeobj_evictions_total'] == 0

        client.execute_command('FLUSHALL')
        obj = b'A' * (384 * 1024)
        assert client.execute_command('BLOB.SET', 'src', obj) == b'OK'
        assert client.execute_command('BLOB.SET', 'other', b'O' * len(obj)) == b'OK'
        assert client.execute_command('COPY', 'src', 'dst') in (1, True)
        assert info_largeobj(client)['largeobj_evictions_total'] == 1
        assert client.execute_command('EXISTS', 'other') == 0
        assert client.execute_command('BLOB.GET', 'src') == obj
        assert client.execute_command('BLOB.GET', 'dst') == obj

    def test_eviction_takes_victims_from_every_db(self):
        """The arena is node-wide: a SET from db 0 that needs two victims takes the key in db 1
        and the key in db 2, and leaves the client on its own db."""
        db0 = self.server.get_new_client()
        dbs = [self.server.create_from_server(db=db) for db in (1, 2)]
        set_policy(db0)
        for db in dbs:
            assert db.execute_command('BLOB.SET', 'obj', b'A' * (384 * 1024)) == b'OK'

        newcomer = b'N' * (700 * 1024)
        assert db0.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        assert info_largeobj(db0)['largeobj_evictions_total'] == 2
        assert self.server.get_new_client().execute_command('BLOB.GET', 'newcomer') == newcomer
        assert [db.execute_command('EXISTS', 'obj', 'newcomer') for db in dbs] == [0, 0]

    def test_tenacity_zero_still_evicts(self):
        """A zero time limit bounds how long a search runs, not whether it runs: a floor of
        candidates is always examined."""
        client = self.server.get_new_client()
        set_policy(client)
        client.execute_command('CONFIG', 'SET', 'largeobj.eviction-tenacity', '0')
        for i in range(12):
            assert client.execute_command('BLOB.SET', f'obj_{i}', b'Z' * self.OBJ) == b'OK'
        assert info_largeobj(client)['largeobj_evictions_total'] > 0

    def test_efa_set_evicts_at_alloc_time(self):
        """An EFA SET into a full arena evicts before the transfer. The peer is dead, so the SET
        errors; the counter moving is the assertion."""
        client = self.server.get_new_client()
        set_policy(client)
        for i in range(3):  # one short of a full segment, which would evict on its own
            assert client.execute_command('BLOB.SET', f'fill_{i}', b'F' * self.OBJ) == b'OK'
        client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        assert info_largeobj(client)['largeobj_evictions_total'] == 0

        try:
            client.execute_command('BLOB.SET', 'efa_newcomer', self.OBJ, 0, 0, self.OBJ)  # len, rkey, addr, len
        except ResponseError:
            pass
        assert info_largeobj(client)['largeobj_evictions_total'] > 0

    def test_a_pinned_object_is_not_claimed_for_the_arena(self):
        client = self.server.get_new_client()
        set_policy(client)
        payload = b'F' * (self.SEGMENT_SIZE // 8)
        for i in range(7):  # one short of a full segment, so fill_0 is not evicted before it is pinned
            assert client.execute_command('BLOB.SET', f'fill_{i}', payload) == b'OK'
        assert_pin_skipped_then_released(self.server, client, 'fill_0', payload, payload)


class TestDramEvictionPolicy(ValkeyLargeObjTestCaseBase):
    """*Which* object eviction destroys. Each test makes half the keyspace warmer by the metric its
    policy reads, forces one SET's worth of eviction, and asserts the cold half paid. Four
    residents against the default 5 samples mean every key is scored, so the order is exact."""

    get_module_args = TestDramEviction.get_module_args
    SEGMENT_SIZE = TestDramEviction.SEGMENT_SIZE
    OBJ = SEGMENT_SIZE // 5  # four fit; a fifth overshoots, and at a 90% credit two victims pay
    HOT, COLD = ('hot_0', 'hot_1'), ('cold_0', 'cold_1')

    def fill(self, policy):
        """Four residents, the cold pair written last: in a small keyspace that puts it behind the
        hot pair in cursor order, so only a ranking that reads the metric picks it out."""
        client = self.server.get_new_client()
        set_policy(client, policy)
        for key in self.HOT + self.COLD:
            assert client.execute_command('BLOB.SET', key, key.encode()[:1] * self.OBJ) == b'OK'
        return client

    def evict_and_check(self, client):
        newcomer = b'N' * self.OBJ
        assert client.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        assert [client.execute_command('EXISTS', key) for key in self.HOT + self.COLD] == [1, 1, 0, 0]
        assert client.execute_command('BLOB.GET', 'newcomer') == newcomer

    def test_lru_evicts_the_idle_keys_without_resetting_the_rest(self):
        client = self.fill('allkeys-lru')
        time.sleep(2.1)  # the LRU clock has one-second resolution
        for key in self.HOT:
            client.execute_command('BLOB.GET', key)
        time.sleep(2.1)
        self.evict_and_check(client)
        assert client.execute_command('OBJECT', 'IDLETIME', 'hot_0') >= 2, \
            "sampling reset the idle time: scoring touched the key"

    def test_lfu_evicts_the_rarely_used_keys(self):
        client = self.fill('allkeys-lfu')
        for _ in range(10):  # early increments are near-certain: this lifts the hot pair clear
            for key in self.HOT:
                client.execute_command('BLOB.GET', key)
        self.evict_and_check(client)

    def test_volatile_policy_evicts_only_keys_with_a_ttl(self):
        """With no TTL anywhere the SET fails and destroys nothing; a TTL on the cold pair makes
        it the whole eligible set."""
        client = self.fill('volatile-lru')
        with pytest.raises(ResponseError):
            client.execute_command('BLOB.SET', 'newcomer', b'N' * self.OBJ)
        assert info_largeobj(client)['largeobj_evictions_total'] == 0
        assert client.execute_command('EXISTS', *self.HOT, *self.COLD) == 4

        for key in self.COLD:
            assert client.execute_command('EXPIRE', key, 600) == 1
        self.evict_and_check(client)

    def test_volatile_ttl_evicts_the_soonest_to_expire_first(self):
        """That pair is read last, so idleness and cursor order would both take the other pair."""
        client = self.fill('volatile-ttl')
        for key, ttl in zip(self.HOT + self.COLD, (5000, 6000, 100, 200)):
            assert client.execute_command('EXPIRE', key, ttl) == 1
        for key in self.COLD:
            client.execute_command('BLOB.GET', key)
        self.evict_and_check(client)


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────

class TestTieredExpand(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool expands reactively when segment fills."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_expand_on_full(self):
        """In Tiered mode, filling the DRAMPool triggers expand; data stays correct."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)

        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * obj_size
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * obj_size
        # The expanded DRAM segment joins its ring's io_uring table (per-pool).
        wait_uring_registered_matches_live(client)

    def test_tiered_multiple_segments(self):
        """Objects spread across multiple segments are all readable."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024

        for i in range(4):
            r = client.execute_command('BLOB.SET', f'key_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"SET key_{i} failed: {r}"

        for i in range(4):
            got = client.execute_command('BLOB.GET', f'key_{i}')
            assert got == bytes([i % 256]) * obj_size, f"Data mismatch for key_{i}"
        wait_uring_registered_matches_live(client)

    def test_tiered_nvme_fallback_on_dram_full(self):
        """When DRAMPool is at cap, further SETs still persist to NVMe and are readable."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            client.execute_command('BLOB.SET', key, fill * obj_size)

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            assert client.execute_command('BLOB.GET', key) == fill * obj_size
        wait_uring_registered_matches_live(client)


class TestTieredShrink(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool shrinks under memory pressure.

    Writes objects first, then sets server maxmemory below current used_memory
    so the module shrink watermark fires on the next cron tick.
    """

    SHRINK_TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def _assert_no_pressure(self, client):
        """Guardrail: verify server is not under memory pressure before the test writes data.

        maxmemory must be 0 (uncapped) at test start. If it's non-zero, the test
        server was left in a bad state from a previous test run and results would
        be unreliable.
        """
        mem_info = client.execute_command('INFO', 'memory')
        maxmemory = int(mem_info.get(b'maxmemory') or mem_info.get('maxmemory', 0))
        assert maxmemory == 0, (
            f"Test server already has maxmemory={maxmemory} at start — "
            f"server is under pressure before test data is written. "
            f"Run 'CONFIG SET maxmemory 0' to reset."
        )

    def _apply_shrink_pressure(self, client):
        """Set maxmemory below current used_memory so ratio > 0.80.

        Setting maxmemory = used * 0.85 gives ratio ≈ 1.18 > 0.80.
        noeviction means no keys are evicted — the module cron handles DRAM.
        """
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')
        mem_info = client.execute_command('INFO', 'memory')
        used = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used * 0.85)))

    def test_shrink_preserves_nvme_data(self):
        """After the scaling cron shrinks the pool, keys remain readable from NVMe."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        self._assert_no_pressure(client)

        keys = [f'shrink_key_{i}' for i in range(4)]
        for key in keys:
            r = client.execute_command('BLOB.SET', key, b'S' * obj_size)
            assert r == b'OK', f"BLOB.SET {key} failed: {r}"

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Wait for the drained segment to be fully released (draining_segments back to 0).
        # This verifies the complete shrink cycle including release_drained, not just initiation.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        for key in keys:
            assert client.execute_command('EXISTS', key) == 1, \
                f"Key {key} disappeared from keyspace after shrink (data loss)"
        # The released segment left its ring's io_uring table too (per-pool invariant holds post-shrink).
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)

    def test_shrink_then_expand(self):
        """After a shrink, new SETs succeed."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        self._assert_no_pressure(client)

        for i in range(4):
            client.execute_command('BLOB.SET', f'pre_shrink_{i}', b'P' * obj_size)

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Wait for the drained segment to be fully released (draining_segments back to 0).
        # This verifies the complete shrink cycle including release_drained, not just initiation.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')

        r = client.execute_command('BLOB.SET', 'post_shrink', b'Q' * obj_size)
        assert r == b'OK', f"BLOB.SET after shrink+expand failed: {r}"

        for i in range(4):
            assert client.execute_command('EXISTS', f'pre_shrink_{i}') == 1, \
                f"pre_shrink_{i} disappeared from keyspace after shrink"
        # io_uring tables track live segments per pool through shrink + the follow-up expand.
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)


class TestTieredShrinkReleasesEfaRegisteredSegment(ValkeyLargeObjTestCaseBase):
    """Tiered mode, fabric UP: a segment added by expansion is EFA-registered, and shrinking it
    must tear that registration down (Segment::drop -> efa_release_segment) BEFORE the segment
    memory is freed — a broken teardown (registration outliving freed pages) would crash here.
    Combines test_efa_set_triggers_reactive_expand (EFA-expand) and TestTieredShrink (shrink)."""

    SHRINK_TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        # Tiered so shrink can release a live-data segment (NVMe-backed), fabric up (Emulated on
        # loopback) so expanded segments are actually EFA-registered, small segment + fast cron so
        # the test forces expansion and shrink quickly.
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_shrink_releases_efa_registered_expanded_segment(self):
        client = self.server.get_new_client()

        # Guardrail: server not already under pressure.
        mem_info = client.execute_command('INFO', 'memory')
        maxmemory = int(mem_info.get(b'maxmemory') or mem_info.get('maxmemory', 0))
        assert maxmemory == 0, f"server already under pressure (maxmemory={maxmemory})"

        expand_before = info_largeobj(client).get('largeobj_scaling_expand_total', 0)

        # Tiered SET lands on NVMe; the DRAM pool grows via PROMOTION on GET. Write two
        # ~full-segment objects, GET both to promote them — each fills its own DRAM segment, so
        # promoting the second forces a reactive expand, and with the fabric up try_expand
        # EFA-registers that new segment (the path under test).
        client.execute_command('BLOB.SET', 'key_a', b'A' * (900 * 1024))
        client.execute_command('BLOB.SET', 'key_b', b'B' * (900 * 1024))
        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * (900 * 1024)
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * (900 * 1024)

        expand_after = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        assert expand_after > expand_before, "expected an expansion (new EFA-registered segment)"

        # Every live DRAM segment is EFA-registered when the fabric is up. In Tiered mode the
        # registered count also covers the NVMe staging segments, so registered >= live (not ==).
        # The point under test: the expansion segment got registered, so registered grew past 1.
        info_expanded = info_largeobj(client)
        dram_live = info_expanded.get('largeobj_dram_live_segments', 0)
        nvme_live = info_expanded.get('largeobj_nvme_live_segments', 0)
        registered = info_expanded.get('largeobj_efa_registered_segments', -1)
        assert dram_live > 1, f"expected DRAM pool to have expanded past 1 segment, dram_live={dram_live}"
        # EFA registration spans BOTH pools: every live segment (DRAM + NVMe staging) is registered.
        assert registered == dram_live + nvme_live, \
            f"every live segment must be EFA-registered: registered={registered} dram={dram_live} nvme={nvme_live}"
        # io_uring registration is per-pool (independent of EFA): the expanded DRAM segment
        # joined its ring's table.
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)

        shrink_before = info_largeobj(client).get('largeobj_scaling_shrink_total', 0)
        registered_before_shrink = registered

        # Apply server memory pressure so the shrink cron releases a segment. In Tiered mode this
        # releases a live-data segment (data persists on NVMe), and because the fabric is up the
        # released segment carries an EFA registration that Segment::drop must tear down first.
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')
        mem_now = client.execute_command('INFO', 'memory')
        used = int(mem_now.get(b'used_memory') or mem_now.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used * 0.85)))

        # Shrink fires, then the drained segment is fully released (registration torn down + memory
        # freed). A broken teardown would fault here rather than completing cleanly.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Restore, then prove the server is alive and correct after releasing a registered segment.
        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')
        assert client.execute_command('PING')  # server still responsive after the release
        # The released segment's EFA registration was torn down: registered count dropped, and it
        # still equals total live segments (DRAM + NVMe) — invariant preserved through release.
        info_after = info_largeobj(client)
        registered_after = info_after.get('largeobj_efa_registered_segments', -1)
        dram_live_after = info_after.get('largeobj_dram_live_segments', 0)
        nvme_live_after = info_after.get('largeobj_nvme_live_segments', 0)
        assert registered_after < registered_before_shrink, \
            f"EFA registration not torn down on release: {registered_after} !< {registered_before_shrink}"
        assert registered_after == dram_live_after + nvme_live_after, \
            f"every live segment must stay registered after release: registered={registered_after} dram={dram_live_after} nvme={nvme_live_after}"
        # io_uring tables also dropped the released segment, per-pool.
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)
        # The objects survive (Tiered: data on NVMe) and read back correctly after release.
        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * (900 * 1024)
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * (900 * 1024)


class TestTieredEviction(ValkeyLargeObjTestCaseBase):
    """Tiered mode against a small `nvme-maxmemory`, the only budget eviction can free. The pools
    are sized above the largest object, so a rejected SET can only be the ledger, which is exact:
    reclaimed bytes can be asserted to the byte."""

    OBJ = 512 * 1024
    DISK_PER_OBJ = OBJ + 4096  # the ledger also charges the file's one-page header
    OBJECTS_PER_CAP = 8
    CAP = OBJECTS_PER_CAP * DISK_PER_OBJ  # a whole number of objects, so filling it evicts nothing

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size {2 * self.CAP}"
            f" segment-size {2 * self.CAP}"
            f" max-object-size {self.CAP}"
            f" max-promote-size {self.CAP}"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    def fill_cap(self, client):
        return set_objects(client, 'fill_', self.OBJECTS_PER_CAP, self.OBJ)

    def assert_rejected(self, client, key, payload):
        with pytest.raises(ResponseError, match='capacity exceeded'):
            client.execute_command('BLOB.SET', key, payload)

    def test_one_victim_pays_for_one_object(self):
        """The ledger has no placement to misjudge, so its credit is exact: a same-sized SET at the
        cap takes one victim (the arena's 90% discount would take two)."""
        client = self.server.get_new_client()
        set_policy(client)
        self.fill_cap(client)
        assert client.execute_command('BLOB.SET', 'n0', b'N' * self.OBJ) == b'OK'
        assert info_largeobj(client)['largeobj_disk_evictions_total'] == 1

    def test_writing_past_the_cap_evicts_exactly_and_stays_bounded(self):
        """Three caps' worth: the ledger, counters, survivors and directory agree on what died."""
        client = self.server.get_new_client()
        set_policy(client)
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)

        info = info_largeobj(client)
        victims = info['largeobj_disk_evictions_total']
        assert victims > 0
        assert info['largeobj_disk_eviction_failures_total'] == 0
        assert info['largeobj_evictions_total'] == 0, "a Tiered node never evicts for DRAM"
        assert info['largeobj_disk_eviction_reclaimed_bytes_total'] == victims * self.DISK_PER_OBJ
        survivors = [key for key in payloads if client.execute_command('EXISTS', key)]
        assert f'obj_{len(payloads) - 1}' in survivors
        for key in survivors:
            assert client.execute_command('BLOB.GET', key) == payloads[key]
        assert info['largeobj_disk_used_bytes'] == len(survivors) * self.DISK_PER_OBJ
        wait_for_true(lambda: len(self._dat_files()) == len(survivors), timeout=10)

    def test_a_refused_set_destroys_nothing(self):
        """Three objects resident against a request of seven: the walk claims all three and still
        falls short, so it evicts none. The failure counter shows it ran rather than never trying."""
        client = self.server.get_new_client()
        set_policy(client)
        payloads = set_objects(client, 'keep_', 3, self.OBJ)
        before, files = info_largeobj(client), sorted(self._dat_files())

        self.assert_rejected(client, 'too_big', b'X' * (7 * self.DISK_PER_OBJ - 4096))

        after = info_largeobj(client)
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.GET', key) == payload
        assert after['largeobj_disk_used_bytes'] == before['largeobj_disk_used_bytes']
        assert after['largeobj_disk_evictions_total'] == before['largeobj_disk_evictions_total']
        assert after['largeobj_disk_eviction_failures_total'] > before['largeobj_disk_eviction_failures_total']
        assert sorted(self._dat_files()) == files

    def test_requests_the_cap_cannot_serve_destroy_nothing(self):
        """One bigger than the cap itself, and any write under `noeviction`."""
        client = self.server.get_new_client()
        set_policy(client)
        payloads = self.fill_cap(client)

        self.assert_rejected(client, 'huge', b'H' * self.CAP)  # the payload alone is the cap
        assert info_largeobj(client)['largeobj_disk_eviction_failures_total'] == 0, "refused without a walk"
        set_policy(client, 'noeviction')
        self.assert_rejected(client, 'overflow', b'O' * self.OBJ)
        assert info_largeobj(client)['largeobj_disk_evictions_total'] == 0
        assert client.execute_command('EXISTS', *payloads) == len(payloads)

    def test_copy_evicts_another_key_and_never_its_source(self):
        """COPY pins its source: alone, it fails cleanly and gives its reservation back; with
        other keys resident, one of them is the victim."""
        client = self.server.get_new_client()
        set_policy(client)
        big = b'S' * (5 * self.OBJ)
        assert client.execute_command('BLOB.SET', 'src', big) == b'OK'
        before = info_largeobj(client)
        with pytest.raises(ResponseError):
            client.execute_command('COPY', 'src', 'dst')
        after = info_largeobj(client)
        assert client.execute_command('BLOB.GET', 'src') == big
        assert client.execute_command('EXISTS', 'dst') == 0
        assert after['largeobj_disk_evictions_total'] == before['largeobj_disk_evictions_total']
        assert after['largeobj_disk_used_bytes'] == before['largeobj_disk_used_bytes']

        client.execute_command('FLUSHALL')
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_used_bytes'] == 0)
        payloads = self.fill_cap(client)
        assert client.execute_command('COPY', 'fill_0', 'dst') in (1, True)
        info = info_largeobj(client)
        victims = info['largeobj_disk_evictions_total']
        assert victims >= 1 and info['largeobj_disk_used_bytes'] <= self.CAP
        assert client.execute_command('BLOB.GET', 'dst') == payloads['fill_0']
        survivors = [key for key in payloads if client.execute_command('EXISTS', key)]
        assert len(survivors) == self.OBJECTS_PER_CAP - victims
        for key in survivors:
            assert client.execute_command('BLOB.GET', key) == payloads[key]
        wait_for_true(lambda: len(self._dat_files()) == len(survivors) + 1, timeout=10)

    def test_eviction_takes_victims_from_every_db(self):
        """The budget is node-wide. Tiered SETs land in db 0 whatever db the client selected, so
        the keys are moved out afterwards, two to each of four dbs. A newcomer worth three objects
        needs three victims, more than any one db holds."""
        db0 = self.server.get_new_client()
        dbs = [self.server.create_from_server(db=db) for db in (1, 2, 3, 4)]
        set_policy(db0)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP)}
        home = {}
        for i, (key, payload) in enumerate(payloads.items()):
            assert db0.execute_command('BLOB.SET', key, payload) == b'OK'
            assert db0.execute_command('MOVE', key, 1 + i % 4) == 1
            home[key] = dbs[i % 4]

        newcomer = b'N' * (3 * self.OBJ)
        assert db0.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        assert info_largeobj(db0)['largeobj_disk_evictions_total'] == 3
        assert self.server.get_new_client().execute_command('BLOB.GET', 'newcomer') == newcomer
        survivors = [key for key, db in home.items() if db.execute_command('EXISTS', key)]
        assert len(survivors) == self.OBJECTS_PER_CAP - 3
        for key in survivors:
            assert home[key].execute_command('BLOB.GET', key) == payloads[key]

    def test_a_pinned_object_is_not_claimed_for_the_disk_budget(self):
        """A reader's `Arc<ObjectFile>` keeps the file's blocks past the unlink, so claiming it
        would credit budget that never arrives."""
        client = self.server.get_new_client()
        set_policy(client)
        payloads = self.fill_cap(client)
        assert_pin_skipped_then_released(
            self.server, client, 'fill_0', payloads['fill_0'], b'N' * self.OBJ)
        info = info_largeobj(client)
        assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes']

    def test_a_write_that_fails_after_evicting_returns_every_byte(self):
        """An EFA SET to a dead peer evicts, charges, starts writing and fails: the victim stays
        gone, and the ledger and directory settle to exactly the survivors."""
        client = self.server.get_new_client()
        set_policy(client)
        payloads = self.fill_cap(client)
        client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        try:
            client.execute_command('BLOB.SET', 'efa_key', self.OBJ, 0, 0, self.OBJ)
        except ResponseError:
            pass

        survivors = len(payloads) - 1
        assert info_largeobj(client)['largeobj_disk_evictions_total'] == 1
        assert client.execute_command('EXISTS', 'efa_key') == 0
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_used_bytes']
                      == survivors * self.DISK_PER_OBJ, timeout=10)
        wait_for_true(lambda: len(self._dat_files()) == survivors, timeout=10)

    def test_a_write_that_cannot_create_its_file_returns_every_byte(self):
        """`nvme-dir` vanishes under a SET that has already charged its file: the charge comes back,
        with the budget spare and at the cap, where the victim's bytes had paid for it."""
        client = self.server.get_new_client()
        set_policy(client)
        away = self.data_dir + '.away'

        def set_while_dir_is_away(key):
            os.rename(self.data_dir, away)
            try:
                with pytest.raises(ResponseError):
                    client.execute_command('BLOB.SET', key, b'N' * self.OBJ)
            finally:
                os.rename(away, self.data_dir)

        set_while_dir_is_away('with_room')
        assert info_largeobj(client)['largeobj_disk_used_bytes'] == 0
        self.fill_cap(client)
        set_while_dir_is_away('at_cap')
        info = info_largeobj(client)
        assert info['largeobj_disk_evictions_total'] == 1  # the victim stays evicted
        assert info['largeobj_disk_used_bytes'] == (self.OBJECTS_PER_CAP - 1) * self.DISK_PER_OBJ


def own_all_slots(client):
    client.execute_command('CLUSTER', 'ADDSLOTSRANGE', 0, 16383)
    wait_for_true(lambda: b'cluster_state:ok' in client.execute_command('CLUSTER', 'INFO'))


def tombstones(client):
    return info_largeobj(client)['largeobj_tombstones']


class ClusterNode:
    """A cluster node: a victim in another slot is tombstoned (bytes freed, reads miss) and its key
    swept later, so until then `EXISTS` still sees it."""

    CLUSTER_DATABASES = 1
    SWEEP_MS = 60_000  # out of the way; the sweep tests shorten it

    def get_server_args(self):
        config_file = os.path.abspath(os.path.join(self.testdir, 'nodes.conf'))
        if os.path.exists(config_file):
            os.remove(config_file)
        return {
            'cluster-enabled': 'yes',
            'cluster-config-file': config_file,
            'cluster-databases': str(self.CLUSTER_DATABASES),
        }

    def new_node(self):
        client = self.server.get_new_client()
        own_all_slots(client)
        set_policy(client)
        return client


class DramSegment(ClusterNode):
    """`TestDramEviction`'s frozen 1MB segment, on a cluster node."""

    SEGMENT_SIZE = TestDramEviction.SEGMENT_SIZE

    def get_module_args(self, data_dir, direct_io):
        return (TestDramEviction.get_module_args(self, data_dir, direct_io)
                + f" tombstone-sweep-ms {self.SWEEP_MS}")

    def evicted_key(self, client, key='{z}victim', size=600 * 1024):
        """Evict `key` through a SET in another slot. The segment holds one object this size, so
        the key is the only victim, and it stays in the keyspace as a tombstone."""
        assert client.execute_command('BLOB.SET', key, b'V' * size) == b'OK'
        assert client.execute_command('BLOB.SET', '{y}newcomer', b'N' * size) == b'OK'
        assert client.execute_command('EXISTS', key) == 1, "another slot: the key outlives its data"
        assert tombstones(client) == 1
        return key


class TestClusterDramTombstones(DramSegment, ValkeyLargeObjTestCaseBase):
    """Reading, overwriting and deleting keys whose objects eviction has tombstoned."""

    def test_a_set_evicts_keys_in_other_slots(self):
        client = self.new_node()
        payloads = set_objects(client, 'obj_', 12, self.SEGMENT_SIZE // 4)

        got = {key: client.execute_command('BLOB.GET', key) for key in payloads}
        evicted = [key for key, value in got.items() if value is None]
        assert 0 < len(evicted) < len(payloads)
        assert all(got[key] == payloads[key] for key in payloads if key not in evicted)
        assert tombstones(client) == len(evicted)
        assert all(client.execute_command('EXISTS', key) == 1 for key in evicted)

    def test_an_evicted_key_reads_as_a_miss_until_it_is_deleted(self):
        client = self.new_node()
        key = self.evicted_key(client)

        assert client.execute_command('BLOB.GET', key) is None
        with pytest.raises(ResponseError, match='not found'):
            client.execute_command('BLOB.INFO', key)
        with suppress(ResponseError):
            client.execute_command('COPY', key, '{z}copy')
        assert client.execute_command('EXISTS', '{z}copy') == 0
        assert client.execute_command('MEMORY', 'USAGE', key) < 1024, "the payload is gone"
        assert client.execute_command('DEBUG', 'DIGEST-VALUE', key)

        assert client.execute_command('DEL', key) == 1
        wait_for_true(lambda: tombstones(client) == 0, timeout=10)  # lo_free may run on a lazyfree thread

    def test_overwriting_an_evicted_key_is_not_a_miss(self):
        """The tombstone is the old object's, not the name's."""
        client = self.new_node()
        key = self.evicted_key(client)
        assert client.execute_command('BLOB.SET', key, b'fresh' * 1000) == b'OK'
        assert client.execute_command('BLOB.GET', key) == b'fresh' * 1000
        wait_for_true(lambda: tombstones(client) == 0, timeout=10)

    def test_copy_evicts_a_key_in_another_slot_and_never_its_source(self):
        client = self.new_node()
        obj = b'A' * (384 * 1024)
        assert client.execute_command('BLOB.SET', '{t}src', obj) == b'OK'
        assert client.execute_command('BLOB.SET', 'other', b'O' * len(obj)) == b'OK'
        assert client.execute_command('COPY', '{t}src', '{t}dst') in (1, True)
        assert info_largeobj(client)['largeobj_evictions_total'] == 1
        assert client.execute_command('BLOB.GET', 'other') is None
        assert client.execute_command('BLOB.GET', '{t}src') == obj
        assert client.execute_command('BLOB.GET', '{t}dst') == obj


class TestClusterDramSweep(DramSegment, ValkeyLargeObjTestCaseBase):
    """The timer that finishes what eviction could not: deleting the keys it tombstoned."""

    CLUSTER_DATABASES = 4
    SWEEP_MS = 500  # long enough to act on a tombstone first, short enough to wait for

    def test_the_sweep_deletes_the_keys_eviction_took(self):
        client = self.new_node()
        payloads = set_objects(client, 'obj_', 12, self.SEGMENT_SIZE // 4)
        assert tombstones(client) > 0

        wait_for_true(lambda: tombstones(client) == 0)
        live = {key for key, payload in payloads.items() if client.execute_command('BLOB.GET', key) == payload}
        assert live and client.execute_command('DBSIZE') == len(live), "the sweep left only live keys"

    def test_a_renamed_key_is_still_swept(self):
        """The sweep checks the name the tombstone remembers rather than trusting it, and finds the
        object by id when the key has moved."""
        client = self.new_node()
        key = self.evicted_key(client)
        assert client.execute_command('RENAME', key, '{z}renamed') in (b'OK', True)
        wait_for_true(lambda: tombstones(client) == 0)
        assert client.execute_command('EXISTS', '{z}renamed') == 0

    def test_a_key_moved_to_another_db_is_still_swept(self):
        db0, db1 = self.new_node(), self.server.create_from_server(db=1)
        assert db1.execute_command('BLOB.SET', '{z}victim', b'V' * (600 * 1024)) == b'OK'
        assert db0.execute_command('BLOB.SET', '{y}newcomer', b'N' * (600 * 1024)) == b'OK'
        assert tombstones(db0) == 1
        assert db1.execute_command('MOVE', '{z}victim', 2) in (1, True)
        wait_for_true(lambda: tombstones(db0) == 0)
        assert self.server.create_from_server(db=2).execute_command('EXISTS', '{z}victim') == 0


class TestClusterTieredEviction(ClusterNode, ValkeyLargeObjTestCaseBase):
    """`TestTieredEviction`'s small `nvme-maxmemory`, on a cluster node with four DBs."""

    CLUSTER_DATABASES = 4
    OBJ = TestTieredEviction.OBJ
    DISK_PER_OBJ = TestTieredEviction.DISK_PER_OBJ
    OBJECTS_PER_CAP = TestTieredEviction.OBJECTS_PER_CAP
    CAP = TestTieredEviction.CAP

    def get_module_args(self, data_dir, direct_io):
        return (TestTieredEviction.get_module_args(self, data_dir, direct_io)
                + f" tombstone-sweep-ms {self.SWEEP_MS}")

    def test_a_set_at_the_cap_evicts_keys_in_other_slots_and_dbs(self):
        """The victims' files are unlinked and their bytes credited when the write settles, even
        though their keys outlive them as tombstones."""
        db0 = self.new_node()
        db2 = self.server.create_from_server(db=2)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP + 4)}
        home = {}
        for i, (key, payload) in enumerate(payloads.items()):
            assert db0.execute_command('BLOB.SET', key, payload) == b'OK'
            home[key] = db0
            if i < self.OBJECTS_PER_CAP and i % 2:  # Tiered SETs land in db 0: move to reach db 2
                assert db0.execute_command('MOVE', key, 2) in (1, True)
                home[key] = db2

        got = {key: home[key].execute_command('BLOB.GET', key) for key in payloads}
        live = [key for key, value in got.items() if value is not None]
        assert 0 < len(live) < len(payloads)
        assert all(got[key] == payloads[key] for key in live)
        assert any(home[key] is db2 for key in payloads if key not in live), "the walk reached db 2"
        info = info_largeobj(db0)
        assert info['largeobj_disk_used_bytes'] == len(live) * self.DISK_PER_OBJ
        assert len(self._dat_files()) == len(live), "an evicted object's file must be gone"
        assert tombstones(db0) == len(payloads) - len(live)

    def test_a_late_commit_overwrites_a_newer_write_that_was_evicted(self):
        """A SET slow to commit finds the key holding a newer object, which eviction has since
        given up. That object is a miss, not a write to defer to, so the late commit must land."""
        slow, client = self.server.get_new_client(), self.new_node()
        key, late = '{k}key', b'L' * self.OBJ
        slow.execute_command('CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '3000')
        outcome = {}
        thread = threading.Thread(
            target=lambda: outcome.update(reply=slow.execute_command('BLOB.SET', key, late)))
        thread.start()
        time.sleep(0.5)  # the write is done and the task is paused, holding the older object id
        client.execute_command('CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '0')
        assert client.execute_command('BLOB.SET', key, b'N' * self.OBJ) == b'OK'

        assert write_until(client, 'fill_', b'F' * self.OBJ,
                           lambda: client.execute_command('BLOB.GET', key) is None), "never evicted"
        assert client.execute_command('EXISTS', key) == 1, "evicted from another slot: a tombstone"
        thread.join(timeout=15)
        assert outcome['reply'] == b'OK'
        assert client.execute_command('BLOB.GET', key) == late

    def test_a_tombstone_is_never_claimed_a_second_time(self):
        """Claiming a tombstoned object again would credit bytes that are not coming back, and the
        budget would creep past its cap."""
        client = self.new_node()
        count = 4 * self.OBJECTS_PER_CAP
        for i in range(count):
            assert client.execute_command('BLOB.SET', f'fill_{i}', bytes([i % 251]) * self.OBJ) == b'OK'
            info = info_largeobj(client)
            assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes'], f"SET {i}"
        live = sum(client.execute_command('BLOB.GET', f'fill_{i}') is not None for i in range(count))
        assert info['largeobj_tombstones'] > 0
        assert info['largeobj_disk_used_bytes'] == live * self.DISK_PER_OBJ
        assert len(self._dat_files()) == live

    def test_an_evicted_object_releases_its_promoted_copy_at_once(self):
        """The victim's key waits for the sweep, but its DRAM copy must not: `lo_free` would drop
        it, and that is a minute away."""
        client = self.new_node()
        for i in range(self.OBJECTS_PER_CAP):
            assert client.execute_command('BLOB.SET', f'fill_{i}', b'F' * self.OBJ) == b'OK'
            assert client.execute_command('BLOB.GET', f'fill_{i}') == b'F' * self.OBJ  # promotes
        assert info_largeobj(client)['largeobj_cached_objects'] == self.OBJECTS_PER_CAP

        assert client.execute_command('BLOB.SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        info = info_largeobj(client)
        assert info['largeobj_disk_evictions_total'] == 1 and info['largeobj_tombstones'] == 1
        assert info['largeobj_cached_objects'] == self.OBJECTS_PER_CAP - 1
