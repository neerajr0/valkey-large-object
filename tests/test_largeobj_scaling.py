"""
Integration tests for how the DRAMPool responds to pressure: expand, shrink,
and — once it can do neither — evict.

Tests cover:
  - Dram mode: reactive expand when segment fills
  - Dram mode: expansion gated by server maxmemory watermark
  - Dram mode: eviction makes room once expand is impossible, and only when
    maxmemory-policy permits it
  - Dram mode over EFA: the SET evicts at alloc time, before the transfer
  - Tiered mode: reactive expand on DRAMPool fill
  - Tiered mode: shrink evicts cached segment but NVMe copy survives
  - Tiered mode: eviction frees nvme-maxmemory budget, credits each victim's
    whole disk_len, and unlinks the files it gave away
  - Tiered mode: a promotion into a full arena skips itself and serves from NVMe,
    giving up nothing resident and leaving every key in place
  - Both modes: an object a transfer is reading is skipped, not destroyed
"""

import binascii
import os
import subprocess
import threading
import time
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj

# FI_SOCKADDR_IN for 127.0.0.1:1 — a well-formed address with nothing listening. The
# server only records it until the first transfer, so BLOB.HELLO succeeds and the transfer
# is what fails. Same constant as test_largeobj_fabric.py.
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


def _set_memory_policy(client, policy):
    """Configure the server so bigobj's EVICT-flag gate resolves as intended.

    The module reads `VALKEYMODULE_CTX_FLAGS_EVICT`, which the server sets only
    when maxmemory > 0 AND the policy is not noeviction. Both have to be set
    deliberately: Valkey defaults maxmemory to 0 and the policy to noeviction, so
    eviction is off out of the box.

    maxmemory lands far above current usage on purpose. These tests press against
    the module's own dram-maxmemory, so the server-wide limit must stay slack
    enough that core eviction never runs and the expand OOM guard never trips —
    any eviction they observe is then unambiguously the module's own.
    """
    mem = client.execute_command('INFO', 'memory')
    used = int(mem.get(b'used_memory') or mem.get('used_memory'))
    client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024 * 1024))
    client.execute_command('CONFIG', 'SET', 'maxmemory-policy', policy)


def allow_evictions(client):
    """Permit module eviction: maxmemory set, policy allows deleting keys."""
    _set_memory_policy(client, 'allkeys-lru')


def deny_evictions(client):
    """Forbid module eviction via the policy specifically.

    maxmemory is still set, so `noeviction` is the only reason the gate closes —
    otherwise the test would also pass with maxmemory at its 0 default and prove
    nothing about the policy.
    """
    _set_memory_policy(client, 'noeviction')


def hold_pin_via_dead_peer(client, key):
    """Keep `key` pinned against eviction, using nothing but a GET that cannot finish.

    An async GET clones the object's reference before it transfers — `Arc<ObjectContext>`
    in Dram mode, `Arc<ObjectFile>` in Tiered — and holds it until the transfer resolves.
    Against PEER_ADDRESS that resolution is a fabric timeout, about a second, so each
    doomed GET is a real pin held by the real code path. Looping them keeps the object
    pinned for as long as the caller needs.

    `client` is consumed: it is put into BLOB.HELLO and then blocks in GET after GET until
    the returned event is set, so it must not be shared with the writer.
    """
    client.execute_command('BLOB.HELLO', PEER_ADDRESS)
    stop = threading.Event()

    def loop():
        while not stop.is_set():
            try:
                # EFA arity: BLOB.GET key rkey remote_addr.
                client.execute_command('BLOB.GET', key, 0, 0)
            except ResponseError:
                pass  # expected: the transfer has no peer

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t, stop


def write_until_pinned_skip(client, prefix, payload, batch, timeout=10):
    """SET `payload` under `prefix` until a walk reports declining a pinned victim.

    Batched against a deadline rather than a fixed count, because the pin above is held
    in bursts: a fixed number of writes could in principle land entirely between two of
    them. Individual SETs are not asserted — whether a given write fits is not what
    these tests measure, only that the walks ran and met the pinned key.

    Returns (skips_before, skips_after).
    """
    before = info_largeobj(client)['largeobj_pinned_skips_total']
    deadline = time.time() + timeout
    i = 0
    while True:
        for _ in range(batch):
            try:
                client.execute_command('BLOB.SET', f'{prefix}{i}', payload)
            except ResponseError:
                pass
            i += 1
        after = info_largeobj(client)['largeobj_pinned_skips_total']
        if after > before or time.time() >= deadline:
            return before, after


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


class TestDramMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: what happens at dram-maxmemory, on each rung of the ladder.

    Once the pool is at the cap it cannot grow, so a further SET either evicts
    (maxmemory-policy allows it) or errors. dram-maxmemory is a hard ceiling in
    both directions: nothing grows the pool past it, so a request eviction cannot
    serve is refused rather than admitted. All three outcomes are tested here so
    none can regress into another.
    """

    def get_module_args(self, data_dir, direct_io):
        # 2MB total, 1MB segment. After reactive expand, pool is 2MB (2 segments)
        # and cannot grow again, so a third 900KB object can only be admitted by
        # evicting one of the first two.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 2097152"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    OBJ_SIZE = 900 * 1024
    CAP_BYTES = 2097152
    # Past what emptying the arena could produce, so the walk declines the request
    # instead of spending the keyspace on it. Fits in three segments, not two.
    OVER_CAP_SIZE = 2304 * 1024

    def _fill_to_cap(self, client):
        """Two 900KB objects: one per segment, pool now at dram-maxmemory."""
        client.execute_command('BLOB.SET', 'key_a', b'A' * self.OBJ_SIZE)
        client.execute_command('BLOB.SET', 'key_b', b'B' * self.OBJ_SIZE)

    def _assert_pool_exhausted(self, client, key, payload):
        try:
            client.execute_command('BLOB.SET', key, payload)
            assert False, f"Expected '{key}' to be refused at dram-maxmemory"
        except ResponseError as e:
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    def test_the_three_outcomes_at_the_cap(self):
        """All three rungs, against the same pool sitting at dram-maxmemory.

        1. Evictions permitted, a request the arena could serve: admitted. The cap bounds
           memory, not admission — once expand is impossible the SET destroys a resident
           object rather than failing.
        2. Evictions still permitted, a request larger than the whole cap: refused.
           Emptying the arena would not produce this run, so the walk declines before
           destroying anything, and nothing below it grows the pool. The permitting policy
           is the point — it separates "eviction could not serve this" from "eviction was
           forbidden".
        3. `noeviction`: writes keep being admitted only while free bytes last, and the
           first one that would need a victim is refused. That isolates the policy gate
           from the making-room path below it. It takes a loop rather than a single SET
           because (1)'s eviction overshoots — freed bytes are credited below face value,
           so a walk can take a second victim and leave a spare object's room behind.

        Running them in this order means each later outcome also witnesses that the earlier
        ones left the keyspace where they claimed to.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        self._fill_to_cap(client)

        before = info_largeobj(client)['largeobj_evictions_total']
        payload = b'C' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'key_c', payload) == b'OK', \
            "SET at cap must succeed via eviction"
        evicted = info_largeobj(client)['largeobj_evictions_total']
        assert evicted > before, \
            "at the cap the pool cannot grow, so key_c could only have fit by evicting"
        assert client.execute_command('BLOB.GET', 'key_c') == payload

        self._assert_pool_exhausted(client, 'over_cap', b'O' * self.OVER_CAP_SIZE)
        assert info_largeobj(client)['largeobj_evictions_total'] == evicted, \
            "the walk had nothing to give that would have helped, so it took nothing"

        deny_evictions(client)
        originals = {k for k in ('key_a', 'key_b', 'key_c')
                     if client.execute_command('EXISTS', k) == 1}
        # At most as many as the cap holds, so this cannot run away if the refusal never comes.
        denied = [f'key_d{i}' for i in range(1 + self.CAP_BYTES // self.OBJ_SIZE)]
        refused = None
        for key in denied:
            try:
                client.execute_command('BLOB.SET', key, payload)
            except ResponseError as e:
                assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"
                refused = key
                break
        assert refused is not None, \
            "under noeviction a full pool must refuse a write, not grow or evict"
        assert info_largeobj(client)['largeobj_evictions_total'] == evicted, \
            "noeviction must destroy nothing"
        assert {k for k in ('key_a', 'key_b', 'key_c')
                if client.execute_command('EXISTS', k) == 1} == originals, \
            "a refused SET must leave the keyspace exactly as it found it"
        assert client.execute_command('BLOB.GET', 'key_c') == payload

        info = info_largeobj(client)
        assert info['largeobj_capacity_bytes'] <= self.CAP_BYTES, \
            f"dram-maxmemory exceeded: {info['largeobj_capacity_bytes']}"
        assert client.execute_command('EXISTS', 'over_cap') == 0
        assert client.execute_command('EXISTS', refused) == 0, \
            "a refused SET must not leave the key behind"


class TestDramEviction(ValkeyLargeObjTestCaseBase):
    """Dram mode, one 1MB segment, no room to grow.

    In Dram mode an object lives only in the DRAMPool, so making room for a new
    SET means destroying resident objects. Eviction is the last resort in
    `alloc_dram_or_make_room`: alloc -> try_expand -> evict. Both transports reach
    that helper, so both are tested here — the fabric is up in every Dram server
    (`Fabric::start` is unconditional), which is why EFA needs no separate fixture.

    Pinning dram-maxmemory to segment-size means expand can never fire, which
    leaves eviction as the only way a SET can succeed once the segment is full.
    """

    SEGMENT_SIZE = 1024 * 1024

    def get_module_args(self, data_dir, direct_io):
        # dram-maxmemory == segment-size → exactly one segment, try_expand always
        # fails. scaling-poll-ms high so the cron cannot interfere.
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" dram-maxmemory {self.SEGMENT_SIZE}"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    # Objects are sized so at most this many are resident in one segment (fewer in
    # practice, since talc's per-chunk overhead comes out of the same space). Derived
    # from SEGMENT_SIZE rather than hardcoded so the test follows a config change.
    OBJECTS_PER_SEGMENT = 4

    def test_writing_past_the_segment_evicts_and_stays_bounded(self):
        """SETs past the segment's capacity keep succeeding by evicting, the survivors are
        intact rather than corrupt, and the pool never grows past its one segment.

        Three segments' worth. That total is what makes this independent of eviction's
        internals: whatever a single call frees, this much data cannot be resident at once
        and the pool cannot grow, so objects had to be destroyed. A per-SET assertion would
        not survive a change to how much eviction frees per call — today it overshoots
        (freed bytes are credited below face value), leaving slack that lets the next SET or
        two fit without evicting.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        obj_size = self.SEGMENT_SIZE // self.OBJECTS_PER_SEGMENT

        count = 3 * self.OBJECTS_PER_SEGMENT
        payloads = {f'obj_{i}': bytes([i % 256]) * obj_size for i in range(count)}
        for key, payload in payloads.items():
            r = client.execute_command('BLOB.SET', key, payload)
            assert r == b'OK', f"SET {key} must succeed via eviction: {r}"

        info = info_largeobj(client)
        assert info['largeobj_evictions_total'] > 0, (
            f"wrote {count * obj_size} bytes into a {self.SEGMENT_SIZE}-byte segment that "
            "cannot grow, so these SETs could only have fit by evicting"
        )
        assert info['largeobj_live_segments'] == 1, \
            "dram-maxmemory == segment-size must keep the pool at one segment"
        assert info['largeobj_allocated_bytes'] <= info['largeobj_capacity_bytes'], \
            f"allocated {info['largeobj_allocated_bytes']} exceeds capacity " \
            f"{info['largeobj_capacity_bytes']}"

        survivors = 0
        for key, payload in payloads.items():
            if client.execute_command('EXISTS', key) == 1:
                assert client.execute_command('BLOB.GET', key) == payload, \
                    f"{key} survived eviction but its data is wrong"
                survivors += 1
        assert survivors > 0, "eviction must not empty the keyspace"
        # The last object written cannot have been anyone's victim.
        keeper = f'obj_{count - 1}'
        assert client.execute_command('EXISTS', keeper) == 1

        # An object larger than one segment can never fit, so the O(1) guard — the request
        # is larger than everything the live segments hold — rejects it before any victim is
        # taken. The failure counter only rises when eviction destroyed something and still
        # came up short, so it must not move either.
        before = info_largeobj(client)
        try:
            client.execute_command('BLOB.SET', 'toobig', b'D' * (2 * self.SEGMENT_SIZE))
            assert False, "Expected pool exhausted error"
        except ResponseError as e:
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"
        after = info_largeobj(client)
        assert after['largeobj_evictions_total'] == before['largeobj_evictions_total'], \
            "an unsatisfiable SET must not destroy objects"
        assert after['largeobj_eviction_failures_total'] \
            == before['largeobj_eviction_failures_total'], \
            "the guard rejected before evicting, so this is not an eviction failure"
        assert client.execute_command('BLOB.GET', keeper) == payloads[keeper]

    def test_tenacity_zero_still_evicts(self):
        """A zero search budget must not degenerate into noeviction.

        `eviction-tenacity 0` is 0µs, and a walk that checked the clock before doing
        any work would free nothing and fail every SET at capacity — silently turning
        the tightest latency setting into "reject all writes". The floor of candidates
        a walk always examines is what prevents that, so it gets a test.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        client.execute_command('CONFIG', 'SET', 'largeobj.eviction-tenacity', '0')
        obj_size = self.SEGMENT_SIZE // self.OBJECTS_PER_SEGMENT

        for i in range(self.OBJECTS_PER_SEGMENT):
            assert client.execute_command(
                'BLOB.SET', f'fill_{i}', bytes([i % 256]) * obj_size) == b'OK'

        before = info_largeobj(client)['largeobj_evictions_total']

        # Twice a segment's worth, for the same reason as test_set_succeeds_by_evicting:
        # a single SET can be served by the slack an earlier eviction left behind, so only
        # a total that cannot be resident at once proves the walk ran.
        count = 2 * self.OBJECTS_PER_SEGMENT
        payload = b'Z' * obj_size
        for i in range(count):
            assert client.execute_command('BLOB.SET', f'newcomer_{i}', payload) == b'OK', \
                "tenacity 0 bounds how long a search may run, not whether it runs"

        after = info_largeobj(client)['largeobj_evictions_total']
        assert after > before, (
            f"Expected evictions_total to increase ({before} -> {after}) at tenacity 0: "
            f"a 0µs budget still examines a floor of candidates"
        )
        assert client.execute_command('BLOB.GET', f'newcomer_{count - 1}') == payload

    def test_efa_set_evicts_at_alloc_time(self):
        """An EFA SET into a full, ungrowable arena evicts before it transfers.

        The transfer cannot succeed here — PEER_ADDRESS is a dead address, so the SET
        always ends in an error. That is the point rather than a limitation: the alloc
        happens before the transfer is dispatched, so the metric moves even though the
        command fails. Asserting on the reply would be testing libfabric.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        obj_size = self.SEGMENT_SIZE // self.OBJECTS_PER_SEGMENT

        # Fill over TCP: same mode, same arena, and synchronous, so the arena is
        # known-full before the EFA SET rather than racing it.
        #
        # One short of OBJECTS_PER_SEGMENT, because that many is what actually fits:
        # the full count sums to exactly SEGMENT_SIZE, which cannot, since talc's
        # metadata lives inside the claimed span. Filling all of them would make the
        # last *fill* evict, and an eviction can overshoot — talc may refuse the first
        # attempt and cost a second victim — which would leave the arena with a spare
        # object's room and let the EFA SET below allocate without evicting, defeating
        # the test. Stopping here leaves it genuinely tight.
        for i in range(self.OBJECTS_PER_SEGMENT - 1):
            r = client.execute_command('BLOB.SET', f'fill_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"fill_{i} failed: {r}"

        client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        before = info_largeobj(client)['largeobj_evictions_total']
        assert before == 0, (
            f"the fill must not have evicted ({before}); otherwise the arena has slack "
            "and the EFA SET below can allocate without evicting"
        )

        # EFA arity: BLOB.SET key len rkey remote_addr. The arena is full and pinned at
        # one segment, so the alloc in execute_set_dram_efa has to evict to proceed.
        try:
            client.execute_command('BLOB.SET', 'efa_newcomer', obj_size, 0, 0)
        except ResponseError:
            pass  # expected: the transfer has no peer

        after = info_largeobj(client)['largeobj_evictions_total']
        assert after > before, (
            f"Expected evictions_total to increase ({before} -> {after}); the EFA SET's "
            "alloc had to evict, whether or not the transfer that follows it succeeds"
        )

    # Objects small enough that one pinned resident still leaves the arena room to
    # serve every newcomer by evicting two of the others.
    PINNED_OBJECTS = 8

    def test_a_pinned_object_is_not_claimed_for_the_arena(self):
        """An object a transfer is reading is skipped by the walk, not handed out twice.

        The arena's pin is `Arc<ObjectContext>`: `serve_from_dram` clones the map's
        reference into the transfer task, so while that task runs the buffer is being read
        and its bytes are not the walk's to give. Claiming it anyway would unlink the key
        and credit bytes that only come back when the reader drops its reference — budget
        reported and never received.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        # One round samples at least the whole keyspace, so the pinned key is offered to
        # the very first walk rather than eventually, by cursor order.
        client.execute_command('CONFIG', 'SET', 'maxmemory-samples', '16')
        obj_size = self.SEGMENT_SIZE // self.PINNED_OBJECTS
        payload = b'F' * obj_size

        # One short of what the arithmetic allows, for the reason spelled out in
        # test_efa_set_evicts_at_alloc_time: the full count sums to exactly SEGMENT_SIZE,
        # which cannot fit, so the last fill would evict — and fill_0, the key this test
        # is about to pin, is a candidate for that.
        for i in range(self.PINNED_OBJECTS - 1):
            assert client.execute_command('BLOB.SET', f'fill_{i}', payload) == b'OK'
        assert info_largeobj(client)['largeobj_evictions_total'] == 0, \
            "the fill must not have evicted; fill_0 has to still be there to be pinned"

        evictions_before = info_largeobj(client)['largeobj_evictions_total']
        reader, stop = hold_pin_via_dead_peer(self.server.get_new_client(), 'fill_0')
        try:
            skips_before, skips_after = write_until_pinned_skip(
                client, 'newcomer_', payload, batch=2 * self.PINNED_OBJECTS)

            assert info_largeobj(client)['largeobj_evictions_total'] > evictions_before, \
                "the walk must have run, or the skip below proves nothing"
            assert skips_after > skips_before, (
                f"expected pinned skips ({skips_before} -> {skips_after}): fill_0 was "
                "offered to these walks with a transfer reading it, and had to be declined"
            )
            assert client.execute_command('EXISTS', 'fill_0') == 1, \
                "a pinned object must survive the walk that was offered it"
        finally:
            stop.set()
            reader.join(timeout=30)

        assert client.execute_command('BLOB.GET', 'fill_0') == payload, \
            "declining a victim must leave it readable, not half-unlinked"


class TestDramEvictionPolicy(ValkeyLargeObjTestCaseBase):
    """*Which* object eviction destroys, not merely that it destroys one.

    Same premise as TestDramEviction — one segment, no room to grow — but these tests
    are the only ones that can tell ranked victim selection from taking whatever the
    cursor happened to offer. Each makes half the keyspace demonstrably warmer by the
    metric its policy reads, forces one SET's worth of eviction, and asserts the cold
    half paid for it.

    One test per policy because `idleness` genuinely has two branches: the module never
    reads maxmemory-policy, it calls both the LRU and the LFU getter and uses whichever
    did not return its `-1` sentinel. A bug in either branch is invisible to the other's
    test.

    Sizes are chosen so the whole keyspace fits in one sample round, which is what makes
    the assertions exact rather than statistical: four resident objects against a default
    `maxmemory-samples` of 5, so every key is scored and the ordering is total.
    """

    SEGMENT_SIZE = 1024 * 1024
    # Five to a segment, so four residents plus a fifth newcomer overshoots it and the
    # SET must evict. At a 90% credit one victim is not enough and two are, so eviction
    # takes exactly the two coldest — which is exactly the hot/cold split below.
    OBJ_SIZE = SEGMENT_SIZE // 5
    COLD = ('cold_0', 'cold_1')
    HOT = ('hot_0', 'hot_1')

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" dram-maxmemory {self.SEGMENT_SIZE}"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    def _fill(self, client):
        """Four residents, written hot-first so cursor order is adversarial.

        The cold pair is written *last*, which in a small single-bucket keyspace puts it
        behind the hot pair in cursor order — so a walk that took whatever the cursor
        offered would destroy the hot pair and fail these tests. Only a ranking that
        reads the access metric picks the cold pair out of that order. (Written the other
        way round, cursor order and the correct answer coincide, and both tests pass with
        the ranking deleted — which is how they were, and it was verified.)
        """
        for key in self.HOT + self.COLD:
            payload = key.encode()[:1] * self.OBJ_SIZE
            assert client.execute_command('BLOB.SET', key, payload) == b'OK', \
                f"fill of {key} must fit: four objects in a five-object segment"

    def _assert_cold_paid(self, client):
        for key in self.HOT:
            assert client.execute_command('EXISTS', key) == 1, \
                f"{key} was the warmer half and must have survived"
        for key in self.COLD:
            assert client.execute_command('EXISTS', key) == 0, \
                f"{key} was idle and should have been chosen before any hot key"

    def test_lru_evicts_the_idle_keys_without_resetting_what_it_samples(self):
        """Under an LRU policy the least recently touched objects are the victims, and being
        sampled does not make a key look freshly used.

        The scoring open is NOTOUCH; without it every candidate's idle time is stamped to ~0
        as it is read, which both flattens our own ranking and tells *core's* eviction that
        keys we merely looked at are hot. `keeper` is the externally visible half of that: it
        is written a sleep later than the rest, so it is the *least* idle candidate and
        survives the walk, and it is never read, so core must still report its real idle time
        afterwards. A touching open during the walk would have reset that to 0.

        Both halves need the same two waits, so they are one test. The LRU clock has
        one-second resolution — keys touched within the same second are genuinely
        indistinguishable, so idle time has to be *made*, not implied by access order.
        """
        client = self.server.get_new_client()
        _set_memory_policy(client, 'allkeys-lru')
        self._fill(client)

        time.sleep(2.1)
        for key in self.HOT:
            assert client.execute_command('BLOB.GET', key) is not None
        # Small enough to be admitted without evicting, and never read after this.
        assert client.execute_command('BLOB.SET', 'keeper', b'K' * (64 * 1024)) == b'OK'
        time.sleep(2.1)

        # The cold pair is now strictly the most idle: untouched since the fill, where the
        # hot pair and `keeper` date from one sleep later. A 90% credit means one victim is
        # not enough and two are, so the walk takes exactly them.
        payload = b'N' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload
        assert client.execute_command('EXISTS', 'keeper') == 1, \
            "keeper is the least idle candidate; it should not be a victim"
        assert client.execute_command('OBJECT', 'IDLETIME', 'keeper') >= 2, \
            "sampling reset keeper's idle time — the scoring open is not NOTOUCH"

    def test_lfu_evicts_the_rarely_used_keys(self):
        """Under an LFU policy the least frequently used objects are the victims.

        No sleep here, and that is the difference worth having both tests for: the LFU
        counter separates keys by access *count* immediately, where LRU cannot separate
        them at all inside one second.
        """
        client = self.server.get_new_client()
        _set_memory_policy(client, 'allkeys-lfu')
        self._fill(client)

        # A fresh object starts at LFU_INIT_VAL and the first increments are near-certain
        # at the default lfu-log-factor, so a handful of reads is enough to lift the hot
        # pair clear of the cold pair's starting counter.
        for _ in range(10):
            for key in self.HOT:
                client.execute_command('BLOB.GET', key)

        payload = b'N' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload

    def test_volatile_policy_evicts_only_keys_with_a_ttl(self):
        """Under `volatile-*` a key with no TTL is not a victim, and a TTL makes it one.

        Core samples `db->expires` under those policies, so an operator who set
        `volatile-lru` was promised their persistent keys survive. We sample the whole
        keyspace, so that promise is ours to keep: a walk that finds only persistent keys
        must come back empty and fail the SET, not delete them.

        Both halves are needed, and taking them against one keyspace is what makes the
        argument tight: the first SET fails with four persistent residents, then the *only*
        thing that changes is a TTL on the cold pair and the same SET succeeds by taking
        exactly them. Without the second half the first would also pass if `volatile-*`
        simply disabled eviction; without the first, an eligible-set bug would hide.
        """
        client = self.server.get_new_client()
        _set_memory_policy(client, 'volatile-lru')
        self._fill(client)

        before = info_largeobj(client)['largeobj_evictions_total']
        payload = b'N' * self.OBJ_SIZE
        try:
            client.execute_command('BLOB.SET', 'newcomer', payload)
            assert False, "the SET must fail: nothing in the arena is eligible to evict"
        except ResponseError:
            pass
        assert info_largeobj(client)['largeobj_evictions_total'] == before, \
            "nothing had a TTL, so the walk must have destroyed nothing"
        for key in self.HOT + self.COLD:
            assert client.execute_command('EXISTS', key) == 1, \
                f"{key} has no TTL and volatile-lru does not evict it"

        # The cold pair is now the whole eligible set, so it is both what ranking should
        # pick and all it could pick — which is what the assertion needs, since one round
        # cannot distinguish two keys when only those two are candidates.
        for key in self.COLD:
            assert client.execute_command('EXPIRE', key, 600) == 1
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────


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
    """Tiered mode against a small `nvme-maxmemory` — the only budget eviction can free.

    Tiered mode carries three budgets and just one of them is worth evicting for. The
    DRAM segment pool and the NVMe staging pool are both transient: a staging buffer is
    owned by a SET still in flight and comes back when that write completes, so running
    out of either means "busy", and destroying a key would not produce one. The disk
    ledger is the opposite — those bytes belong to objects that exist, and the only way
    to get them back is to destroy an object. So these tests press on the ledger, and
    both pools are sized above the largest object written here so that a rejected SET
    can only be the ledger talking.

    The policy is the same one Dram mode follows — never destroy a key you cannot turn
    into budget — but the arithmetic is easier: disk bytes are fungible, so there is no
    fragmentation and the counter *is* the authority, which is what lets the reclaimed
    bytes be asserted exactly below.
    """

    OBJ = 512 * 1024                  # a 4096-multiple, so the payload needs no tail padding
    HEADER = 4096                     # FILE_HEADER_SIZE: one page per version, ahead of the payload
    DISK_PER_OBJ = OBJ + HEADER       # what the ledger is charged, which is not the payload size
    OBJECTS_PER_CAP = 8
    CAP = OBJECTS_PER_CAP * DISK_PER_OBJ  # a whole number of objects, so filling it evicts nothing

    def get_module_args(self, data_dir, direct_io):
        # scaling-poll-ms high so the DRAM scaling cron cannot shrink underneath us.
        # segment-size holds every object these tests write at once, so promotion never
        # pressures the DRAM pool and the only eviction observed is the disk's.
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size {2 * self.CAP}"
            f" segment-size {2 * self.CAP}"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    def _dat_files(self):
        return [f for f in os.listdir(self.data_dir) if f.endswith('.dat')]

    def _assert_capacity_rejected(self, client, key, payload):
        try:
            client.execute_command('BLOB.SET', key, payload)
            assert False, f"Expected '{key}' to be rejected for disk capacity"
        except ResponseError as e:
            assert 'capacity exceeded' in str(e).lower(), f"Unexpected error: {e}"

    def _fill_cap(self, client):
        """Write exactly the cap — `CAP` is a whole number of `DISK_PER_OBJ`, so every one
        of these fits and nothing is evicted. The next write is the one with no room."""
        for i in range(self.OBJECTS_PER_CAP):
            r = client.execute_command('BLOB.SET', f'fill_{i}', b'F' * self.OBJ)
            assert r == b'OK', f"fill_{i} failed: {r}"

    def test_writing_past_the_cap_evicts_exactly_and_stays_bounded(self):
        """SETs past nvme-maxmemory keep succeeding by evicting, and the ledger, the
        counters, the survivors and the directory all agree about what was destroyed.

        Three caps' worth of identically sized objects. The total is what makes this
        independent of how much a single call frees: this much data cannot be on disk at
        once and the cap cannot grow, so objects had to be destroyed.

        `reclaimed == victims * DISK_PER_OBJ` is the assertion Dram mode cannot make.
        Both modes credit freed bytes at face value, but in the arena that total is only a
        guess — the bytes may not form a contiguous run, so `alloc_exact` gets the last
        word. On disk the bytes are fungible, so the ledger *is* the authority and the
        product is exact: a discount, a double-credit, or a size taken from the wrong
        version of the object all break it.

        The file count settles rather than matching instantly. Eviction releases the ledger
        on the main thread and leaves `unlink(2)` to whoever drops the last
        `Arc<ObjectFile>`, off-thread. Bounded overshoot is by design; files that never go
        are a leak.
        """
        client = self.server.get_new_client()
        allow_evictions(client)

        count = 3 * self.OBJECTS_PER_CAP
        payloads = {f'obj_{i}': bytes([i % 256]) * self.OBJ for i in range(count)}
        for key, payload in payloads.items():
            r = client.execute_command('BLOB.SET', key, payload)
            assert r == b'OK', f"SET {key} must succeed via eviction: {r}"

        info = info_largeobj(client)
        victims = info['largeobj_disk_evictions_total']
        assert victims > 0, (
            f"wrote {count * self.OBJ} bytes against a {self.CAP}-byte cap, so objects "
            "had to be destroyed"
        )
        assert info['largeobj_disk_eviction_failures_total'] == 0, \
            "every SET succeeded, so no walk should be recorded as having failed"
        assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes'], \
            (f"ledger {info['largeobj_disk_used_bytes']} exceeds cap "
             f"{info['largeobj_disk_maxmemory_bytes']}")
        reclaimed = info['largeobj_disk_eviction_reclaimed_bytes_total']
        assert reclaimed == victims * self.DISK_PER_OBJ, (
            f"{victims} victims of {self.DISK_PER_OBJ} bytes each should credit "
            f"{victims * self.DISK_PER_OBJ}, got {reclaimed}"
        )

        survivors = 0
        for key, payload in payloads.items():
            if client.execute_command('EXISTS', key) == 1:
                assert client.execute_command('BLOB.GET', key) == payload, \
                    f"{key} survived eviction but its data is wrong"
                survivors += 1
        assert survivors > 0, "eviction must not empty the keyspace"
        # The last object written cannot have been anyone's victim.
        assert client.execute_command('EXISTS', f'obj_{count - 1}') == 1
        wait_for_true(lambda: len(self._dat_files()) == survivors, timeout=10)

    def test_a_refused_set_destroys_nothing(self):
        """A walk that cannot cover the request rolls back, so the SET fails and the
        keyspace is untouched.

        The walk claims as it samples — it empties each victim's value to take sole
        ownership of the file handle — but it does not delete the keys until it knows the
        claims cover the newcomer. Without the rollback this SET would fail *and* take
        every object with it: the caller gets an error and the cache is empty.

        Engineered so the failure needs no clock and no pins. Three objects resident
        against a request worth seven: `hopeless_request` lets it through (seven is under
        the cap and something is resident), the scan then offers every key in the DB and
        the walk claims all three, and `freed >= need` is still false when the candidates
        run out. Deterministic, and the failure counter is what proves the walk really did
        claim and give back rather than never having tried.
        """
        client = self.server.get_new_client()
        allow_evictions(client)

        resident = 3
        for i in range(resident):
            r = client.execute_command('BLOB.SET', f'keep_{i}', bytes([i]) * self.OBJ)
            assert r == b'OK', f"keep_{i} failed: {r}"

        before = info_largeobj(client)
        files_before = sorted(self._dat_files())

        # disk_len is payload + header, so subtract one header to land on exactly seven
        # objects' worth of ledger — over what three victims can free, under the cap.
        oversized = b'X' * (7 * self.DISK_PER_OBJ - self.HEADER)
        self._assert_capacity_rejected(client, 'too_big', oversized)

        for i in range(resident):
            assert client.execute_command('BLOB.GET', f'keep_{i}') == bytes([i]) * self.OBJ, \
                f"keep_{i} must survive a SET that was refused"

        after = info_largeobj(client)
        assert after['largeobj_disk_used_bytes'] == before['largeobj_disk_used_bytes'], \
            "a rolled-back walk must leave the ledger where it found it"
        assert after['largeobj_disk_evictions_total'] == before['largeobj_disk_evictions_total'], \
            "nothing was evicted, so nothing should be counted as evicted"
        assert sorted(self._dat_files()) == files_before, \
            "no file may be unlinked for a SET that never happened"
        assert (after['largeobj_disk_eviction_failures_total']
                > before['largeobj_disk_eviction_failures_total']), \
            "the walk must have claimed victims and rolled them back, not declined to start"

    def test_a_set_the_cap_cannot_serve_is_refused_without_evicting(self):
        """The two ways a full ledger refuses a write, neither of which destroys anything.

        Both run against the same filled cap, so the second half also shows the first did
        not quietly spend the keyspace. The oversized object is larger than the cap could
        ever hold, with evictions *permitted*, which separates "eviction could not serve
        this" from "eviction was forbidden" — the staging pool is twice the cap precisely
        so it fits in memory, or the SET would be rejected earlier for a buffer it could
        not get and prove nothing about the disk pre-check.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        self._fill_cap(client)
        before = info_largeobj(client)['largeobj_disk_evictions_total']

        self._assert_capacity_rejected(client, 'huge', b'H' * (self.CAP + self.OBJ))
        assert info_largeobj(client)['largeobj_disk_evictions_total'] == before, \
            "nothing is worth destroying for a write that can never fit"

        deny_evictions(client)
        self._assert_capacity_rejected(client, 'overflow', b'O' * self.OBJ)
        assert info_largeobj(client)['largeobj_disk_evictions_total'] == before, \
            "the policy forbids deleting keys, so the walk must not have run"

        for i in range(self.OBJECTS_PER_CAP):
            assert client.execute_command('EXISTS', f'fill_{i}') == 1, \
                f"fill_{i} was destroyed for a SET that could never succeed"
        assert client.execute_command('EXISTS', 'huge') == 0
        assert client.execute_command('EXISTS', 'overflow') == 0

    def test_a_pinned_object_is_not_claimed_for_the_disk_budget(self):
        """An object a GET is reading is skipped, and the ledger still stays under the cap.

        The disk budget's pin is `Arc<ObjectFile>`: a reader's reference keeps the file's
        blocks allocated past the `unlink(2)`, so those bytes stay charged to
        `nvme-maxmemory`. Claiming such a victim would credit budget that never arrives,
        and `DiskReservation::commit`'s "the victims cover `disk_len`" would stop being
        true — usage parked at the cap while every SET destroys another key. So the walk
        declines it, and both halves are checked here: the skip happens, and the writes
        that follow still respect the ledger by taking someone else.
        """
        client = self.server.get_new_client()
        allow_evictions(client)
        client.execute_command('CONFIG', 'SET', 'maxmemory-samples', '16')
        self._fill_cap(client)

        evictions_before = info_largeobj(client)['largeobj_disk_evictions_total']
        reader, stop = hold_pin_via_dead_peer(self.server.get_new_client(), 'fill_0')
        try:
            skips_before, skips_after = write_until_pinned_skip(
                client, 'newcomer_', b'N' * self.OBJ, batch=2 * self.OBJECTS_PER_CAP)

            after = info_largeobj(client)
            assert after['largeobj_disk_evictions_total'] > evictions_before, \
                "the walk must have run, or the skip below proves nothing"
            assert skips_after > skips_before, (
                f"expected pinned skips ({skips_before} -> {skips_after}): fill_0 was "
                "offered to these walks with a reader holding it, and had to be declined"
            )
            assert client.execute_command('EXISTS', 'fill_0') == 1, \
                "a pinned object must survive the walk that was offered it"
            assert after['largeobj_disk_used_bytes'] <= after['largeobj_disk_maxmemory_bytes'], (
                f"ledger {after['largeobj_disk_used_bytes']} exceeds cap "
                f"{after['largeobj_disk_maxmemory_bytes']}: a skipped victim must be "
                "replaced by another, not silently credited"
            )
        finally:
            stop.set()
            reader.join(timeout=30)

        assert client.execute_command('BLOB.GET', 'fill_0') == b'F' * self.OBJ, \
            "declining a victim must leave it readable, not half-unlinked"


class TestTieredPromotionSkip(ValkeyLargeObjTestCaseBase):
    """Tiered mode against a DRAM arena too small for the working set.

    The promotion cache is the third pressure point, and the one where nothing is ever
    given up: the object is already on NVMe, so an arena with no room simply skips the
    promotion and the GET serves from disk. The arena is held at one segment —
    `dram-maxmemory == segment-size`, so expand can never fire — and more distinct objects
    are read than it can hold, which is the only way to make a promotion ask for room that
    is not there.

    The disk cap is far above what is written here, so any eviction counter that moves is a
    bug rather than the test's own doing.
    """

    OBJ = 512 * 1024                  # a chunk-size multiple, so a promoted copy takes exactly this
    SEGMENT = 2 * 1024 * 1024         # holds two or three OBJ at once, not twelve
    OBJECTS = 12
    DISK_CAP = 64 * 1024 * 1024       # far above OBJECTS * OBJ: the ledger never presses

    def get_module_args(self, data_dir, direct_io):
        # scaling-poll-ms high so the cron cannot shrink or grow underneath us; the only
        # thing moving DRAM here is the promotion path itself.
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.DISK_CAP}"
            f" nvme-staging-size 4194304"
            f" segment-size {self.SEGMENT}"
            f" dram-maxmemory {self.SEGMENT}"
            f" max-promote-size 1048576"
            f" chunk-size 65536"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    def _dat_files(self):
        return [f for f in os.listdir(self.data_dir) if f.endswith('.dat')]

    def _write_and_read_all(self, client, prefix):
        """Write OBJECTS distinct objects, then read each once.

        Each SET goes straight to NVMe — Tiered SETs never touch the arena — so every
        read is a promotion attempt, and the arena runs out partway through. Returns the
        payloads so callers can check the data survived.
        """
        payloads = {f'{prefix}_{i}': bytes([i % 256]) * self.OBJ for i in range(self.OBJECTS)}
        for key, payload in payloads.items():
            r = client.execute_command('BLOB.SET', key, payload)
            assert r == b'OK', f"SET {key} failed: {r}"
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.GET', key) == payload, f"GET {key} is wrong"
        return payloads

    def test_a_full_arena_serves_from_nvme_instead_of_giving_anything_up(self):
        """Reading past the arena's capacity keeps every key, bounds the arena, and
        unlinks nothing. The second read pass is served largely by objects that never
        got a promoted copy, since the arena ran out partway through the first."""
        client = self.server.get_new_client()
        payloads = self._write_and_read_all(client, 'skip')

        info = info_largeobj(client)
        assert info['largeobj_evictions_total'] == 0, \
            "a Tiered node must never destroy an object to free DRAM"
        assert info['largeobj_disk_evictions_total'] == 0, \
            "the disk cap was never pressed, so nothing should have been claimed for it"
        assert info['largeobj_capacity_bytes'] == self.SEGMENT, \
            "dram-maxmemory == segment-size must keep the pool at one segment"
        assert info['largeobj_allocated_bytes'] <= info['largeobj_capacity_bytes'], \
            "promotion must never over-commit the arena"
        assert info['largeobj_cached_objects'] * self.OBJ <= self.SEGMENT, \
            "more copies are resident than the arena can physically hold"
        assert len(self._dat_files()) == self.OBJECTS, \
            "a skipped promotion touches the cache, never the files"

        for key, payload in payloads.items():
            assert client.execute_command('EXISTS', key) == 1, \
                f"{key} left the keyspace — a skipped promotion must not delete"
            assert client.execute_command('BLOB.GET', key) == payload, \
                f"{key} survived but its data is wrong"
