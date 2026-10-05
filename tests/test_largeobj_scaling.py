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
    evicting nothing resident and leaving every key in place
  - Both modes: an object a transfer is reading is skipped, not destroyed
  - Both modes: COPY at the cap evicts for room like a SET, but never its own source
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


def cap_dram_growth(client):
    """Freeze the DRAM pool at its current size. Only the server `maxmemory` stops it growing:
    `try_expand` refuses once used + one segment reaches the shrink watermark of maxmemory.

    At the watermark's 50% floor that leaves about two segments of headroom under maxmemory for
    the client's own buffers; any tighter and the core evicts or refuses on its own.
    """
    segment = info_largeobj(client)['largeobj_dram_segment_size_bytes']
    used = int(client.info('memory')['used_memory'])
    client.execute_command('CONFIG', 'SET', 'largeobj.scaling-shrink-watermark', 50)
    client.execute_command('CONFIG', 'SET', 'maxmemory', str(2 * (used + segment) - 512 * 1024))


def _set_memory_policy(client, policy):
    """Make the module's EVICT-flag gate resolve as intended: maxmemory > 0 and a policy that
    permits eviction (Valkey defaults to neither).

    Tiered leaves maxmemory far above current usage, so core eviction never runs and any eviction
    a test observes is the module's own. Dram freezes the pool, so a full segment can only be
    served by evicting.
    """
    if 'largeobj_nvme_live_segments' in info_largeobj(client):
        used = int(client.info('memory')['used_memory'])
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024 * 1024))
    else:
        cap_dram_growth(client)
    client.execute_command('CONFIG', 'SET', 'maxmemory-policy', policy)


def allow_evictions(client):
    """Permit module eviction: maxmemory set, policy allows deleting keys."""
    _set_memory_policy(client, 'allkeys-lru')


def deny_evictions(client):
    """Forbid module eviction via the policy alone: maxmemory stays set, so `noeviction` is the
    only reason the gate closes."""
    _set_memory_policy(client, 'noeviction')


def hold_pin_via_dead_peer(client, key):
    """Keep `key` pinned by looping GETs that cannot finish: each clones the object's reference
    and holds it until a fabric timeout against PEER_ADDRESS (about a second).

    `client` is consumed: it is put into BLOB.HELLO, then blocks in GET after GET until the
    returned event is set.
    """
    client.execute_command('BLOB.HELLO', PEER_ADDRESS)
    stop = threading.Event()

    def loop():
        while not stop.is_set():
            try:
                # EFA arity: BLOB.GET key rkey addr len.
                client.execute_command('BLOB.GET', key, 0, 0, 1 << 30)
            except ResponseError:
                pass  # expected: the transfer has no peer

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t, stop


def write_until_pinned_skip(client, prefix, payload, batch, timeout=10):
    """SET `payload` under `prefix` until a walk reports declining a pinned victim.

    Batched against a deadline because the pin is held in bursts. Individual SETs are not
    asserted. Returns (skips_before, skips_after).
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


def write_until_key_is_claimed(client, prefix, payload, batch, key, timeout=10):
    """SET `payload` under `prefix` until a walk destroys `key`. False if it never does.

    Shows a pinned skip was the pin and not a permanent refusal: once the transfer resolves,
    the key must be a candidate again.
    """
    deadline = time.time() + timeout
    i = 0
    while client.execute_command('EXISTS', key) == 1:
        if time.time() >= deadline:
            return False
        for _ in range(batch):
            try:
                client.execute_command('BLOB.SET', f'{prefix}{i}', payload)
            except ResponseError:
                pass
            i += 1
    return True


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
    """Dram mode with the pool frozen at two segments: a further SET either evicts (the policy
    allows it) or errors."""

    def get_module_args(self, data_dir, direct_io):
        # 1MB segments: the pool grows to two for the first pair of 900KB objects, and is then
        # frozen there, so a third can only be admitted by evicting one of them.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-poll-ms 60000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    OBJ_SIZE = 900 * 1024
    CAP_BYTES = 2097152

    def _fill_to_cap(self, client):
        """Two 900KB objects: one per segment. The pool grows to hold them, then is frozen."""
        client.execute_command('BLOB.SET', 'key_a', b'A' * self.OBJ_SIZE)
        client.execute_command('BLOB.SET', 'key_b', b'B' * self.OBJ_SIZE)
        assert info_largeobj(client)['largeobj_dram_live_segments'] == 2

    def test_the_two_outcomes_at_the_cap(self):
        """Against a pool that cannot grow, in this order so the second also witnesses that the
        first left the keyspace alone.

        1. Evictions permitted: admitted by evicting a resident object.
        2. `noeviction`: refused at the first SET that would need a victim. It takes a loop
           because (1)'s eviction overshoots and leaves a spare object's room behind.
        """
        client = self.server.get_new_client()
        self._fill_to_cap(client)
        allow_evictions(client)

        before = info_largeobj(client)['largeobj_evictions_total']
        payload = b'C' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'key_c', payload) == b'OK', \
            "SET at cap must succeed via eviction"
        evicted = info_largeobj(client)['largeobj_evictions_total']
        assert evicted > before, \
            "at the cap the pool cannot grow, so key_c could only have fit by evicting"
        assert client.execute_command('BLOB.GET', 'key_c') == payload

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
            f"pool grew past its two segments: {info['largeobj_capacity_bytes']}"
        assert client.execute_command('EXISTS', refused) == 0, \
            "a refused SET must not leave the key behind"


class TestDramEviction(ValkeyLargeObjTestCaseBase):
    """Dram mode, one 1MB segment that cannot grow, so eviction is the only way a SET can succeed
    once it is full. Both transports reach `alloc_dram_or_make_room`, and the fabric is up in
    every Dram server, so EFA needs no separate fixture."""

    SEGMENT_SIZE = 1024 * 1024

    def get_module_args(self, data_dir, direct_io):
        # One segment, frozen there by `allow_evictions`. scaling-poll-ms high so the cron
        # cannot interfere.
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" max-object-size {self.SEGMENT_SIZE - 64 * 1024}"
            f" chunk-size 65536"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    # Objects are sized so this many would fill the segment exactly; fewer fit, because talc's
    # metadata lives inside it.
    OBJECTS_PER_SEGMENT = 4

    def test_writing_past_the_segment_evicts_and_stays_bounded(self):
        """SETs past the segment keep succeeding by evicting, the survivors are intact, and the
        pool never grows.

        Three segments' worth: that much cannot be resident whatever one call frees, so no
        per-SET assertion (which eviction's overshoot would break) is needed.
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
        assert info['largeobj_dram_live_segments'] == 1, \
            "a frozen pool must stay at one segment"
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

        # An object over max-object-size is refused at admission, before any victim is taken.
        before = info_largeobj(client)['largeobj_evictions_total']
        try:
            client.execute_command('BLOB.SET', 'toobig', b'D' * self.SEGMENT_SIZE)
            assert False, "Expected max object size error"
        except ResponseError as e:
            assert 'max object size' in str(e).lower(), f"Unexpected error: {e}"
        assert info_largeobj(client)['largeobj_evictions_total'] == before, \
            "an inadmissible SET must not destroy objects"
        assert client.execute_command('BLOB.GET', keeper) == payloads[keeper]

    def test_copy_at_the_segment_evicts_another_key_and_never_its_source(self):
        """COPY makes room like a SET does, but never out of its source: alone in the segment it
        fails cleanly, and with another key resident that key is the victim."""
        client = self.server.get_new_client()
        allow_evictions(client)
        obj = b'A' * (384 * 1024)

        big = b'S' * (640 * 1024)
        assert client.execute_command('BLOB.SET', 'src', big) == b'OK'
        before = info_largeobj(client)
        try:
            client.execute_command('COPY', 'src', 'dst')
            assert False, "Expected COPY to fail: the source is the only thing to evict"
        except ResponseError:
            pass
        assert client.execute_command('BLOB.GET', 'src') == big, "the source must survive"
        assert client.execute_command('EXISTS', 'dst') == 0
        assert info_largeobj(client)['largeobj_evictions_total'] == before['largeobj_evictions_total']

        client.execute_command('FLUSHALL')
        assert client.execute_command('BLOB.SET', 'src', obj) == b'OK'
        assert client.execute_command('BLOB.SET', 'other', b'O' * len(obj)) == b'OK'
        evictions_before = info_largeobj(client)['largeobj_evictions_total']
        assert client.execute_command('COPY', 'src', 'dst') in (1, True)
        assert info_largeobj(client)['largeobj_evictions_total'] == evictions_before + 1
        assert client.execute_command('EXISTS', 'other') == 0
        assert client.execute_command('BLOB.GET', 'src') == obj
        assert client.execute_command('BLOB.GET', 'dst') == obj

    def test_eviction_takes_victims_from_every_db(self):
        """A SET from db 0 evicts keys in dbs 1 and 2: the arena is node-wide. The newcomer needs
        both victims (one is not enough room), so the walk has to rotate across dbs. The client
        must be left on its own db, so the newcomer is read back from a fresh connection."""
        db0 = self.server.get_new_client()
        dbs = {1: self.server.create_from_server(db=1), 2: self.server.create_from_server(db=2)}
        allow_evictions(db0)
        payloads = {1: b'A' * (384 * 1024), 2: b'B' * (384 * 1024)}
        for db, payload in payloads.items():
            assert dbs[db].execute_command('BLOB.SET', 'obj', payload) == b'OK'

        before = info_largeobj(db0)['largeobj_evictions_total']
        newcomer = b'N' * (700 * 1024)
        assert db0.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        assert info_largeobj(db0)['largeobj_evictions_total'] == before + 2
        fresh = self.server.get_new_client()
        assert fresh.execute_command('BLOB.GET', 'newcomer') == newcomer
        assert all(c.execute_command('EXISTS', 'newcomer') == 0 for c in dbs.values())
        assert all(c.execute_command('EXISTS', 'obj') == 0 for c in dbs.values()), \
            "freeing 700KB took the key in each db"

    def test_tenacity_zero_still_evicts(self):
        """A zero search time limit must not degenerate into noeviction: the floor of candidates
        a walk always examines is what keeps tenacity 0 evicting."""
        client = self.server.get_new_client()
        allow_evictions(client)
        client.execute_command('CONFIG', 'SET', 'largeobj.eviction-tenacity', '0')
        obj_size = self.SEGMENT_SIZE // self.OBJECTS_PER_SEGMENT

        for i in range(self.OBJECTS_PER_SEGMENT):
            assert client.execute_command(
                'BLOB.SET', f'fill_{i}', bytes([i % 256]) * obj_size) == b'OK'

        before = info_largeobj(client)['largeobj_evictions_total']

        # Twice a segment's worth: one SET can be served by slack an earlier eviction left, so
        # only a total that cannot be resident at once proves the walk ran.
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
        """An EFA SET into a full, ungrowable arena evicts at alloc time, before the transfer.
        PEER_ADDRESS is dead so the SET errors; the metric moving is the assertion."""
        client = self.server.get_new_client()
        allow_evictions(client)
        obj_size = self.SEGMENT_SIZE // self.OBJECTS_PER_SEGMENT

        # Fill over TCP, which is synchronous, so the arena is known-full. One short of
        # OBJECTS_PER_SEGMENT: the full count sums to exactly SEGMENT_SIZE, which cannot fit, so
        # the last fill would evict, overshoot and leave slack for the EFA SET to use.
        for i in range(self.OBJECTS_PER_SEGMENT - 1):
            r = client.execute_command('BLOB.SET', f'fill_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"fill_{i} failed: {r}"

        client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        before = info_largeobj(client)['largeobj_evictions_total']
        assert before == 0, (
            f"the fill must not have evicted ({before}); otherwise the arena has slack "
            "and the EFA SET below can allocate without evicting"
        )

        # EFA arity: BLOB.SET key total_len rkey addr len.
        try:
            client.execute_command('BLOB.SET', 'efa_newcomer', obj_size, 0, 0, obj_size)
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
        """An object a transfer is reading is skipped by the walk: claiming it would credit bytes
        that only come back when the reader drops its `Arc<ObjectContext>`."""
        client = self.server.get_new_client()
        allow_evictions(client)
        # One round samples the whole keyspace, so the pinned key is offered to the first walk.
        client.execute_command('CONFIG', 'SET', 'maxmemory-samples', '16')
        obj_size = self.SEGMENT_SIZE // self.PINNED_OBJECTS
        payload = b'F' * obj_size

        # One short of what fits, as in test_efa_set_evicts_at_alloc_time, so the last fill does
        # not evict fill_0 before it is pinned.
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
        assert write_until_key_is_claimed(
            client, 'after_', payload, 2 * self.PINNED_OBJECTS, 'fill_0'), \
            "with the transfer over, fill_0 must be a candidate again, not pinned for good"


class TestDramEvictionPolicy(ValkeyLargeObjTestCaseBase):
    """*Which* object eviction destroys. These are the only tests that tell ranked victim
    selection from taking whatever the cursor offers: each makes half the keyspace demonstrably
    warmer by the metric its policy reads, forces one SET's worth of eviction, and asserts the
    cold half paid.

    One test per ranking, since `victim_score` has branches the others' tests cannot see. Four
    residents against the default `maxmemory-samples` of 5 means every key is scored, so the
    ordering is exact.
    """

    SEGMENT_SIZE = 1024 * 1024
    # Five to a segment, so four residents plus a fifth newcomer overshoots it and the SET
    # must evict. At a 90% credit one victim is not enough and two are, so eviction takes
    # exactly the two coldest — which is exactly the hot/cold split below.
    OBJ_SIZE = SEGMENT_SIZE // 5
    COLD = ('cold_0', 'cold_1')
    HOT = ('hot_0', 'hot_1')

    get_module_args = TestDramEviction.get_module_args

    def _fill(self, client):
        """Four residents, the cold pair written last. In a small keyspace that puts it behind the
        hot pair in cursor order, so only a ranking that reads the metric picks it out (written
        the other way round, cursor order and the right answer coincide)."""
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
        """Under LRU the least recently touched objects are the victims, and sampling does not
        refresh a key's idle time.

        `keeper` is written a sleep after the rest and never read: it survives as the least idle
        candidate, and its real idle time must still show afterwards. The LRU clock has
        one-second resolution, so the waits are needed.
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

        # The cold pair is now strictly the most idle, and the 90% credit makes the walk take
        # exactly it.
        payload = b'N' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload
        assert client.execute_command('EXISTS', 'keeper') == 1, \
            "keeper is the least idle candidate; it should not be a victim"
        assert client.execute_command('OBJECT', 'IDLETIME', 'keeper') >= 2, \
            "sampling reset keeper's idle time — scoring touched the key"

    def test_lfu_evicts_the_rarely_used_keys(self):
        """Under LFU the rarely used objects are the victims. No sleep: the counter separates
        keys by access count immediately."""
        client = self.server.get_new_client()
        _set_memory_policy(client, 'allkeys-lfu')
        self._fill(client)

        # A fresh object starts at LFU_INIT_VAL and early increments are near-certain, so a
        # few reads lift the hot pair clear of the cold pair.
        for _ in range(10):
            for key in self.HOT:
                client.execute_command('BLOB.GET', key)

        payload = b'N' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload

    def test_volatile_policy_evicts_only_keys_with_a_ttl(self):
        """Under `volatile-*` a key with no TTL is not a victim, and a TTL makes it one.

        Core samples `db->expires`; we sample the whole keyspace and must exclude persistent
        keys ourselves. Both halves run against one keyspace: the first SET fails with four
        persistent residents, then only a TTL on the cold pair changes and the same SET takes
        exactly them.
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

        # The cold pair is now the whole eligible set.
        for key in self.COLD:
            assert client.execute_command('EXPIRE', key, 600) == 1
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload

    def test_volatile_ttl_evicts_the_soonest_to_expire_first(self):
        """Under `volatile-ttl` the soonest-to-expire keys are the victims, whatever their idle
        time. That pair is written and read last, so idleness and cursor order would both take
        the other pair."""
        client = self.server.get_new_client()
        _set_memory_policy(client, 'volatile-ttl')
        self._fill(client)
        for key, ttl in zip(self.HOT + self.COLD, (5000, 6000, 100, 200)):
            assert client.execute_command('EXPIRE', key, ttl) == 1
        for key in self.COLD:
            assert client.execute_command('BLOB.GET', key) is not None

        payload = b'N' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        self._assert_cold_paid(client)
        assert client.execute_command('BLOB.GET', 'newcomer') == payload

    def test_allkeys_random_still_finds_victims(self):
        """A random policy ranks nothing: which keys go is not asserted, that the walk takes
        exactly enough and leaves the rest readable is."""
        client = self.server.get_new_client()
        _set_memory_policy(client, 'allkeys-random')
        self._fill(client)
        before = info_largeobj(client)['largeobj_evictions_total']
        payload = b'N' * self.OBJ_SIZE
        assert client.execute_command('BLOB.SET', 'newcomer', payload) == b'OK'
        assert info_largeobj(client)['largeobj_evictions_total'] == before + 2
        assert client.execute_command('BLOB.GET', 'newcomer') == payload
        survivors = [k for k in self.HOT + self.COLD if client.execute_command('EXISTS', k)]
        assert len(survivors) == 2
        for key in survivors:
            assert client.execute_command('BLOB.GET', key) == key.encode()[:1] * self.OBJ_SIZE


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
    """Tiered mode against a small `nvme-maxmemory`, the only budget eviction can free: the DRAM
    and staging pools are transient (running out means "busy"), while ledger bytes belong to
    objects that exist. Both pools are sized above the largest object, so a rejected SET can
    only be the ledger. Disk bytes are fungible, so the counter is the authority and reclaimed
    bytes can be asserted exactly."""

    OBJ = 512 * 1024                  # a 4096-multiple, so the payload needs no tail padding
    HEADER = 4096                     # FILE_HEADER_SIZE: one page per version, ahead of the payload
    DISK_PER_OBJ = OBJ + HEADER       # what the ledger is charged, which is not the payload size
    OBJECTS_PER_CAP = 8
    CAP = OBJECTS_PER_CAP * DISK_PER_OBJ  # a whole number of objects, so filling it evicts nothing

    def get_module_args(self, data_dir, direct_io):
        # segment-size holds every object at once, so promotion never pressures the DRAM pool
        # and the only eviction observed is the disk's.
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

    def test_the_ledger_credit_is_exact_so_one_victim_pays_for_one_object(self):
        """A same-sized SET at the cap takes exactly one victim: the ledger has no placement
        to misjudge, so no discount is applied to its credit (the arena's is 90%, which makes
        the same request take two)."""
        client = self.server.get_new_client()
        allow_evictions(client)
        self._fill_cap(client)

        assert client.execute_command('BLOB.SET', 'n0', b'N' * self.OBJ) == b'OK'
        assert info_largeobj(client)['largeobj_disk_evictions_total'] == 1

    def test_writing_past_the_cap_evicts_exactly_and_stays_bounded(self):
        """SETs past nvme-maxmemory keep succeeding by evicting, and the ledger, counters,
        survivors and directory agree on what was destroyed.

        Three caps' worth, so objects had to be destroyed whatever one call frees.
        `reclaimed == victims * DISK_PER_OBJ` is exact here, where Dram mode cannot assert it.
        Files are unlinked off-thread, so their count settles rather than matching instantly.
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
        """A walk that cannot cover the request evicts nothing: the SET fails and the keyspace is
        untouched.

        Three objects resident against a request worth seven: `cannot_be_satisfied` lets it
        through, the walk claims all three, and the claims still fall short. The failure counter
        proves the walk ran rather than never trying.
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
        """The two ways a full ledger refuses a write, neither destroying anything: a request
        larger than the cap (evictions permitted) and `noeviction`. Both run against one filled
        cap, so the second half also shows the first spent nothing. The oversized SET is as big
        as max-object-size allows: its payload alone is the cap, so with the header it cannot fit."""
        client = self.server.get_new_client()
        allow_evictions(client)
        self._fill_cap(client)
        before = info_largeobj(client)['largeobj_disk_evictions_total']

        self._assert_capacity_rejected(client, 'huge', b'H' * self.CAP)
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

    def test_copy_at_the_cap_evicts_another_key_and_never_its_source(self):
        """COPY makes room like a SET, but never out of its source: COPY pins it, so the walk
        skips it. Alone, COPY fails cleanly with the source untouched; with other keys resident,
        one of them is the victim."""
        client = self.server.get_new_client()
        allow_evictions(client)

        big = b'S' * (5 * self.OBJ)
        assert client.execute_command('BLOB.SET', 'src', big) == b'OK'
        before = info_largeobj(client)
        try:
            client.execute_command('COPY', 'src', 'dst')
            assert False, "Expected COPY to fail: the source is the only thing to evict"
        except ResponseError:
            pass
        after = info_largeobj(client)
        assert client.execute_command('BLOB.GET', 'src') == big, "the source must survive"
        assert client.execute_command('EXISTS', 'dst') == 0
        assert after['largeobj_disk_evictions_total'] == before['largeobj_disk_evictions_total']
        assert after['largeobj_disk_used_bytes'] == before['largeobj_disk_used_bytes'], \
            "a refused COPY must give its reservation back"

        client.execute_command('FLUSHALL')
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_used_bytes'] == 0)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP)}
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'

        evictions_before = info_largeobj(client)['largeobj_disk_evictions_total']
        assert client.execute_command('COPY', 'fill_0', 'dst') in (1, True)
        info = info_largeobj(client)
        victims = info['largeobj_disk_evictions_total'] - evictions_before
        assert victims >= 1, "the cap was full, so COPY had to evict"
        assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes']
        assert client.execute_command('BLOB.GET', 'dst') == payloads['fill_0']
        assert client.execute_command('BLOB.GET', 'fill_0') == payloads['fill_0']
        survivors = [k for k in payloads if client.execute_command('EXISTS', k) == 1]
        assert len(survivors) == self.OBJECTS_PER_CAP - victims
        for key in survivors:
            assert client.execute_command('BLOB.GET', key) == payloads[key]
        wait_for_true(lambda: len(self._dat_files()) == len(survivors) + 1, timeout=10)

    def test_eviction_takes_victims_from_every_db(self):
        """A SET from db 0 evicts keys in dbs 1 to 4: the budget is node-wide. A refused SET
        leaves each key in the db it came from, and the client ends on its own db (the newcomer
        is read back from a fresh connection).

        Tiered keys land in db 0 whichever db the SET selected, so they are moved after writing.
        """
        db0 = self.server.get_new_client()
        dbs = {db: self.server.create_from_server(db=db) for db in (1, 2, 3, 4)}
        allow_evictions(db0)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP)}
        home = {}

        def park(key):
            home[key] = 1 + len(home) % len(dbs)
            assert db0.execute_command('BLOB.SET', key, payloads[key]) == b'OK'
            assert db0.execute_command('MOVE', key, home[key]) == 1

        def assert_intact(keys):
            for key in keys:
                assert dbs[home[key]].execute_command('BLOB.GET', key) == payloads[key], \
                    f"{key} must be readable in db {home[key]}"

        for key in list(payloads)[:6]:
            park(key)

        before = info_largeobj(db0)
        self._assert_capacity_rejected(db0, 'too_big', b'X' * (7 * self.DISK_PER_OBJ - self.HEADER))
        after = info_largeobj(db0)
        assert_intact(home)
        assert after['largeobj_disk_used_bytes'] == before['largeobj_disk_used_bytes']
        assert after['largeobj_disk_evictions_total'] == before['largeobj_disk_evictions_total']
        assert (after['largeobj_disk_eviction_failures_total']
                > before['largeobj_disk_eviction_failures_total']), \
            "the walk must have reached the other dbs, claimed their keys and given them back"

        for key in list(payloads)[6:]:
            park(key)
        # Three objects' worth needs three victims, and no db holds more than two.
        newcomer = b'N' * (3 * self.OBJ)
        before = info_largeobj(db0)['largeobj_disk_evictions_total']
        assert db0.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        victims = info_largeobj(db0)['largeobj_disk_evictions_total'] - before
        assert victims == 3, "the cap was full of keys in other dbs, so the SET had to take three"
        fresh = self.server.get_new_client()
        assert fresh.execute_command('BLOB.GET', 'newcomer') == newcomer
        assert all(c.execute_command('EXISTS', 'newcomer') == 0 for c in dbs.values())
        survivors = [k for k in payloads if dbs[home[k]].execute_command('EXISTS', k) == 1]
        assert len(survivors) == self.OBJECTS_PER_CAP - victims
        assert_intact(survivors)

    def test_a_pinned_object_is_not_claimed_for_the_disk_budget(self):
        """An object a GET is reading is skipped, and the ledger stays under the cap: a reader's
        `Arc<ObjectFile>` keeps the file's blocks allocated past the `unlink(2)`, so claiming it
        would credit budget that never arrives."""
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
        assert write_until_key_is_claimed(
            client, 'after_', b'N' * self.OBJ, 2 * self.OBJECTS_PER_CAP, 'fill_0'), \
            "with the reader gone, fill_0 must be a candidate again, not pinned for good"


class TestTieredPromotionSkip(ValkeyLargeObjTestCaseBase):
    """Tiered mode against a DRAM arena too small for the working set: a promotion into a full
    arena skips itself and the GET serves from NVMe, evicting nothing. Reading more distinct
    objects than fit is what makes a promotion ask for room that is not there. The disk cap is
    far above what is written, so any eviction counter that moves is a bug."""

    OBJ = 512 * 1024                  # a chunk-size multiple, so a promoted copy takes exactly this
    SEGMENT = 2 * 1024 * 1024         # holds two or three OBJ at once, not twelve
    OBJECTS = 12
    DISK_CAP = 64 * 1024 * 1024       # far above OBJECTS * OBJ: the ledger never presses

    def get_module_args(self, data_dir, direct_io):
        # scaling-poll-ms high so the cron cannot resize the pool underneath us.
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.DISK_CAP}"
            f" nvme-staging-size 4194304"
            f" segment-size {self.SEGMENT}"
            f" max-object-size 1048576"
            f" max-promote-size 1048576"
            f" chunk-size 65536"
            f" scaling-poll-ms 60000"
            f" bench-mode no"
            f" direct-io no"
        )

    def _write_and_read_all(self, client, prefix):
        """Write OBJECTS distinct objects, then read each once. Tiered SETs never touch the arena,
        so every read is a promotion attempt. Returns the payloads."""
        cap_dram_growth(client)
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
            "a frozen pool must stay at one segment"
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


def own_all_slots(client):
    client.execute_command('CLUSTER', 'ADDSLOTSRANGE', 0, 16383)
    wait_for_true(lambda: b'cluster_state:ok' in client.execute_command('CLUSTER', 'INFO'))


def tombstones(client):
    return info_largeobj(client)['largeobj_tombstones']


class ClusterNode:
    """Run the server as a cluster node. Core resolves a lookup made during a command to that
    command's own slot, so a walk inside `BLOB.SET foo` cannot find a victim `bar` in another
    slot: eviction tombstones it instead (bytes freed, reads miss) and a sweep deletes the key
    later. These tests write keys in different slots, and the keys an eviction takes stay
    visible to core commands like `EXISTS` until the sweep runs.
    """

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


class DramSegment(ClusterNode):
    """One 1MB segment that cannot grow, so a SET past it can only succeed by evicting."""

    SEGMENT_SIZE = 1024 * 1024

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" max-object-size {self.SEGMENT_SIZE - 64 * 1024}"
            f" chunk-size 65536"
            f" scaling-poll-ms 60000"
            f" tombstone-sweep-ms {self.SWEEP_MS}"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def new_node(self):
        client = self.server.get_new_client()
        own_all_slots(client)
        allow_evictions(client)
        return client

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

    start_target = TestDramReactiveExpand.start_target

    def test_a_set_at_the_segment_evicts_keys_in_other_slots(self):
        """SETs past the segment keep succeeding by evicting, the survivors are intact, and every
        key answers rather than crashing the node — the ones eviction took read as misses."""
        client = self.new_node()
        obj_size = self.SEGMENT_SIZE // 4
        payloads = {f'obj_{i}': bytes([i + 1]) * obj_size for i in range(12)}
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'

        assert info_largeobj(client)['largeobj_evictions_total'] > 0
        live, evicted = [], []
        for key, payload in payloads.items():
            got = client.execute_command('BLOB.GET', key)
            if got is None:
                evicted.append(key)
            else:
                assert got == payload
                live.append(key)
        assert live and evicted
        assert tombstones(client) == len(evicted), "every key eviction could not delete is tombstoned"
        for key in evicted:
            assert client.execute_command('EXISTS', key) == 1
        assert client.execute_command('EXISTS', live[-1]) == 1

    def test_an_evicted_key_reads_as_a_miss_everywhere_in_the_module(self):
        client = self.new_node()
        key = self.evicted_key(client)

        assert client.execute_command('BLOB.GET', key) is None
        try:
            client.execute_command('BLOB.INFO', key)
            assert False, "BLOB.INFO must report the key as not found"
        except ResponseError as e:
            assert 'not found' in str(e).lower()
        try:
            assert not client.execute_command('COPY', key, '{z}copy')
        except ResponseError:
            pass
        assert client.execute_command('EXISTS', '{z}copy') == 0
        assert client.execute_command('MEMORY', 'USAGE', key) < 1024, \
            "the payload is gone, so it must not be reported"
        assert client.execute_command('DEBUG', 'DIGEST-VALUE', key)
        assert client.execute_command('PING')

    def test_deleting_an_evicted_key_clears_its_tombstone(self):
        client = self.new_node()
        key = self.evicted_key(client)

        assert client.execute_command('DEL', key) == 1
        # Freeing the value clears it, possibly on a lazyfree thread. The sweep would clear it too,
        # a minute from now, which is not what is under test.
        wait_for_true(lambda: tombstones(client) == 0, timeout=10)

    def test_overwriting_an_evicted_key_is_not_a_miss(self):
        """The tombstone is the old object's, not the name's."""
        client = self.new_node()
        key = self.evicted_key(client)

        assert client.execute_command('BLOB.SET', key, b'fresh' * 1000) == b'OK'
        assert client.execute_command('BLOB.GET', key) == b'fresh' * 1000
        wait_for_true(lambda: tombstones(client) == 0, timeout=10)

    def test_an_efa_set_at_the_segment_evicts_keys_in_other_slots(self):
        """The arena is full once anything has been evicted, and an eviction can leave a few KB
        of slack, so the EFA SETs together write well past it."""
        client = self.new_node()
        filled = 0
        while info_largeobj(client)['largeobj_evictions_total'] == 0:
            assert client.execute_command('BLOB.SET', f'fill_{filled}', b'F' * 4096) == b'OK'
            filled += 1
            assert filled < 1000, "the arena never filled"

        before = info_largeobj(client)['largeobj_evictions_total']
        for i in range(16):
            process, address, rkey, remote_addr, length = self.start_target('--read')
            try:
                peer = self.server.get_new_client()  # one BLOB.HELLO per connection
                peer.execute_command('BLOB.HELLO', address)
                result = peer.execute_command(
                    'BLOB.SET', f'efa_{i}', EFA_TARGET_LEN, rkey, remote_addr, length)
                assert result == b'OK'
            finally:
                process.kill()
        assert info_largeobj(client)['largeobj_evictions_total'] > before
        assert client.execute_command('BLOB.GET', 'efa_15') == EFA_PATTERN
        for key in [f'fill_{i}' for i in range(filled)] + [f'efa_{i}' for i in range(16)]:
            got = client.execute_command('BLOB.GET', key)
            assert got is None or len(got) in (4096, EFA_TARGET_LEN)

    def test_copy_at_the_segment_evicts_a_key_in_another_slot_and_never_its_source(self):
        client = self.new_node()
        obj = b'A' * (384 * 1024)
        assert client.execute_command('BLOB.SET', '{t}src', obj) == b'OK'
        assert client.execute_command('BLOB.SET', 'other', b'O' * len(obj)) == b'OK'
        before = info_largeobj(client)['largeobj_evictions_total']

        assert client.execute_command('COPY', '{t}src', '{t}dst') in (1, True)
        assert info_largeobj(client)['largeobj_evictions_total'] == before + 1
        assert client.execute_command('BLOB.GET', 'other') is None
        assert client.execute_command('BLOB.GET', '{t}src') == obj
        assert client.execute_command('BLOB.GET', '{t}dst') == obj


class TestClusterDramSweep(DramSegment, ValkeyLargeObjTestCaseBase):
    """The timer that finishes what eviction could not: deleting the keys it tombstoned."""

    SWEEP_MS = 500  # long enough to act on a tombstone first, short enough to wait for

    def test_the_sweep_deletes_the_keys_eviction_took(self):
        client = self.new_node()
        obj_size = self.SEGMENT_SIZE // 4
        payloads = {f'obj_{i}': bytes([i + 1]) * obj_size for i in range(12)}
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'
        assert tombstones(client) > 0

        wait_for_true(lambda: tombstones(client) == 0)
        live = 0
        for key, payload in payloads.items():
            got = client.execute_command('BLOB.GET', key)
            assert (client.execute_command('EXISTS', key) == 1) == (got is not None), \
                f"{key}: a key the sweep left must be a live one"
            if got is not None:
                assert got == payload
                live += 1
        assert live > 0
        assert client.execute_command('DBSIZE') == live

    def test_a_renamed_key_is_still_swept(self):
        """The tombstone remembers the name the key had. The sweep checks that rather than trusting
        it, and finds the object by id when the key has moved."""
        client = self.new_node()
        key = self.evicted_key(client)

        assert client.execute_command('RENAME', key, '{z}renamed') in (b'OK', True)
        wait_for_true(lambda: tombstones(client) == 0)
        assert client.execute_command('EXISTS', '{z}renamed') == 0
        assert client.execute_command('EXISTS', key) == 0
        assert len(client.execute_command('BLOB.GET', '{y}newcomer')) == 600 * 1024


class TestClusterDramAcrossDbs(DramSegment, ValkeyLargeObjTestCaseBase):
    """The same node with four DBs: a SET evicts keys that live in other DBs and other slots."""

    CLUSTER_DATABASES = 4
    SWEEP_MS = 500

    def test_eviction_takes_victims_from_every_db(self):
        db0 = self.server.get_new_client()
        own_all_slots(db0)
        dbs = {d: self.server.create_from_server(db=d) for d in (1, 2)}
        allow_evictions(db0)
        obj_size = 384 * 1024
        payloads = {1: b'A' * obj_size, 2: b'B' * obj_size}
        for db, payload in payloads.items():
            assert dbs[db].execute_command('BLOB.SET', 'obj', payload) == b'OK'

        before = info_largeobj(db0)['largeobj_evictions_total']
        assert db0.execute_command('BLOB.SET', 'newcomer', b'N' * obj_size) == b'OK'
        assert info_largeobj(db0)['largeobj_evictions_total'] > before
        fresh = self.server.get_new_client()
        assert fresh.execute_command('BLOB.GET', 'newcomer') == b'N' * obj_size
        survivors = [db for db in payloads if dbs[db].execute_command('BLOB.GET', 'obj') is not None]
        assert len(survivors) < len(payloads), "a key in another db had to give way"
        for db in survivors:
            assert dbs[db].execute_command('BLOB.GET', 'obj') == payloads[db]

    def test_a_key_moved_to_another_db_is_still_swept(self):
        """The tombstone remembers the DB the key was in. A MOVE before the sweep leaves that
        wrong, and the sweep finds the object by id."""
        db0 = self.server.get_new_client()
        own_all_slots(db0)
        db1 = self.server.create_from_server(db=1)
        allow_evictions(db0)
        size = 600 * 1024
        assert db1.execute_command('BLOB.SET', '{z}victim', b'V' * size) == b'OK'
        assert db0.execute_command('BLOB.SET', '{y}newcomer', b'N' * size) == b'OK'
        assert tombstones(db0) == 1

        assert db1.execute_command('MOVE', '{z}victim', 2) in (1, True)
        wait_for_true(lambda: tombstones(db0) == 0)
        db2 = self.server.create_from_server(db=2)
        assert db2.execute_command('EXISTS', '{z}victim') == 0
        assert db1.execute_command('EXISTS', '{z}victim') == 0


class TestClusterTieredEviction(ClusterNode, ValkeyLargeObjTestCaseBase):
    """Tiered mode against a small `nvme-maxmemory`, on a node with four DBs."""

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
        though their keys outlive them as tombstones.

        Written from db 0 and then moved: a Tiered SET from another db lands in db 0 whichever
        db the client selected.
        """
        db0 = self.server.get_new_client()
        own_all_slots(db0)
        db2 = self.server.create_from_server(db=2)
        allow_evictions(db0)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP + 4)}
        clients = {key: db0 for key in payloads}
        for i, (key, payload) in enumerate(payloads.items()):
            assert db0.execute_command('BLOB.SET', key, payload) == b'OK'
            if i < self.OBJECTS_PER_CAP and i % 2 == 1:
                assert db0.execute_command('MOVE', key, 2) in (1, True)
                clients[key] = db2

        info = info_largeobj(db0)
        assert info['largeobj_disk_evictions_total'] > 0
        assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes']
        live = []
        for key, payload in payloads.items():
            got = clients[key].execute_command('BLOB.GET', key)
            if got is not None:
                assert got == payload
                live.append(key)
        assert 0 < len(live) < len(payloads)
        assert any(clients[key] is db2 for key in payloads if key not in live), \
            "the walk must reach the victims in db 2 as well as db 0"
        assert len(self._dat_files()) == len(live), "an evicted object's file must be gone"
        assert info['largeobj_disk_used_bytes'] == len(live) * self.DISK_PER_OBJ
        assert tombstones(db0) == len(payloads) - len(live)

    def test_copy_at_the_cap_evicts_a_key_in_another_slot_and_never_its_source(self):
        client = self.server.get_new_client()
        own_all_slots(client)
        allow_evictions(client)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP - 1)}
        payloads['{t}src'] = b'S' * self.OBJ
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'
        before = info_largeobj(client)['largeobj_disk_evictions_total']

        assert client.execute_command('COPY', '{t}src', '{t}dst') in (1, True)
        info = info_largeobj(client)
        assert info['largeobj_disk_evictions_total'] > before
        assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes']
        assert client.execute_command('BLOB.GET', '{t}src') == payloads['{t}src']
        assert client.execute_command('BLOB.GET', '{t}dst') == payloads['{t}src']

    def test_a_late_commit_overwrites_a_newer_write_that_was_evicted(self):
        """A SET slow to commit finds the key holding a newer object, which eviction has since given
        up. That object is a miss, not a write to defer to, so the late commit has to land."""
        slow = self.server.get_new_client()
        client = self.server.get_new_client()
        own_all_slots(client)
        allow_evictions(client)
        key, late, newer = '{k}key', b'L' * self.OBJ, b'N' * self.OBJ
        slow.execute_command('CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '3000')
        outcome = {}
        thread = threading.Thread(
            target=lambda: outcome.update(reply=slow.execute_command('BLOB.SET', key, late)))
        thread.start()
        time.sleep(0.5)  # the write is done and the task is paused, holding the older object id
        client.execute_command('CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '0')
        assert client.execute_command('BLOB.SET', key, newer) == b'OK'

        filled = 0
        while client.execute_command('BLOB.GET', key) is not None:
            assert client.execute_command('BLOB.SET', f'fill_{filled}', b'F' * self.OBJ) == b'OK'
            filled += 1
            assert filled < 100, "the newer object was never evicted"
        assert client.execute_command('EXISTS', key) == 1, "evicted from another slot: a tombstone"

        thread.join(timeout=15)
        assert not thread.is_alive()
        assert outcome['reply'] == b'OK'
        assert client.execute_command('BLOB.GET', key) == late

    def test_a_tombstone_is_never_claimed_a_second_time(self):
        """A tombstoned object is already evicted; offering it again would credit the walk with
        bytes that are not coming back, and the budget would creep past its cap."""
        client = self.server.get_new_client()
        own_all_slots(client)
        allow_evictions(client)
        for i in range(4 * self.OBJECTS_PER_CAP):
            assert client.execute_command('BLOB.SET', f'fill_{i}', bytes([i % 251]) * self.OBJ) == b'OK'
            info = info_largeobj(client)
            assert info['largeobj_disk_used_bytes'] <= info['largeobj_disk_maxmemory_bytes'], \
                f"over the cap after SET {i}"
        assert info['largeobj_tombstones'] > 0
        assert info['largeobj_pinned_skips_total'] == 0, "nothing here is being read"
        live = sum(client.execute_command('BLOB.GET', f'fill_{i}') is not None
                   for i in range(4 * self.OBJECTS_PER_CAP))
        assert info['largeobj_disk_used_bytes'] == live * self.DISK_PER_OBJ
        assert len(self._dat_files()) == live

    def test_an_evicted_object_releases_its_promoted_copy_at_once(self):
        """The victim's key waits for the sweep, but its DRAM copy must not: `lo_free` would drop
        it, and that is a minute away."""
        client = self.server.get_new_client()
        own_all_slots(client)
        allow_evictions(client)
        for i in range(self.OBJECTS_PER_CAP):
            assert client.execute_command('BLOB.SET', f'fill_{i}', b'F' * self.OBJ) == b'OK'
            assert client.execute_command('BLOB.GET', f'fill_{i}') == b'F' * self.OBJ
        assert info_largeobj(client)['largeobj_cached_objects'] == self.OBJECTS_PER_CAP

        assert client.execute_command('BLOB.SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        info = info_largeobj(client)
        assert info['largeobj_disk_evictions_total'] == 1 and info['largeobj_tombstones'] == 1
        assert info['largeobj_cached_objects'] == self.OBJECTS_PER_CAP - 1

    def test_an_evicted_key_reads_as_a_miss(self):
        client = self.server.get_new_client()
        own_all_slots(client)
        allow_evictions(client)
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP + 2)}
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'

        evicted = [k for k in payloads if client.execute_command('BLOB.GET', k) is None]
        assert evicted
        for key in evicted:
            try:
                client.execute_command('BLOB.INFO', key)
                assert False, "BLOB.INFO must report the key as not found"
            except ResponseError as e:
                assert 'not found' in str(e).lower()
            assert client.execute_command('MEMORY', 'USAGE', key) < 1024
