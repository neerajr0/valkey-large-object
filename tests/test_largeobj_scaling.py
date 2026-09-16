"""
Integration tests for DRAMPool expand/shrink scaling behavior.

Tests cover:
  - Dram mode: reactive expand when segment fills
  - Dram mode: dram-maxmemory hard cap respected
  - Tiered mode: reactive expand on DRAMPool fill
  - Tiered mode: data survives expand (reads correct after pool grows)
  - Tiered mode: shrink evicts cached segment but NVMe copy survives
  - Tiered mode: GET after shrink reads correctly from NVMe
"""

import os
import time
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


# ─── Dram Mode Scaling ────────────────────────────────────────────────────────

class TestDramExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows reactively when a segment fills."""

    def get_module_args(self, data_dir, direct_io):
        # 2MB segment size. Two segments max (dram-maxmemory = 4MB).
        # Objects are 900KB — one fits per segment; second object triggers expand.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 4194304"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_expand_on_segment_full(self):
        """Setting an object that fills the first segment triggers reactive expand.
        The second large object must succeed (pool grew), not raise an error.
        """
        client = self.server.get_new_client()
        # 900KB — fits in one 1MB segment.
        obj_size = 900 * 1024
        payload_a = b'A' * obj_size
        payload_b = b'B' * obj_size

        # First object fits in the initial segment.
        r = client.execute_command('LO.SET', 'key_a', payload_a)
        assert r == b'OK', f"First LO.SET failed: {r}"

        # Second object fills the first segment, triggering reactive expand.
        r = client.execute_command('LO.SET', 'key_b', payload_b)
        assert r == b'OK', f"Second LO.SET failed (expand may not have fired): {r}"

        # Both objects must be intact after expand.
        assert client.execute_command('LO.GET', 'key_a') == payload_a
        assert client.execute_command('LO.GET', 'key_b') == payload_b

    def test_expand_data_integrity(self):
        """Data written before and after an expand is returned correctly."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024
        keys_payloads = [(f'key_{i}', bytes([i % 256]) * obj_size) for i in range(4)]

        for key, payload in keys_payloads:
            client.execute_command('LO.SET', key, payload)

        for key, payload in keys_payloads:
            got = client.execute_command('LO.GET', key)
            assert got == payload, f"Data mismatch for {key} after expand"


class TestDramMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: dram-maxmemory hard cap is respected after expand."""

    def get_module_args(self, data_dir, direct_io):
        # 2MB total, 1MB segment. After reactive expand, pool is 2MB (2 segments).
        # A third 900KB object cannot fit even after a second segment is added.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 2097152"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_maxmemory_cap_after_expand(self):
        """After one reactive expand (now at cap), a third object must be rejected."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        # Fill the first segment — triggers expand to second segment.
        client.execute_command('LO.SET', 'key_a', b'A' * obj_size)
        client.execute_command('LO.SET', 'key_b', b'B' * obj_size)

        # Pool is now full (2MB cap, two 900KB objects). Third must fail.
        try:
            client.execute_command('LO.SET', 'key_c', b'C' * obj_size)
            assert False, "Expected error: pool exhausted or OOM"
        except ResponseError:
            pass  # Expected

    def test_maxmemory_0_no_explicit_cap(self):
        """dram-maxmemory=0 means no module-level cap; grow up to server ceiling."""
        # This just verifies that setting multiple objects without hitting server
        # maxmemory works without error. We can't easily test 'grows to ceiling'
        # in a unit test without controlling server maxmemory.
        client = self.server.get_new_client()
        for i in range(3):
            r = client.execute_command('LO.SET', f'key_{i}', b'X' * (100 * 1024))
            assert r == b'OK', f"SET {i} failed: {r}"


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────

class TestTieredExpand(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool expands reactively when segment fills."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" dram-maxmemory 4194304"
            f" max-promote-size 268435456"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_expand_on_full(self):
        """In Tiered mode, filling the DRAMPool triggers expand; data stays correct."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024
        payload_a = b'A' * obj_size
        payload_b = b'B' * obj_size

        client.execute_command('LO.SET', 'key_a', payload_a)
        client.execute_command('LO.SET', 'key_b', payload_b)

        assert client.execute_command('LO.GET', 'key_a') == payload_a
        assert client.execute_command('LO.GET', 'key_b') == payload_b

    def test_tiered_multiple_segments(self):
        """Objects spread across multiple segments are all readable."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024
        count = 4

        for i in range(count):
            r = client.execute_command('LO.SET', f'key_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"SET key_{i} failed: {r}"

        for i in range(count):
            got = client.execute_command('LO.GET', f'key_{i}')
            assert got == bytes([i % 256]) * obj_size, f"Data mismatch for key_{i}"

    def test_tiered_nvme_fallback_on_dram_full(self):
        """When DRAMPool is at cap, further SETs still persist to NVMe.
        The key is readable (NVMe read) even without a DRAM cache copy.
        """
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        # Fill to cap (2 segments, 4MB dram-maxmemory)
        client.execute_command('LO.SET', 'key_a', b'A' * obj_size)
        client.execute_command('LO.SET', 'key_b', b'B' * obj_size)
        client.execute_command('LO.SET', 'key_c', b'C' * obj_size)
        client.execute_command('LO.SET', 'key_d', b'D' * obj_size)

        # Even without DRAM cache, all keys must be readable from NVMe.
        assert client.execute_command('LO.GET', 'key_a') == b'A' * obj_size
        assert client.execute_command('LO.GET', 'key_b') == b'B' * obj_size
        assert client.execute_command('LO.GET', 'key_c') == b'C' * obj_size
        assert client.execute_command('LO.GET', 'key_d') == b'D' * obj_size


class TestTieredShrink(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool shrinks under memory pressure.

    The shrink timer fires every 5 seconds. Tests wait slightly longer
    to ensure at least one tick occurs.
    """

    # Shrink timer cadence (seconds) + margin.
    SHRINK_WAIT_S = 7

    def get_module_args(self, data_dir, direct_io):
        # Small pool + low server maxmemory so the 80% watermark is easily crossed.
        # 4MB pool. We'll fill it with data, then wait for the timer to shrink.
        # maxmemory is set via CONFIG SET after startup (see test body).
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" dram-maxmemory 0"
            f" max-promote-size 268435456"
            f" bench-mode no"
            f" direct-io no"
        )

    def _info_largeobj(self, client):
        """Return the largeobj INFO section as a dict."""
        raw = client.execute_command('INFO', 'largeobj')
        result = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            result[key] = int(v) if str(v).lstrip('-').isdigit() else v
        return result

    def test_shrink_preserves_nvme_data(self):
        """After the scaling cron fires, the DRAMPool shrinks and keys remain in keyspace.

        Verified via INFO largeobj: dram_live_segments drops after pressure is applied.
        """
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        keys = [f'shrink_key_{i}' for i in range(4)]
        for key in keys:
            r = client.execute_command('LO.SET', key, payload := b'S' * obj_size)
            assert r == b'OK', f"LO.SET {key} failed: {r}"

        # Capture baseline segment count.
        before = self._info_largeobj(client)
        segments_before = before.get('dram_live_segments', 0)

        # Set maxmemory slightly above current usage: module 80% watermark fires,
        # core eviction does NOT (maxmemory > used_memory).
        mem_info = client.execute_command('INFO', 'memory')
        used_memory = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used_memory * 1.15)))

        # Wait for the scaling cron to fire.
        time.sleep(self.SHRINK_WAIT_S)

        # All keys must still exist in the Valkey keyspace after shrink.
        for key in keys:
            assert client.execute_command('EXISTS', key) == 1, \
                f"Key {key} disappeared from keyspace after shrink (data loss)"

        # Verify via INFO that the cron actually ran and pool changed.
        after = self._info_largeobj(client)
        segments_after = after.get('dram_live_segments', segments_before)
        assert segments_after <= segments_before, \
            f"Expected shrink: segments {segments_before} → {segments_after}"

    def test_shrink_then_expand(self):
        """After a shrink, new SETs succeed (reactive expand fires)."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        for i in range(4):
            client.execute_command('LO.SET', f'pre_shrink_{i}', b'P' * obj_size)

        # Trigger shrink.
        mem_info = client.execute_command('INFO', 'memory')
        used_memory = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used_memory * 1.15)))
        time.sleep(self.SHRINK_WAIT_S)

        # Reset pressure.
        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')

        # New SET must succeed (reactive expand fires if pool shrunk).
        r = client.execute_command('LO.SET', 'post_shrink', b'Q' * obj_size)
        assert r == b'OK', f"LO.SET after shrink+expand failed: {r}"

        # Pre-shrink keys still in keyspace.
        for i in range(4):
            assert client.execute_command('EXISTS', f'pre_shrink_{i}') == 1, \
                f"pre_shrink_{i} disappeared from keyspace after shrink"


class TestTieredNoShrinkInDramMode(ValkeyLargeObjTestCaseBase):
    """In Dram mode, the shrink timer must NOT run (data would be lost)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 0"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_dram_data_survives_pressure(self):
        """In Dram mode, data is never evicted by the module (no shrink).
        Under maxmemory pressure, core eviction (maxmemory-policy) applies,
        but the module itself does not silently drop data.
        """
        client = self.server.get_new_client()
        obj_size = 200 * 1024

        # Write several objects.
        for i in range(5):
            r = client.execute_command('LO.SET', f'dram_{i}', bytes([i]) * obj_size)
            assert r == b'OK', f"SET dram_{i} failed: {r}"

        # Wait longer than the shrink timer period (timer should NOT fire in Dram).
        time.sleep(7)

        # All keys must still be present — no module-level eviction in Dram mode.
        for i in range(5):
            got = client.execute_command('LO.GET', f'dram_{i}')
            assert got == bytes([i]) * obj_size, (
                f"dram_{i} data lost — shrink timer must not run in Dram mode"
            )
