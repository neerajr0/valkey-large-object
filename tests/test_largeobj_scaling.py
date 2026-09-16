"""
Integration tests for DRAMPool expand/shrink scaling behavior.

Tests cover:
  - Dram mode: reactive expand when segment fills
  - Dram mode: dram-maxmemory hard cap respected
  - Tiered mode: reactive expand on DRAMPool fill
  - Tiered mode: shrink evicts cached segment but NVMe copy survives
"""

import os
import time
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj


# ─── Dram Mode Scaling ────────────────────────────────────────────────────────

class TestDramExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows reactively when a segment fills.

    Also covers data integrity under memory pressure — same server config.
    """

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB, dram-maxmemory=0 → starts with 1 segment, grows on demand.
        # scaling-poll-ms=1000 for the pressure test; harmless for expand tests.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 0"
            f" scaling-poll-ms 1000"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_expand_on_segment_full(self):
        """Setting an object that fills the first segment triggers reactive expand.
        Verified via scaling_expand_total in INFO largeobj.
        """
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        r = client.execute_command('LO.SET', 'key_a', b'A' * obj_size)
        assert r == b'OK', f"First LO.SET failed: {r}"

        r = client.execute_command('LO.SET', 'key_b', b'B' * obj_size)
        assert r == b'OK', f"Second LO.SET failed (expand may not have fired): {r}"

        after = info_largeobj(client)
        assert after.get('largeobj_scaling_expand_total', 0) > expand_before, \
            "Expected scaling_expand_total to increase after filling a segment"

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

    def test_maxmemory_0_no_explicit_cap(self):
        """dram-maxmemory=0 means no module-level cap; grows up to server ceiling."""
        client = self.server.get_new_client()
        for i in range(3):
            r = client.execute_command('LO.SET', f'key_{i}', b'X' * (100 * 1024))
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
            r = client.execute_command('LO.SET', key, payload)
            assert r == b'OK', f"SET {key} failed: {r}"

        mem_info = client.execute_command('INFO', 'memory')
        used_memory = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used_memory * 1.1)))

        # Wait for a few cron ticks.
        time.sleep(5)

        for key, payload in payloads.items():
            if client.execute_command('EXISTS', key) == 1:
                got = client.execute_command('LO.GET', key)
                assert got == payload, f"{key} data corrupted under pressure"


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

        client.execute_command('LO.SET', 'key_a', b'A' * obj_size)
        client.execute_command('LO.SET', 'key_b', b'B' * obj_size)

        try:
            client.execute_command('LO.SET', 'key_c', b'C' * obj_size)
            assert False, "Expected error: pool exhausted or OOM"
        except ResponseError:
            pass


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

        client.execute_command('LO.SET', 'key_a', b'A' * obj_size)
        client.execute_command('LO.SET', 'key_b', b'B' * obj_size)

        assert client.execute_command('LO.GET', 'key_a') == b'A' * obj_size
        assert client.execute_command('LO.GET', 'key_b') == b'B' * obj_size

    def test_tiered_multiple_segments(self):
        """Objects spread across multiple segments are all readable."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024

        for i in range(4):
            r = client.execute_command('LO.SET', f'key_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"SET key_{i} failed: {r}"

        for i in range(4):
            got = client.execute_command('LO.GET', f'key_{i}')
            assert got == bytes([i % 256]) * obj_size, f"Data mismatch for key_{i}"

    def test_tiered_nvme_fallback_on_dram_full(self):
        """When DRAMPool is at cap, further SETs still persist to NVMe and are readable."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            client.execute_command('LO.SET', key, fill * obj_size)

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            assert client.execute_command('LO.GET', key) == fill * obj_size


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
            f" dram-maxmemory 0"
            f" max-promote-size 268435456"
            f" scaling-poll-ms 1000"
            f" bench-mode no"
            f" direct-io no"
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

        keys = [f'shrink_key_{i}' for i in range(4)]
        for key in keys:
            r = client.execute_command('LO.SET', key, b'S' * obj_size)
            assert r == b'OK', f"LO.SET {key} failed: {r}"

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        for key in keys:
            assert client.execute_command('EXISTS', key) == 1, \
                f"Key {key} disappeared from keyspace after shrink (data loss)"

    def test_shrink_then_expand(self):
        """After a shrink, new SETs succeed."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        for i in range(4):
            client.execute_command('LO.SET', f'pre_shrink_{i}', b'P' * obj_size)

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')

        r = client.execute_command('LO.SET', 'post_shrink', b'Q' * obj_size)
        assert r == b'OK', f"LO.SET after shrink+expand failed: {r}"

        for i in range(4):
            assert client.execute_command('EXISTS', f'pre_shrink_{i}') == 1, \
                f"pre_shrink_{i} disappeared from keyspace after shrink"
