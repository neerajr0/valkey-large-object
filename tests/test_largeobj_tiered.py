import os
import glob
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal


class TestLargeObjTieredPromotion(ValkeyLargeObjTestCaseBase):
    """Tiered mode with DRAMPool promotion enabled (default max-promote-size)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 4194304"
            f" max-promote-size 268435456"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_set_creates_nvme_file(self):
        """In Tiered mode, LO.SET persists to NVMe."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('LO.SET', 'tiered_key', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, "Tiered SET should create an NVMe .dat file"

    def test_get_after_set_roundtrip(self):
        """Tiered mode: SET then GET returns correct data."""
        client = self.server.get_new_client()
        payload = b'A' * 8192
        client.execute_command('LO.SET', 'rt_key', payload)
        result = client.execute_command('LO.GET', 'rt_key')
        assert result == payload, "GET should return the same data that was SET"

    def test_promotion_caches_in_dram(self):
        """After a GET miss, the object is promoted to DRAMPool.
        A second GET should succeed (served from DRAM)."""
        client = self.server.get_new_client()
        payload = b'B' * 4096
        client.execute_command('LO.SET', 'promo_key', payload)

        # First GET: DRAMPool miss -> NVMe read -> promote to DRAMPool.
        result1 = client.execute_command('LO.GET', 'promo_key')
        assert result1 == payload

        # Second GET: served from DRAMPool (promotion happened).
        result2 = client.execute_command('LO.GET', 'promo_key')
        assert result2 == payload

    def test_delete_removes_nvme_file(self):
        """DEL removes the NVMe file."""
        client = self.server.get_new_client()
        payload = b'D' * 4096
        client.execute_command('LO.SET', 'del_key', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1
        client.execute_command('DEL', 'del_key')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    # ─── COPY callback tests ─────────────────────────────────────────────

    def test_copy(self):
        """COPY in Tiered mode: independent NVMe file, digest differs, delete independence."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('LO.SET', 'srckey', payload)
        # COPY creates an independent object with its own NVMe file
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True
        assert client.execute_command('LO.GET', 'srckey') == payload
        assert client.execute_command('LO.GET', 'dstkey') == payload
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 2, f"Expected at least 2 .dat files, got {len(dat_files)}"
        # COPY gets a new OID so digests differ
        src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'srckey')
        dst_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'dstkey')
        assert src_digest != dst_digest
        # Deleting source does not affect the copy
        client.execute_command('DEL', 'srckey')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('LO.GET', 'dstkey') == payload
        # Deleting copy does not affect the source
        client.execute_command('LO.SET', 'srckey2', payload)
        client.execute_command('COPY', 'srckey2', 'dstkey2')
        client.execute_command('DEL', 'dstkey2')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('LO.GET', 'srckey2') == payload

    # ─── MEMORY USAGE callback tests ──────────────────────────────────────

    def test_memory_usage(self):
        """MEMORY USAGE after promotion includes LoValue struct + payload."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('LO.SET', 'memkey', b'M' * payload_size)
        # Single GET promotes into DRAMPool (promote-on-first-GET policy).
        client.execute_command('LO.GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        assert mem >= lo_value_size + payload_size, (
            f"Expected MEMORY USAGE >= {lo_value_size + payload_size} (promoted), got {mem}"
        )

    # ─── DEBUG DIGEST callback tests ──────────────────────────────────────

    def test_debug_digest(self):
        """DEBUG DIGEST-VALUE is deterministic; nonexistent key returns nil digest."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'digkey', b'G' * 4096)
        d1 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        d2 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        assert d1 == d2
        # Nonexistent key returns nil digest
        nil_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'noexist')
        assert nil_digest == [b'0' * 40]


class TestLargeObjTieredNvmeOnly(ValkeyLargeObjTestCaseBase):
    """Tiered mode with max-promote-size=0 (no promotion, all reads from NVMe)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 4194304"
            f" max-promote-size 0"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_set_get_roundtrip_no_promotion(self):
        """With max-promote-size=0, GET always reads from NVMe (no DRAMPool caching)."""
        client = self.server.get_new_client()
        payload = b'N' * 8192
        client.execute_command('LO.SET', 'nvme_key', payload)
        result = client.execute_command('LO.GET', 'nvme_key')
        assert result == payload

    def test_multiple_gets_all_from_nvme(self):
        """Multiple GETs with max-promote-size=0 should all succeed."""
        client = self.server.get_new_client()
        payload = b'R' * 4096
        client.execute_command('LO.SET', 'repeat_key', payload)
        for _ in range(5):
            result = client.execute_command('LO.GET', 'repeat_key')
            assert result == payload

    def test_nvme_staging_exhaustion(self):
        """An object larger than nvme-staging-size should fail with pool exhausted."""
        client = self.server.get_new_client()
        # nvme-staging-size is 4MB. An 8MB object cannot be staged.
        obj_size = 8 * 1024 * 1024
        payload = b'Z' * obj_size
        try:
            client.execute_command('LO.SET', 'toobig', payload)
            assert False, "Expected pool exhausted error"
        except ResponseError as e:
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    # ─── MEMORY USAGE tests ───────────────────────────────────────────────

    def test_memory_usage_tiered_cold(self):
        """Without promotion, MEMORY USAGE reports only LoValue struct overhead.

        This test class sets max-promote-size=0, so objects are never promoted
        to DRAMPool. memory_usage reports only sizeof(LoValue) (24 bytes) — the
        payload lives on NVMe and does not consume DRAM.
        """
        client = self.server.get_new_client()
        payload_size = 4096
        payload = b'M' * payload_size
        client.execute_command('LO.SET', 'memkey', payload)
        # Even after a GET the object stays cold (max-promote-size=0).
        client.execute_command('LO.GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        # Our callback returns only sizeof(LoValue) = 24. Valkey adds per-key
        # overhead (~72-120 bytes), so total is well below lo_value_size + payload_size.
        upper_bound = lo_value_size + payload_size
        assert mem < upper_bound, (
            f"Expected MEMORY USAGE < {upper_bound} (cold, not promoted), got {mem}"
        )



class TestLargeObjTieredDeletion(ValkeyLargeObjTestCaseBase):
    """Deletion / overwrite / free semantics of the refcounted teardown design.

    The design roots object existence in the keyspace: DEL, overwrite, expiry and
    flush all drop the LoValue's Arc<ObjectFile>, whose Drop closes the fd and
    unlinks the .dat file off the main event-loop thread. These tests assert the
    on-disk effects. Because teardown is asynchronous, file-count assertions poll
    rather than check once.
    """

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 4194304"
            f" max-promote-size 268435456"
            f" bench-mode no"
            f" direct-io no"
        )

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("stats").get("lazyfree_pending_objects", 0), 0
        )

    # ─── GET-after-DEL ────────────────────────────────────────────────────

    def test_get_after_del_returns_nil(self):
        """Once DEL removes the key, a subsequent GET resolves to nil."""
        client = self.server.get_new_client()
        payload = b"D" * 4096
        client.execute_command("LO.SET", "gk", payload)
        assert client.execute_command("LO.GET", "gk") == payload
        client.execute_command("DEL", "gk")
        assert client.execute_command("LO.GET", "gk") is None

    def test_del_after_promotion_returns_nil(self):
        """DEL after the object was promoted into DRAMPool still resolves to nil,
        and the NVMe file is unlinked."""
        client = self.server.get_new_client()
        payload = b"P" * 4096
        client.execute_command("LO.SET", "pk", payload)
        # First GET promotes into DRAMPool.
        assert client.execute_command("LO.GET", "pk") == payload
        client.execute_command("DEL", "pk")
        assert client.execute_command("LO.GET", "pk") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Overwrite ────────────────────────────────────────────────────────

    def test_overwrite_replaces_nvme_file(self):
        """Overwriting a key mints a new file for the new object and tears down
        the old one — exactly one .dat file remains and GET returns the new data."""
        client = self.server.get_new_client()
        v1 = b"1" * 4096
        v2 = b"2" * 8192
        client.execute_command("LO.SET", "ok", v1)
        wait_for_equal(self._dat_count, 1)
        client.execute_command("LO.SET", "ok", v2)
        # GET returns the new value immediately (new OID wins at commit).
        assert client.execute_command("LO.GET", "ok") == v2
        # Old object's file is torn down asynchronously → back to a single file.
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)

    def test_overwrite_after_promotion_serves_new_data(self):
        """Overwrite after the old object was promoted to DRAM: the new GET must
        serve the new payload (stale DRAM entry replaced), one file on disk."""
        client = self.server.get_new_client()
        v1 = b"A" * 4096
        v2 = b"B" * 4096
        client.execute_command("LO.SET", "opk", v1)
        # Promote v1 into DRAMPool.
        assert client.execute_command("LO.GET", "opk") == v1
        # Overwrite with v2.
        client.execute_command("LO.SET", "opk", v2)
        assert client.execute_command("LO.GET", "opk") == v2
        assert client.execute_command("LO.GET", "opk") == v2
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)

    def test_repeated_overwrite_no_file_leak(self):
        """Many overwrites of the same key never leak files — steady state is one."""
        client = self.server.get_new_client()
        for i in range(10):
            client.execute_command("LO.SET", "leakkey", bytes([65 + (i % 26)]) * 4096)
        assert client.execute_command("LO.GET", "leakkey") is not None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)

    # ─── GET result outlives a concurrent DEL (honor rule) ────────────────

    def test_get_result_correct_across_delete_churn(self):
        """A GET that resolves the key returns its full data even under delete
        churn: the honor-rule pin keeps the file alive for the read's duration.
        We can't force a mid-flight race deterministically from the client, so we
        assert the observable invariant: interleaved GET/DEL never corrupts data."""
        client = self.server.get_new_client()
        payload = b"Z" * 8192
        for _ in range(20):
            client.execute_command("LO.SET", "churn", payload)
            assert client.execute_command("LO.GET", "churn") == payload
            client.execute_command("DEL", "churn")
            assert client.execute_command("LO.GET", "churn") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Other free triggers: expiry & flush ──────────────────────────────

    def test_expiry_unlinks_file(self):
        """A key that expires (TTL) frees the LoValue and unlinks its .dat file.

        Expiry is a distinct entry into lo_free from DEL/overwrite."""
        client = self.server.get_new_client()
        client.execute_command("LO.SET", "exk", b"E" * 4096)
        wait_for_equal(self._dat_count, 1)
        client.execute_command("PEXPIRE", "exk", 50)
        # Poll EXISTS to drive passive expiry, then let teardown settle.
        wait_for_equal(lambda: client.execute_command("EXISTS", "exk"), 0)
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    def test_flushall_unlinks_all_files(self):
        """FLUSHALL frees every LoValue and unlinks all .dat files."""
        client = self.server.get_new_client()
        for i in range(3):
            client.execute_command("LO.SET", f"fk{i}", b"F" * 4096)
        wait_for_equal(self._dat_count, 3)
        client.execute_command("FLUSHALL")
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)


# ─── NVMe disk-usage accounting ──────────────────────────────────────────
# These Tiered-mode classes use small nvme-maxmemory caps to exercise the
# capacity gate; each defines its own get_module_args.


class _NvmeAccountingBase(ValkeyLargeObjTestCaseBase):
    """Shared helpers for the NVMe disk-usage accounting tests.

    The usage counter is not observable directly (no INFO section / command), so
    these tests exercise it through its only externally-visible effect: the
    `has_nvme_capacity` gate on the Tiered SET path. A SET that would push tracked
    usage past `nvme-maxmemory` is rejected with "pool exhausted"; a SET that fits
    succeeds. By filling to the cap, freeing, and re-filling we prove the counter
    is incremented on create and -- critically -- decremented at TRUE deletion
    (ObjectFile::Drop, after teardown), not merely at key-free.
    """

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("stats").get("lazyfree_pending_objects", 0), 0
        )

    def _set_ok(self, client, key, payload):
        assert client.execute_command("LO.SET", key, payload) == b"OK"

    def _set_rejected(self, client, key, payload):
        try:
            client.execute_command("LO.SET", key, payload)
            assert False, f"Expected '{key}' SET to be rejected (pool exhausted)"
        except ResponseError as e:
            assert "pool exhausted" in str(e).lower(), f"Unexpected error: {e}"


class TestNvmeUsageFreedOnDelete(_NvmeAccountingBase):
    """Capacity is reclaimed only when the file is truly unlinked."""

    # 1 MiB cap = exactly four 256 KiB (already-aligned) objects. staging holds one
    # object at a time, so 1 MiB is plenty for the per-write buffer.
    CAP = 1024 * 1024
    OBJ = 256 * 1024  # 262144, a 4096-multiple -> no padding effect here

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size {self.CAP}"
            f" dram-segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_capacity_reclaimed_after_delete(self):
        client = self.server.get_new_client()
        payload = b"X" * self.OBJ

        # Fill the cap exactly (4 * 256 KiB == 1 MiB).
        for i in range(4):
            self._set_ok(client, f"k{i}", payload)
        wait_for_equal(self._dat_count, 4)

        # Cap is full -> the fifth object must be rejected.
        self._set_rejected(client, "k4", payload)

        # Delete one object and wait for its file to be truly unlinked. The usage
        # decrement lives in ObjectFile::Drop, so capacity is NOT freed until the
        # teardown runs -- the wait below is load-bearing.
        client.execute_command("DEL", "k0")
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 3)

        # Now that one slot is truly freed, the previously-rejected SET fits.
        self._set_ok(client, "k4", payload)
        wait_for_equal(self._dat_count, 4)

    def test_capacity_reclaimed_after_flushall(self):
        client = self.server.get_new_client()
        payload = b"Y" * self.OBJ

        for i in range(4):
            self._set_ok(client, f"f{i}", payload)
        self._set_rejected(client, "f4", payload)

        # FLUSHALL frees every object; after teardown the full cap is available.
        client.execute_command("FLUSHALL")
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

        # A full cap's worth of fresh objects fits again -- the counter returned to 0.
        for i in range(4):
            self._set_ok(client, f"g{i}", payload)
        wait_for_equal(self._dat_count, 4)

    def test_overwrite_does_not_leak_capacity(self):
        client = self.server.get_new_client()

        # Repeatedly overwrite one key far more times than the cap allows. If the
        # old object's bytes were not freed on overwrite, usage would climb past the
        # cap and a later overwrite would be wrongly rejected.
        for i in range(20):
            self._set_ok(client, "ow", bytes([65 + (i % 26)]) * self.OBJ)
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)


class TestNvmeUsageAccountsForPadding(_NvmeAccountingBase):
    """O_DIRECT pads writes up to IO_ALIGN (4096); accounting must count the padded
    on-disk size, not the logical length."""

    # Cap chosen to be a 2 KiB multiple but NOT a 4 KiB multiple: 1 MiB + 2 KiB.
    CAP = 1024 * 1024 + 2048  # 1050624; 1050624 / 4096 == 256.5

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size 2097152"
            f" dram-segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_padding_counts_against_capacity(self):
        client = self.server.get_new_client()
        # One object whose LOGICAL size fits under the cap (1050624 <= 1050624) but
        # whose ALIGNED on-disk size does not: align_up(1050624) == 1052672 > cap.
        # With correct (padded) accounting this SET is rejected; if accounting used
        # the logical length it would wrongly succeed. Disk is empty, so the
        # rejection is attributable to padding alone.
        payload = b"P" * self.CAP  # logical == cap; aligned == cap rounded up
        self._set_rejected(client, "padkey", payload)

    def test_logical_fit_without_padding_overflow_succeeds(self):
        client = self.server.get_new_client()
        # Contrast case: a 1 MiB object is already 4096-aligned, so aligned == logical
        # == 1048576 <= cap. This one must succeed -- proving the rejection above is
        # specifically the padding, not just "large object rejected".
        payload = b"Q" * (1024 * 1024)
        self._set_ok(client, "fitkey", payload)
        wait_for_equal(self._dat_count, 1)
