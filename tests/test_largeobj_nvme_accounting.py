import os
import glob
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal


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
        """Wait for Valkey lazyfree to drain AND the teardown worker to unlink."""
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
        # teardown worker runs -- the wait below is load-bearing.
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
