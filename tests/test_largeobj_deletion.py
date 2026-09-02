import os
import glob
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal


class TestLargeObjDeletion(ValkeyLargeObjTestCaseBase):
    """Deletion / overwrite / free semantics of the refcounted teardown design.

    The design roots object existence in the keyspace: DEL, overwrite, expiry and
    flush all drop the LoValue's Arc<ObjectFile>, whose Drop closes the fd and
    unlinks the .dat file off the main thread (via the teardown worker). These
    tests assert the on-disk effects. Because teardown is asynchronous, file-count
    assertions poll rather than check once.
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
        """Wait for Valkey lazyfree to drain AND the teardown worker to unlink."""
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
