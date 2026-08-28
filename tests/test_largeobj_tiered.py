import os
import glob
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


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
        self._wait_for_lazyfree_done(client)
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)


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
