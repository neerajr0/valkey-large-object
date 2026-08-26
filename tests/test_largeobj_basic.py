import os
import glob
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestLargeObjBasic(ValkeyLargeObjTestCaseBase):

    def test_module_loaded(self):
        """Verify the largeobj module is loaded."""
        client = self.server.get_new_client()
        module_list = client.execute_command('MODULE LIST')
        module_names = [m[b'name'] for m in module_list]
        assert b'largeobj' in module_names or b'largeob-k' in module_names

    def test_lo_set_get_roundtrip(self):
        """LO.SET writes data, LO.GET retrieves it."""
        client = self.server.get_new_client()
        payload = b'A' * 4096
        result = client.execute_command('LO.SET', 'testkey', '4096', payload)
        assert result == b'OK'
        # GET returns the object bytes
        data = client.execute_command('LO.GET', 'testkey')
        assert len(data) == 4096
        assert data == payload

    def test_lo_get_nonexistent_key(self):
        """LO.GET on a nonexistent key returns nil."""
        client = self.server.get_new_client()
        result = client.execute_command('LO.GET', 'nokey')
        assert result is None

    def test_lo_set_creates_nvme_file(self):
        """LO.SET creates a .dat file in data-dir."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('LO.SET', 'filekey', '4096', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, f"Expected .dat file in {self.data_dir}, found: {os.listdir(self.data_dir)}"

    def test_delete_removes_nvme_file(self):
        """DEL on an LO key removes the .dat file."""
        client = self.server.get_new_client()
        payload = b'Y' * 4096
        client.execute_command('LO.SET', 'delkey', '4096', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1, "LO.SET didn't create a .dat file"
        client.execute_command('DEL', 'delkey')
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    def test_pool_exhaustion_error(self):
        """An object larger than the NVMe pool segment fails allocation."""
        client = self.server.get_new_client()
        # NVMe pool is 1MB. An object of 2MB cannot be allocated.
        obj_size = 2 * 1024 * 1024
        payload = 'A' * obj_size
        try:
            client.execute_command(f'LO.SET toobig {obj_size} {payload}')
            assert False, "Expected error for object larger than pool but command succeeded"
        except ResponseError as e:
            # Allocation failure from talc when object exceeds segment capacity.
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    def test_bench_mode_reply_format(self):
        """With bench-mode=yes, LO.GET returns integer size."""
        # This test requires server started with bench-mode=yes.
        # Skip if not configured — the base setup uses bench-mode=no.
        pass  # TODO: parametrize setup_test with bench-mode=yes variant
