import os
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestLargeObjDram(ValkeyLargeObjTestCaseBase):
    """Dram-only mode: all objects live in DRAMPool, no NVMe."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_set_get_roundtrip(self):
        """Basic SET + GET in Dram mode."""
        client = self.server.get_new_client()
        payload = b'A' * 4096
        result = client.execute_command('LO.SET', 'dramkey', payload)
        assert result == b'OK'
        data = client.execute_command('LO.GET', 'dramkey')
        assert data == payload

    def test_get_nonexistent_key(self):
        """GET on nonexistent key returns nil in Dram mode."""
        client = self.server.get_new_client()
        result = client.execute_command('LO.GET', 'nokey')
        assert result is None

    def test_overwrite_key(self):
        """SET same key twice returns OK both times."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'overkey', b'X' * 4096)
        client.execute_command('LO.SET', 'overkey', b'Y' * 4096)
        data = client.execute_command('LO.GET', 'overkey')
        assert data == b'Y' * 4096

    def test_delete_key(self):
        """DEL removes the key, subsequent GET returns nil."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'delkey', b'Z' * 4096)
        client.execute_command('DEL', 'delkey')
        result = client.execute_command('LO.GET', 'delkey')
        assert result is None

    def test_dram_pool_exhaustion(self):
        """An object larger than dram-segment-size fails with pool exhausted."""
        client = self.server.get_new_client()
        # dram-segment-size is 1MB. A 2MB object cannot be allocated.
        obj_size = 2 * 1024 * 1024
        payload = b'D' * obj_size
        try:
            client.execute_command('LO.SET', 'toobig', payload)
            assert False, "Expected pool exhausted error"
        except ResponseError as e:
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    def test_multiple_objects(self):
        """Multiple small objects can coexist in DRAMPool."""
        client = self.server.get_new_client()
        for i in range(10):
            payload = bytes([i % 256]) * 4096
            client.execute_command('LO.SET', f'multi{i}', payload)
        for i in range(10):
            data = client.execute_command('LO.GET', f'multi{i}')
            expected = bytes([i % 256]) * 4096
            assert data == expected, f"Key multi{i} mismatch"
