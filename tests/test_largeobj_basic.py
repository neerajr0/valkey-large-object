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
        result = client.execute_command('LO.SET', 'testkey', payload)
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
        client.execute_command('LO.SET', 'filekey', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, f"Expected .dat file in {self.data_dir}, found: {os.listdir(self.data_dir)}"

    def test_delete_removes_nvme_file(self):
        """DEL on an LO key removes the .dat file."""
        client = self.server.get_new_client()
        payload = b'Y' * 4096
        client.execute_command('LO.SET', 'delkey', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1, "LO.SET didn't create a .dat file"
        client.execute_command('DEL', 'delkey')
        # Free is async (BIO thread). Wait until lazyfree completes before checking the filesystem.
        self._wait_for_lazyfree_done(client)
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    def test_pool_exhaustion_error(self):
        """An object larger than nvme-staging-size fails allocation on the NVMe staging pool."""
        client = self.server.get_new_client()
        # nvme-staging-size is 1MB (base class default). A 2MB object cannot be staged.
        obj_size = 2 * 1024 * 1024
        payload = b'A' * obj_size
        try:
            client.execute_command('LO.SET', 'toobig', payload)
            assert False, "Expected error for object larger than pool but command succeeded"
        except ResponseError as e:
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    def test_bench_mode_reply_format(self):
        """With bench-mode=yes, LO.GET returns integer size."""
        # This test requires server started with bench-mode=yes.
        # Skip if not configured — the base setup uses bench-mode=no.
        pass  # TODO: parametrize setup_test with bench-mode=yes variant

    # ─── COPY command tests ───────────────────────────────────────────────

    def test_copy_creates_independent_object(self):
        """COPY creates a new large object with its own OID and NVMe file."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('LO.SET', 'srckey', payload)

        # COPY srckey → dstkey
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True, f"COPY returned {result}"

        # Both keys should be readable
        src_data = client.execute_command('LO.GET', 'srckey')
        dst_data = client.execute_command('LO.GET', 'dstkey')
        assert src_data == payload
        assert dst_data == payload

        # Verify two .dat files exist (source + copy = independent files)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 2, f"Expected at least 2 .dat files, got {len(dat_files)}"

    def test_copy_different_digest(self):
        """COPY of an object has a different DEBUG DIGEST than the original.

        Because the copy gets a new OID, its digest (which includes the OID)
        must differ from the source key's digest.
        """
        client = self.server.get_new_client()
        payload = b'D' * 4096
        client.execute_command('LO.SET', 'digestsrc', payload)
        client.execute_command('COPY', 'digestsrc', 'digestdst')

        src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digestsrc')
        dst_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digestdst')

        # Digests should differ because OID is included in the digest
        assert src_digest != dst_digest, (
            f"Expected different digests for src and dst, both got {src_digest}"
        )

    def test_copy_source_unaffected_by_dst_delete(self):
        """Deleting a COPY destination does not affect the source."""
        client = self.server.get_new_client()
        payload = b'E' * 4096
        client.execute_command('LO.SET', 'copysrc', payload)
        client.execute_command('COPY', 'copysrc', 'copydst')

        # Delete the copy
        client.execute_command('DEL', 'copydst')

        # Source should still be readable
        src_data = client.execute_command('LO.GET', 'copysrc')
        assert src_data == payload

    # ─── MEMORY USAGE tests ───────────────────────────────────────────────

    def test_memory_usage_returns_positive(self):
        """MEMORY USAGE on a large object key returns a positive value."""
        client = self.server.get_new_client()
        payload = b'M' * 4096
        client.execute_command('LO.SET', 'memkey', payload)

        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        assert mem > 0, f"Expected positive memory usage, got {mem}"

    def test_memory_usage_reflects_object_size(self):
        """MEMORY USAGE includes the on-disk object size."""
        client = self.server.get_new_client()
        payload = b'N' * 4096
        client.execute_command('LO.SET', 'memkey2', payload)

        mem = client.execute_command('MEMORY', 'USAGE', 'memkey2')
        # Our mem_usage callback returns sizeof(LoValue) + obj_len = 24 + 4096 = 4120.
        # Valkey adds per-key overhead (dict entry, robj, SDS key name, etc.) on top.
        # The total should be exactly our callback value + Valkey's key overhead.
        # Valkey key overhead is ~72-120 bytes depending on version, so assert a
        # tight range: at least 4120 (our callback) and no more than 4300 (reasonable cap).
        assert 4120 <= mem <= 4300, (
            f"Expected MEMORY USAGE in [4120, 4300], got {mem}"
        )

    # ─── DEBUG DIGEST tests ───────────────────────────────────────────────

    def test_debug_digest_deterministic(self):
        """DEBUG DIGEST-VALUE is deterministic for the same key."""
        client = self.server.get_new_client()
        payload = b'G' * 4096
        client.execute_command('LO.SET', 'digkey', payload)

        d1 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        d2 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        assert d1 == d2, f"Digests differ: {d1} vs {d2}"

    def test_debug_digest_nonexistent_key(self):
        """DEBUG DIGEST-VALUE on nonexistent key returns the nil digest."""
        client = self.server.get_new_client()
        result = client.execute_command('DEBUG', 'DIGEST-VALUE', 'noexist')
        # Valkey returns a list with a single element: 40 zero hex chars (empty digest)
        assert result == [b'0000000000000000000000000000000000000000'], (
            f"Expected nil digest, got {result}"
        )
