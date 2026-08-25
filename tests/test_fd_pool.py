import os

import pytest
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestFdPoolReadPath(ValkeyLargeObjTestCaseBase):
    """Integration tests for the fd-pool-backed LO.GET read path.

    Covers the flow in StorageEngine::read_into: cache miss opens+inserts an fd,
    subsequent reads hit the cache, and delete drops the fd. Uses the default
    (auto-sized) fd-pool capacity from the base setup.
    """

    def test_fd_pool_reuse_repeated_get(self):
        """Repeated LO.GET on one key reuses the cached fd and always returns bytes.

        First GET is a cache miss (open + insert); the rest are cache hits. All must
        return identical, correct data.
        """
        client = self.server.get_new_client()
        payload = b'R' * 4096
        assert client.execute_command('LO.SET', 'reusekey', '4096', payload) == b'OK'
        for _ in range(50):
            data = client.execute_command('LO.GET', 'reusekey')
            assert data == payload

    def test_fd_pool_many_keys_interleaved_get(self):
        """Many distinct objects read correctly when GETs interleave across keys.

        Exercises the fd pool across many entries with mixed access order.
        """
        client = self.server.get_new_client()
        keys = {}
        for i in range(30):
            key = f'multi:{i}'
            payload = bytes([i % 256]) * 4096
            assert client.execute_command('LO.SET', key, '4096', payload) == b'OK'
            keys[key] = payload
        # Read every key twice in interleaved order.
        for _ in range(2):
            for key, payload in keys.items():
                assert client.execute_command('LO.GET', key) == payload

    def test_delete_then_reuse_slot(self):
        """DEL drops the fd; the key reads nil afterwards, and new keys still work.

        Covers StorageEngine::delete -> FdPool::remove, and that a fresh object read
        succeeds after a prior object was removed.
        """
        client = self.server.get_new_client()
        payload = b'D' * 4096
        client.execute_command('LO.SET', 'delkey', '4096', payload)
        assert client.execute_command('LO.GET', 'delkey') == payload
        assert client.execute_command('DEL', 'delkey') == 1
        assert client.execute_command('LO.GET', 'delkey') is None
        # A new object still reads back correctly after the delete.
        payload2 = b'E' * 4096
        client.execute_command('LO.SET', 'newkey', '4096', payload2)
        assert client.execute_command('LO.GET', 'newkey') == payload2


class TestFdPoolEviction(ValkeyLargeObjTestCaseBase):
    """Correctness of the LO.GET read path when the fd pool is forced to evict.

    Starts the server with a tiny `fd-pool-size` so reads across more objects than
    the cap must evict idle fds and reopen them on the next access — results must
    stay correct throughout.
    """

    FD_POOL_SIZE = 4

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        module_path = os.getenv('MODULE_PATH')
        data_dir = os.path.abspath(self.testdir)
        direct_io = "no" if os.environ.get("ASAN_BUILD") else "yes"
        args = {
            'enable-debug-command': 'yes',
            'loadmodule': (
                f"{module_path} data-dir {data_dir} pool-buf-size 4096 "
                f"pool-buf-count 128 bench-mode no direct-io {direct_io} "
                f"fd-pool-size {self.FD_POOL_SIZE}"
            ),
        }
        server_path = (
            f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/"
            f"{os.environ['SERVER_VERSION']}/valkey-server"
        )
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=server_path,
            args=args,
        )
        self.data_dir = data_dir

    def test_reads_correct_under_eviction(self):
        """Read many more objects than the fd-pool cap, repeatedly, in mixed order.

        Every read must return the right bytes even though the pool (cap=4) is
        constantly evicting and reopening fds.
        """
        client = self.server.get_new_client()
        num_keys = self.FD_POOL_SIZE * 8  # well beyond the cache capacity
        keys = {}
        for i in range(num_keys):
            key = f'evict:{i}'
            payload = bytes([(i * 7) % 256]) * 4096
            assert client.execute_command('LO.SET', key, '4096', payload) == b'OK'
            keys[key] = payload
        # Several passes so cold entries get evicted and reopened repeatedly.
        for _ in range(3):
            for key, payload in keys.items():
                assert client.execute_command('LO.GET', key) == payload
