"""Integration tests for read coalescing (per-key singleflight).

Tests validate that concurrent LO.GET requests for the same key return correct
data. Because Valkey's command dispatch is single-threaded, true concurrency
at the io_uring level requires pipelining — multiple commands dispatched before
any completions are processed. We use pipeline mode to issue concurrent reads.

The coalescing logic is internal to the storage layer and transparent to clients.
These tests verify external correctness (data integrity, no corruption under
concurrent access, distinct keys remain independent).
"""

import os
import threading
import concurrent.futures
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestReadCoalescing(ValkeyLargeObjTestCaseBase):

    def test_concurrent_same_key_reads_pipeline(self):
        """Multiple pipelined LO.GET for the same key all return correct data.

        Pipeline dispatches N reads before any responses are consumed.
        If coalescing is active, the leader performs 1 NVMe read and all N
        consumers receive the same data via SharedBuffer (Arc clone).
        """
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('LO.SET', 'coalkey', '4096', payload)

        # Pipeline 50 concurrent reads for the same key.
        pipe = client.pipeline(transaction=False)
        num_reads = 50
        for _ in range(num_reads):
            pipe.execute_command('LO.GET', 'coalkey')

        results = pipe.execute()

        assert len(results) == num_reads
        for i, data in enumerate(results):
            assert data == payload, (
                f"Read {i}: expected {len(payload)} bytes of 'C', "
                f"got {len(data) if data else 'None'} bytes"
            )

    def test_concurrent_same_key_data_integrity(self):
        """Concurrent reads return byte-for-byte correct data (no corruption).

        Uses a randomized payload to detect any partial reads or memcpy errors.
        """
        client = self.server.get_new_client()
        # Use a non-repeating pattern so corruption is detectable.
        payload = bytes(range(256)) * 16  # 4096 bytes, non-uniform
        client.execute_command('LO.SET', 'intkey', '4096', payload)

        pipe = client.pipeline(transaction=False)
        num_reads = 100
        for _ in range(num_reads):
            pipe.execute_command('LO.GET', 'intkey')

        results = pipe.execute()

        assert len(results) == num_reads
        for i, data in enumerate(results):
            assert data == payload, (
                f"Read {i}: data integrity check failed "
                f"(got {len(data) if data else 'None'} bytes)"
            )

    def test_distinct_keys_no_false_coalescing(self):
        """Reads of different keys are independent — no cross-key contamination.

        Sets up multiple keys with distinct payloads, then reads them
        concurrently via pipeline to verify each returns its own data.
        """
        client = self.server.get_new_client()
        num_keys = 20
        payloads = {}

        for i in range(num_keys):
            key = f'distkey:{i}'
            payload = bytes([i % 256]) * 4096
            payloads[key] = payload
            client.execute_command('LO.SET', key, '4096', payload)

        # Pipeline reads for all distinct keys.
        pipe = client.pipeline(transaction=False)
        keys_order = list(payloads.keys())
        for key in keys_order:
            pipe.execute_command('LO.GET', key)

        results = pipe.execute()

        assert len(results) == num_keys
        for i, key in enumerate(keys_order):
            assert results[i] == payloads[key], (
                f"Key '{key}': expected payload[0]={payloads[key][0]}, "
                f"got {results[i][0] if results[i] else 'None'}"
            )

    def test_concurrent_same_key_multi_client(self):
        """Multiple client connections reading the same key concurrently.

        Each client connection sends LO.GET independently. The server's
        coalescing logic deduplicates NVMe reads across client connections.
        """
        client = self.server.get_new_client()
        payload = b'M' * 4096
        client.execute_command('LO.SET', 'multikey', '4096', payload)

        num_clients = 10
        reads_per_client = 10
        errors = []

        def reader(thread_id):
            try:
                c = self.server.get_new_client()
                for i in range(reads_per_client):
                    data = c.execute_command('LO.GET', 'multikey')
                    if data != payload:
                        errors.append(
                            f"Thread {thread_id} iter {i}: "
                            f"got {len(data) if data else 'None'} bytes"
                        )
            except Exception as e:
                errors.append(f"Thread {thread_id}: exception {e}")

        threads = []
        for t in range(num_clients):
            th = threading.Thread(target=reader, args=(t,))
            threads.append(th)
            th.start()

        for th in threads:
            th.join()

        assert len(errors) == 0, f"Errors: {errors[:5]}"

    def test_read_after_overwrite(self):
        """After LO.SET overwrites a key, LO.GET returns the new data.

        Validates that coalescing keyed by ObjectId does not serve stale data
        after a key is re-written (new ObjectId assigned).
        """
        client = self.server.get_new_client()
        payload_v1 = b'V' * 4096
        payload_v2 = b'W' * 4096

        client.execute_command('LO.SET', 'overkey', '4096', payload_v1)
        data = client.execute_command('LO.GET', 'overkey')
        assert data == payload_v1

        # Overwrite with new value (creates new ObjectId internally).
        client.execute_command('LO.SET', 'overkey', '4096', payload_v2)
        data = client.execute_command('LO.GET', 'overkey')
        assert data == payload_v2

        # Pipeline reads after overwrite — all should see new data.
        pipe = client.pipeline(transaction=False)
        for _ in range(20):
            pipe.execute_command('LO.GET', 'overkey')
        results = pipe.execute()
        for i, d in enumerate(results):
            assert d == payload_v2, f"Read {i} after overwrite: got stale data"

    def test_interleaved_reads_different_keys(self):
        """Interleaved reads of two keys via pipeline — each returns its own data.

        Validates that coalescing correctly separates entries by ObjectId
        when multiple keys are read in an interleaved pattern.
        """
        client = self.server.get_new_client()
        payload_a = b'A' * 4096
        payload_b = b'B' * 4096

        client.execute_command('LO.SET', 'ilkey:a', '4096', payload_a)
        client.execute_command('LO.SET', 'ilkey:b', '4096', payload_b)

        # Interleave reads: A, B, A, B, ...
        pipe = client.pipeline(transaction=False)
        pattern = []
        for _ in range(25):
            pipe.execute_command('LO.GET', 'ilkey:a')
            pattern.append('a')
            pipe.execute_command('LO.GET', 'ilkey:b')
            pattern.append('b')

        results = pipe.execute()

        assert len(results) == 50
        for i, key_id in enumerate(pattern):
            expected = payload_a if key_id == 'a' else payload_b
            assert results[i] == expected, (
                f"Position {i} (key {key_id}): expected '{key_id.upper()}' bytes, "
                f"got first byte {results[i][0] if results[i] else 'None'}"
            )
