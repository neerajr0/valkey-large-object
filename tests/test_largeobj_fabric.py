import binascii
import collections
import crc32c
import os
import subprocess
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1. The server only records it until
# the first transfer, so any well-formed address will do.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()


class TestLargeObjFabric(ValkeyLargeObjTestCaseBase):
    """LO.HELLO against real fabric services over the tcp provider on loopback."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_hello_returns_one_address_per_server(self):
        """One server was pinned to lo, so HELLO returns exactly one address, and it decodes."""
        client = self.server.get_new_client()
        reply = client.execute_command('LO.HELLO', PEER_ADDRESS)
        assert isinstance(reply, list) and len(reply) == 1
        address = binascii.unhexlify(reply[0])
        assert 0 < len(address)

    def test_hello_is_once_per_connection(self):
        """A second HELLO on the same connection is refused; a new connection may HELLO again."""
        client = self.server.get_new_client()
        first = client.execute_command('LO.HELLO', PEER_ADDRESS)
        self.verify_error_response(
            client, f'LO.HELLO {PEER_ADDRESS}',
            'DMA session already established (one LO.HELLO per connection)')
        assert self.server.get_new_client().execute_command('LO.HELLO', PEER_ADDRESS) == first

    def test_hello_rejects_bad_hex(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, 'LO.HELLO zz', 'invalid peer address hex')
        try:
            client.execute_command('LO.HELLO', '')
            assert False, "Expected an error for an empty address"
        except ResponseError as e:
            assert str(e) == 'peer address must not be empty'

    def test_efa_get_needs_hello(self):
        """Both the legacy and multi-region EFA arities of LO.GET are refused until this
        client has a session."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'key', b'A' * 4096)
        # Multi-region form.
        self.verify_error_response(
            client, 'LO.GET key 1 999 0 4096', 'no DMA session (call LO.HELLO first)')
        # Legacy form.
        self.verify_error_response(
            client, 'LO.GET key 999 0', 'no DMA session (call LO.HELLO first)')

    def test_arity_gaps_are_refused(self):
        """Arg counts that are neither TCP, legacy EFA, nor multi-region EFA are rejected.

        GET: 2=TCP, 4=legacy EFA, >=5=multi-region EFA. So 3 args is invalid.
        SET: 3=TCP, 5=legacy EFA, >=6=multi-region EFA. So 4 args is invalid.
        A well-formed region list with trailing args past the declared count is also
        refused, rather than transferring against a region set the client did not send."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'key', b'A' * 4096)
        for command in ('LO.GET key 999',
                        'LO.SET key 4096 999',
                        # Trailing arg past a well-formed single-region list.
                        'LO.GET key 1 999 0 4096 7'):
            name = command.split()[0]
            self.verify_error_response(
                client, command, f"wrong number of arguments for '{name}' command")


class TestLargeObjFabricUnavailable(ValkeyLargeObjTestCaseBase):
    """A domain that doesn't exist leaves the module TCP-only rather than refusing to load."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces no-such-interface"
        )

    def test_hello_reports_unavailable(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, f'LO.HELLO {PEER_ADDRESS}', 'EFA unavailable on this instance')
        assert client.execute_command('LO.SET', 'key', b'A' * 4096) == b'OK'

# What tests/harness/fabric_target waits for (write) or serves (--read): buffers of this byte
# totalling TARGET_LEN, in one region by default or several under --split.
PATTERN = b'\xab'
TARGET_LEN = 4096

# One advertised client memory region: the fabric address to HELLO with, plus the
# (rkey, addr, len) triple that LO.GET / LO.SET carries per region.
Region = collections.namedtuple('Region', 'address rkey addr len')


def region_args(regions):
    """The regions as the EFA commands carry them: n_regions, then a triple each.

    Order is load-bearing — the object's bytes are laid across the regions in this
    order, so it must match the order the target registered them."""
    args = [len(regions)]
    for region in regions:
        args += [region.rkey, region.addr, region.len]
    return args


class TestLargeObjFabricTransfer(ValkeyLargeObjTestCaseBase):
    """Bytes actually move: tests/harness/fabric_target, a passive libfabric peer on tcp loopback, is
    the client's buffer. Its advertisement is what a real client would carry into LO.HELLO and the
    per-request rkey / remote address."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" lo-buffer-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags, split=None):
        """Launch the passive peer and return it with the regions it advertised.

        `split` is a list of region sizes totalling TARGET_LEN; the target then registers
        one separate buffer per size, each with its own rkey. Omitted, it registers the
        single whole-buffer region."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        command = [target, '127.0.0.1', *flags]
        if split is not None:
            command.append('--split=' + ','.join(str(size) for size in split))
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        regions = []
        for _ in range(1 if split is None else len(split)):
            line = process.stdout.readline()
            assert line.startswith('advertisement: '), line
            address, rkey, remote_addr, length = line.split()[1:]
            regions.append(Region(address, int(rkey), int(remote_addr), int(length)))
        return process, regions

    def test_get_writes_into_the_target(self):
        process, regions = self.start_target()
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.SET', 'key', PATTERN * TARGET_LEN)
            client.execute_command('LO.HELLO', regions[0].address)
            reply = client.execute_command('LO.GET', 'key', *region_args(regions))
            assert reply == [TARGET_LEN, crc32c.crc32c(PATTERN * TARGET_LEN)]
            # The target exits once every byte of the pattern has landed.
            output = process.communicate(timeout=30)[0]
            assert 'payload verified' in output, output
        finally:
            process.kill()

    def test_set_reads_from_the_target(self):
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', regions[0].address)
            assert client.execute_command(
                'LO.SET', 'key', TARGET_LEN, *region_args(regions)) == b'OK'
            assert client.execute_command('LO.GET', 'key') == PATTERN * TARGET_LEN
        finally:
            process.kill()

    def test_multi_transfer_mixed_protocol(self):
        """Do mixed sets and gets, read the server value into the client, set it back, and
        read it back in the test"""
        payload = b'\x5a' * TARGET_LEN
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.SET', 'key', payload)
            client.execute_command('LO.HELLO', regions[0].address)
            reply = client.execute_command('LO.GET', 'key', *region_args(regions))
            assert reply == [TARGET_LEN, crc32c.crc32c(payload)]
            assert client.execute_command(
                'LO.SET', 'copy', TARGET_LEN, *region_args(regions)) == b'OK'
            assert client.execute_command('LO.GET', 'copy') == payload
        finally:
            process.kill()

    def test_legacy_single_region_get(self):
        """LO.GET with the legacy 2-arg EFA syntax: key rkey addr (no len, no n_regions).

        The server synthesizes a single region with obj_len as the length. Verifies
        that the object lands in the target and the reply is [obj_len, crc32c]."""
        process, regions = self.start_target()
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.SET', 'key', PATTERN * TARGET_LEN)
            client.execute_command('LO.HELLO', regions[0].address)
            r = regions[0]
            reply = client.execute_command('LO.GET', 'key', r.rkey, r.addr)
            assert reply == [TARGET_LEN, crc32c.crc32c(PATTERN * TARGET_LEN)]
            output = process.communicate(timeout=30)[0]
            assert 'payload verified' in output, output
        finally:
            process.kill()

    def test_legacy_single_region_set(self):
        """LO.SET with the legacy 3-arg EFA syntax: key total_len rkey addr (no n_regions).

        The server synthesizes a single region with total_len as the length. Verifies
        the object is persisted and readable back over TCP."""
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', regions[0].address)
            r = regions[0]
            assert client.execute_command(
                'LO.SET', 'key', TARGET_LEN, r.rkey, r.addr) == b'OK'
            assert client.execute_command('LO.GET', 'key') == PATTERN * TARGET_LEN
        finally:
            process.kill()

    def test_multi_region_transfer(self):
        """GET and SET across several separate client regions.

        Covers equal and unequal splits, and a boundary that falls mid-chunk: 1024 is not
        a multiple of chunk-size 4096, so the first chunk must be scattered across both
        regions. The target reports 'payload verified' only once EVERY region has filled,
        so a transfer that wrote the head and dropped the tail fails here."""
        payload = PATTERN * TARGET_LEN
        for sizes in ([2048, 2048], [1024, 3072]):
            process, regions = self.start_target(split=sizes)
            try:
                assert [region.len for region in regions] == sizes
                # Separate registrations, so distinct keys — the multi-rkey path.
                assert regions[0].rkey != regions[1].rkey
                client = self.server.get_new_client()
                client.execute_command('LO.SET', 'key', payload)
                client.execute_command('LO.HELLO', regions[0].address)
                reply = client.execute_command('LO.GET', 'key', *region_args(regions))
                assert reply == [TARGET_LEN, crc32c.crc32c(payload)]
                output = process.communicate(timeout=30)[0]
                assert 'payload verified' in output, output
            finally:
                process.kill()
        # SET gathers the object back out of unequal regions, verified byte for byte.
        process, regions = self.start_target('--read', split=[1024, 3072])
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', regions[0].address)
            assert client.execute_command(
                'LO.SET', 'copy', TARGET_LEN, *region_args(regions)) == b'OK'
            assert client.execute_command('LO.GET', 'copy') == payload
        finally:
            process.kill()

    def test_region_coverage_is_validated(self):
        """Regions must cover the object, and may exceed it.

        Validation is sum(len_i) >= obj_len, so surplus space is accepted and the reply's
        obj_len is how the client knows where the object ends. A shortfall is refused
        before any transfer is issued."""
        short_len = 2048
        process, regions = self.start_target(split=[1024, 3072])
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', regions[0].address)
            # 4096 bytes of advertised space for a 2048-byte object.
            client.execute_command('LO.SET', 'short', PATTERN * short_len)
            assert client.execute_command('LO.GET', 'short', *region_args(regions)) == [
                short_len, crc32c.crc32c(PATTERN * short_len)]
            # The 1024-byte region alone cannot hold a 4096-byte object.
            client.execute_command('LO.SET', 'key', PATTERN * TARGET_LEN)
            first = f'{regions[0].rkey} {regions[0].addr} {regions[0].len}'
            self.verify_error_response(
                client, f'LO.GET key 1 {first}',
                'client address space smaller than object length')
            self.verify_error_response(
                client, f'LO.SET key {TARGET_LEN} 1 {first}',
                'client address space smaller than object length')
        finally:
            process.kill()


class TestLargeObjFabricTieredTransfer(TestLargeObjFabricTransfer):
    """Tiered mode with promotion off to run the NVMe paths."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" chunk-size 4096"
            f" max-promote-size 0"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_set_over_efa_persists_to_nvme(self):
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', regions[0].address)
            assert client.execute_command(
                'LO.SET', 'key', TARGET_LEN, *region_args(regions)) == b'OK'
            assert len(self._object_files()) == 1
        finally:
            process.kill()


class TestLargeObjFabricTieredPromotedTransfer(TestLargeObjFabricTransfer):
    """Tiered mode with promotion on."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" chunk-size 4096"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_get_over_efa_cold_then_warm_on_one_session(self):
        payload = PATTERN * TARGET_LEN
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', regions[0].address)
            assert client.execute_command(
                'LO.SET', 'key', TARGET_LEN, *region_args(regions)) == b'OK'
            # Cold load into dram
            assert client.execute_command(
                'LO.GET', 'key', *region_args(regions)) == [TARGET_LEN, crc32c.crc32c(payload)]
            # Hot load from dram
            assert client.execute_command(
                'LO.GET', 'key', *region_args(regions)) == [TARGET_LEN, crc32c.crc32c(payload)]
            # Read back from client and verify literal bytes
            assert client.execute_command(
                'LO.SET', 'copy', TARGET_LEN, *region_args(regions)) == b'OK'
            assert client.execute_command('LO.GET', 'copy') == payload
        finally:
            process.kill()
