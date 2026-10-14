#!/usr/bin/env python3
"""
EFA RDMA transfer test — exercises BLOB.RDMA_HELLO, BLOB.RDMA_GET, and
BLOB.RDMA_SET over real EFA hardware using fabric_target as the passive peer.

Run on the CLIENT instance:
    python3 efa_rdma_test.py --server-ip <SERVER_PRIVATE_IP> --server-fabric-addr <HEX>

The server's fabric address is logged at startup:
    grep "fabric service" /tmp/valkey.log
"""

import argparse
import os
import subprocess
import sys
import time

import valkey

FABRIC_TARGET = "/mnt/nvme0/cargo-target/release/fabric_target"
PATTERN = bytes(i % 256 for i in range(4096))

PASS = 0
FAIL = 0


def check(label, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        msg = f"  ✗ {label}"
        if detail:
            msg += f"  — {detail}"
        print(msg)


def start_target(server_fabric_addr, read_mode=False):
    """Launch fabric_target --efa and parse its advertisement."""
    cmd = [FABRIC_TARGET, "--efa", server_fabric_addr]
    if read_mode:
        cmd.insert(1, "--read")
    env = dict(os.environ, LD_LIBRARY_PATH="/opt/amazon/efa/lib64:" + os.environ.get("LD_LIBRARY_PATH", ""))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, env=env)
    line = proc.stdout.readline()
    if not line.startswith("advertisement:"):
        rest = proc.stdout.read()
        proc.kill()
        raise RuntimeError(f"fabric_target failed to advertise: {line}{rest}")
    parts = line.split()
    addr_hex = parts[1]
    rkey = int(parts[2])
    remote_addr = int(parts[3])
    length = int(parts[4])
    return proc, addr_hex, rkey, remote_addr, length


def test_rdma_get(c, server_fabric_addr):
    """BLOB.RDMA_GET: server writes object into client's EFA-registered buffer."""
    print("\n── RDMA GET test ──")
    # Store a test object
    c.execute_command('BLOB.TCP_SET', 'rdma_get_key', PATTERN)
    expected_crc = c.execute_command('BLOB.INFO', 'rdma_get_key', 'CRC')
    # Start passive peer
    proc, client_addr, rkey, remote_addr, length = start_target(server_fabric_addr)
    try:
        print(f"  fabric_target addr={client_addr[:16]}... rkey={rkey} len={length}")
        # Establish RDMA session (must be on this connection)
        hello_reply = c.execute_command('BLOB.RDMA_HELLO', client_addr)
        check("BLOB.RDMA_HELLO returns server addresses",
              isinstance(hello_reply, list) and len(hello_reply) > 0,
              f"got {hello_reply!r}")
        # RDMA GET — server writes into our registered buffer
        get_reply = c.execute_command('BLOB.RDMA_GET', 'rdma_get_key',
                                      str(rkey), str(remote_addr), str(length))
        check("BLOB.RDMA_GET returns [len, crc]",
              isinstance(get_reply, list) and len(get_reply) == 2,
              f"got {get_reply!r}")
        if isinstance(get_reply, list) and len(get_reply) == 2:
            check(f"  len={get_reply[0]}", get_reply[0] == 4096)
            check(f"  crc matches", get_reply[1] == expected_crc)
        # Wait for fabric_target to verify bytes landed
        time.sleep(2)
        proc.terminate()
        output = proc.communicate(timeout=10)[0]
        check("fabric_target verified payload", "payload verified" in output,
              output.strip().split('\n')[-1] if output else "no output")
    finally:
        proc.kill()
        proc.wait()
    c.execute_command('DEL', 'rdma_get_key')


def test_rdma_set(server_ip, server_port, server_fabric_addr):
    """BLOB.RDMA_SET: server reads from client's EFA-registered buffer."""
    print("\n── RDMA SET test ──")
    # Start passive peer in read mode (prefilled with pattern)
    proc, client_addr, rkey, remote_addr, length = start_target(
        server_fabric_addr, read_mode=True)
    try:
        print(f"  fabric_target addr={client_addr[:16]}... rkey={rkey} len={length}")
        # Fresh connection for a new HELLO
        c2 = valkey.Valkey(host=server_ip, port=server_port, decode_responses=False)
        hello_reply = c2.execute_command('BLOB.RDMA_HELLO', client_addr)
        check("BLOB.RDMA_HELLO (SET session)",
              isinstance(hello_reply, list) and len(hello_reply) > 0)
        # RDMA SET — server reads 4096 bytes from our buffer
        set_reply = c2.execute_command('BLOB.RDMA_SET', 'rdma_set_key',
                                        '4096', str(rkey), str(remote_addr), str(length))
        check("BLOB.RDMA_SET returns OK", set_reply == b'OK', f"got {set_reply!r}")
        # Verify via TCP readback
        tcp_data = c2.execute_command('BLOB.TCP_GET', 'rdma_set_key')
        check("TCP readback matches pattern", tcp_data == PATTERN,
              f"len={len(tcp_data) if tcp_data else 'None'}")
        c2.execute_command('DEL', 'rdma_set_key')
    finally:
        proc.kill()
        proc.wait()


def test_rdma_roundtrip(server_ip, server_port, server_fabric_addr):
    """Full roundtrip: SET via RDMA, GET via RDMA, verify bytes match."""
    print("\n── RDMA roundtrip test ──")
    # SET via RDMA
    proc_set, client_addr, rkey, remote_addr, length = start_target(
        server_fabric_addr, read_mode=True)
    try:
        c = valkey.Valkey(host=server_ip, port=server_port, decode_responses=False)
        c.execute_command('BLOB.RDMA_HELLO', client_addr)
        set_reply = c.execute_command('BLOB.RDMA_SET', 'rdma_rt_key', '4096',
                                       str(rkey), str(remote_addr), str(length))
        check("RDMA SET for roundtrip", set_reply == b'OK')
    finally:
        proc_set.kill()
        proc_set.wait()
    # GET via RDMA on a fresh connection
    proc_get, client_addr2, rkey2, remote_addr2, length2 = start_target(
        server_fabric_addr)
    try:
        c2 = valkey.Valkey(host=server_ip, port=server_port, decode_responses=False)
        c2.execute_command('BLOB.RDMA_HELLO', client_addr2)
        get_reply = c2.execute_command('BLOB.RDMA_GET', 'rdma_rt_key',
                                        str(rkey2), str(remote_addr2), str(length2))
        check("RDMA GET for roundtrip returns [4096, crc]",
              isinstance(get_reply, list) and get_reply[0] == 4096)
        time.sleep(2)
        proc_get.terminate()
        output = proc_get.communicate(timeout=10)[0]
        check("Roundtrip: fabric_target verified payload",
              "payload verified" in output,
              output.strip().split('\n')[-1] if output else "no output")
        c2.execute_command('DEL', 'rdma_rt_key')
    finally:
        proc_get.kill()
        proc_get.wait()


def test_rdma_steady_state(server_ip, server_port, server_fabric_addr):
    """Steady-state RDMA: repeated SET/GET cycles over EFA to stress the
    session lifecycle and transfer path under sustained load."""
    print("\n── RDMA steady-state test ──")
    rounds = 10
    errors = []
    for i in range(rounds):
        key = f"rdma_ss_{i}"
        # SET via RDMA: spin up a read-mode target, HELLO, SET, tear down.
        proc_set, addr_set, rkey_set, raddr_set, len_set = start_target(
            server_fabric_addr, read_mode=True)
        try:
            c = valkey.Valkey(host=server_ip, port=server_port,
                              decode_responses=False)
            c.execute_command('BLOB.RDMA_HELLO', addr_set)
            reply = c.execute_command('BLOB.RDMA_SET', key, '4096',
                                      str(rkey_set), str(raddr_set), str(len_set))
            if reply != b'OK':
                errors.append(f"round {i} SET: got {reply!r}")
                continue
        except Exception as e:
            errors.append(f"round {i} SET: {e}")
            continue
        finally:
            proc_set.kill()
            proc_set.wait()
        # GET via RDMA: spin up a write-mode target, HELLO, GET, verify.
        proc_get, addr_get, rkey_get, raddr_get, len_get = start_target(
            server_fabric_addr)
        try:
            c2 = valkey.Valkey(host=server_ip, port=server_port,
                               decode_responses=False)
            c2.execute_command('BLOB.RDMA_HELLO', addr_get)
            get_reply = c2.execute_command('BLOB.RDMA_GET', key,
                                           str(rkey_get), str(raddr_get), str(len_get))
            if not isinstance(get_reply, list) or get_reply[0] != 4096:
                errors.append(f"round {i} GET: unexpected reply {get_reply!r}")
            else:
                # Wait for fabric_target to verify bytes
                time.sleep(1)
                proc_get.terminate()
                output = proc_get.communicate(timeout=10)[0]
                if "payload verified" not in output:
                    errors.append(f"round {i} GET: target did not verify")
        except Exception as e:
            errors.append(f"round {i} GET: {e}")
        finally:
            proc_get.kill()
            proc_get.wait()
        # Clean up key via TCP
        cleanup = valkey.Valkey(host=server_ip, port=server_port,
                                decode_responses=False)
        cleanup.execute_command('DEL', key)
    check(f"{rounds} RDMA SET/GET rounds completed",
          len(errors) == 0,
          "; ".join(errors[:5]) if errors else "")


def main():
    parser = argparse.ArgumentParser(description="EFA RDMA transfer test")
    parser.add_argument('--server-ip', required=True, help='Valkey server private IP')
    parser.add_argument('--server-port', type=int, default=6379)
    parser.add_argument('--server-fabric-addr', required=True,
                        help='Server fabric address hex (from server log)')
    args = parser.parse_args()

    print(f"EFA RDMA Transfer Test")
    print(f"  Server: {args.server_ip}:{args.server_port}")
    print(f"  Fabric: {args.server_fabric_addr[:16]}...")

    c = valkey.Valkey(host=args.server_ip, port=args.server_port,
                      decode_responses=False)
    c.ping()
    c.execute_command('CONFIG', 'SET', 'save', '')

    test_rdma_get(c, args.server_fabric_addr)
    test_rdma_set(args.server_ip, args.server_port, args.server_fabric_addr)
    test_rdma_roundtrip(args.server_ip, args.server_port, args.server_fabric_addr)
    test_rdma_steady_state(args.server_ip, args.server_port, args.server_fabric_addr)

    print(f"\n{'═' * 50}")
    print(f"  EFA RDMA: {PASS} passed, {FAIL} failed")
    print(f"{'═' * 50}")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == '__main__':
    main()
