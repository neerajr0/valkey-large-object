#!/usr/bin/env python3
"""
valkey-large-object module — functional test script.

Exercises every client-facing command and behavior of the largeobj module
over the TCP path. Intended for teams to validate a fresh deployment.

Usage:
    python3 instance_test_client.py [--host HOST] [--port PORT]

Requires: pip3 install valkey
"""

import argparse
import hashlib
import os
import sys
import time

import valkey


# ─── Helpers ──────────────────────────────────────────────────────────────────

PASS = 0
FAIL = 0

# Hash tag prefix for cluster mode — ensures multi-key ops hash to the same slot
KEY_PREFIX = ""  # Set to "{t}" for cluster mode


def key(name):
    """Return key name with cluster-mode prefix if enabled."""
    return f"{KEY_PREFIX}{name}"


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


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def make_payload(size, fill=b'A'):
    """Create a deterministic payload of `size` bytes."""
    return (fill * ((size // len(fill)) + 1))[:size]


# ─── Tests ────────────────────────────────────────────────────────────────────

def test_basic_set_get(c):
    """BLOB.TCP_SET + BLOB.TCP_GET roundtrip at various sizes."""
    section("1. Basic SET/GET roundtrip")
    sizes = [
        ("1 byte",      1),
        ("4 KB",        4096),
        ("4 KB + 1",    4097),    # partial last chunk
        ("8 KB",        8192),    # exact 2-chunk
        ("64 KB",       65536),
        ("1 MB",        1024 * 1024),
        ("10 MB",       10 * 1024 * 1024),
    ]
    for label, size in sizes:
        payload = make_payload(size, fill=bytes([size % 256 or 0x42]))
        k = key(f"basic_{size}")
        result = c.execute_command('BLOB.TCP_SET', k, payload)
        check(f"SET {label}", result == b'OK', f"got {result!r}")
        data = c.execute_command('BLOB.TCP_GET', k)
        check(f"GET {label} roundtrip", data == payload,
              f"len={len(data) if data else 'None'}, expected={size}")
        c.execute_command('DEL', k)


def test_get_nonexistent(c):
    """GET on a nonexistent key returns nil."""
    section("2. GET nonexistent key")
    result = c.execute_command('BLOB.TCP_GET', key('no_such_key'))
    check("GET nonexistent → nil", result is None, f"got {result!r}")


def test_overwrite(c):
    """SET same key twice replaces the value."""
    section("3. Overwrite semantics")
    c.execute_command('BLOB.TCP_SET', key('ow_key'), make_payload(4096, b'X'))
    c.execute_command('BLOB.TCP_SET', key('ow_key'), make_payload(8192, b'Y'))
    data = c.execute_command('BLOB.TCP_GET', key('ow_key'))
    check("Overwrite returns new value", data == make_payload(8192, b'Y'),
          f"len={len(data) if data else 'None'}")
    c.execute_command('DEL', key('ow_key'))


def test_delete(c):
    """DEL removes the key; subsequent GET returns nil."""
    section("4. DEL removes key")
    c.execute_command('BLOB.TCP_SET', key('del_key'), make_payload(4096))
    c.execute_command('DEL', key('del_key'))
    result = c.execute_command('BLOB.TCP_GET', key('del_key'))
    check("GET after DEL → nil", result is None, f"got {result!r}")


def test_blob_info(c):
    """BLOB.INFO returns len, crc, and tier metadata."""
    section("5. BLOB.INFO metadata")
    payload = bytes(range(256)) * 16  # 4096 bytes, non-uniform
    c.execute_command('BLOB.TCP_SET', key('info_key'), payload)
    # Full info
    info = c.execute_command('BLOB.INFO', key('info_key'))
    check("BLOB.INFO returns array", isinstance(info, list) and len(info) == 6,
          f"got {info!r}")
    if isinstance(info, list) and len(info) == 6:
        check("  len field",  info[0] == b'len' and info[1] == len(payload))
        check("  crc field",  info[2] == b'crc' and isinstance(info[3], int))
        check("  tier field", info[4] == b'tier' and info[5] in (b'dram', b'nvme'))
    # Individual fields
    length = c.execute_command('BLOB.INFO', key('info_key'), 'LEN')
    check("BLOB.INFO LEN", length == len(payload), f"got {length}")
    crc = c.execute_command('BLOB.INFO', key('info_key'), 'CRC')
    check("BLOB.INFO CRC is u32", isinstance(crc, int) and 0 <= crc <= 0xFFFFFFFF,
          f"got {crc!r}")
    # CRC is stable
    crc2 = c.execute_command('BLOB.INFO', key('info_key'), 'CRC')
    check("CRC is deterministic", crc == crc2)
    tier = c.execute_command('BLOB.INFO', key('info_key'), 'TIER')
    check("BLOB.INFO TIER", tier in (b'dram', b'nvme'), f"got {tier!r}")
    c.execute_command('DEL', key('info_key'))


def test_info_errors(c):
    """BLOB.INFO error responses."""
    section("6. BLOB.INFO error handling")
    # Nonexistent key
    try:
        c.execute_command('BLOB.INFO', key('no_key'))
        check("BLOB.INFO nonexistent key → error", False, "no error raised")
    except valkey.ResponseError as e:
        check("BLOB.INFO nonexistent key → error", 'not found' in str(e).lower(),
              str(e))
    # Wrong type
    c.execute_command('SET', key('str_key'), 'hello')
    try:
        c.execute_command('BLOB.INFO', key('str_key'))
        check("BLOB.INFO wrong type → WRONGTYPE", False, "no error raised")
    except valkey.ResponseError as e:
        check("BLOB.INFO wrong type → WRONGTYPE", str(e).startswith('WRONGTYPE'),
              str(e))
    c.execute_command('DEL', key('str_key'))
    # Invalid field
    c.execute_command('BLOB.TCP_SET', key('inf_key'), make_payload(4096))
    try:
        c.execute_command('BLOB.INFO', key('inf_key'), 'BADFIELD')
        check("BLOB.INFO bad field → error", False, "no error raised")
    except valkey.ResponseError as e:
        check("BLOB.INFO bad field → error", 'invalid' in str(e).lower(), str(e))
    c.execute_command('DEL', key('inf_key'))


def test_wrong_type_errors(c):
    """Module commands reject keys of a different type with WRONGTYPE."""
    section("7. WRONGTYPE on non-largeobj keys")
    c.execute_command('SET', key('native_str'), 'hello')
    for cmd_args in [
        ('BLOB.TCP_GET', key('native_str')),
        ('BLOB.TCP_SET', key('native_str'), b'data'),
    ]:
        try:
            c.execute_command(*cmd_args)
            check(f"{cmd_args[0]} on string → WRONGTYPE", False, "no error")
        except valkey.ResponseError as e:
            check(f"{cmd_args[0]} on string → WRONGTYPE",
                  str(e).startswith('WRONGTYPE'), str(e))
    c.execute_command('DEL', key('native_str'))


def test_zero_length_rejected(c):
    """BLOB.TCP_SET with empty payload is rejected."""
    section("8. Zero-length object rejected")
    try:
        c.execute_command('BLOB.TCP_SET', key('empty_key'), b'')
        check("Zero-length SET rejected", False, "no error raised")
    except valkey.ResponseError as e:
        check("Zero-length SET rejected", 'length must be > 0' in str(e).lower(),
              str(e))


def test_max_object_size(c):
    """max-object-size enforcement via CONFIG SET."""
    section("9. max-object-size enforcement")
    # Save current, then lower the limit
    orig = c.execute_command('CONFIG', 'GET', 'largeobj.max-object-size')
    orig_val = orig[1] if orig else b'536870912'
    limit = 8192
    c.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit))
    # Oversized SET rejected
    try:
        c.execute_command('BLOB.TCP_SET', key('big_key'), make_payload(limit + 1))
        check("Oversized SET rejected", False, "no error")
    except valkey.ResponseError as e:
        check("Oversized SET rejected", 'max object size' in str(e).lower(), str(e))
    # At-limit SET succeeds
    result = c.execute_command('BLOB.TCP_SET', key('limit_key'), make_payload(limit))
    check("At-limit SET succeeds", result == b'OK')
    # GET still works for existing objects after lowering limit
    c.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit // 2))
    data = c.execute_command('BLOB.TCP_GET', key('limit_key'))
    check("GET unaffected after lowering limit", data == make_payload(limit))
    # Restore
    c.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', orig_val.decode())
    c.execute_command('DEL', key('limit_key'))


def test_copy(c):
    """COPY creates an independent duplicate."""
    section("10. COPY semantics")
    payload = make_payload(4096, b'C')
    c.execute_command('BLOB.TCP_SET', key('cp_src'), payload)
    result = c.execute_command('COPY', key('cp_src'), key('cp_dst'))
    check("COPY returns 1", result in (1, True), f"got {result!r}")
    check("Source intact after COPY",
          c.execute_command('BLOB.TCP_GET', key('cp_src')) == payload)
    check("Destination has same data",
          c.execute_command('BLOB.TCP_GET', key('cp_dst')) == payload)
    # CRC matches
    src_crc = c.execute_command('BLOB.INFO', key('cp_src'), 'CRC')
    dst_crc = c.execute_command('BLOB.INFO', key('cp_dst'), 'CRC')
    check("CRC matches between source and copy", src_crc == dst_crc)
    # Delete source doesn't affect copy
    c.execute_command('DEL', key('cp_src'))
    check("Copy survives source deletion",
          c.execute_command('BLOB.TCP_GET', key('cp_dst')) == payload)
    c.execute_command('DEL', key('cp_dst'))


def test_expiry(c):
    """TTL-based expiry removes the key."""
    section("11. TTL expiry")
    c.execute_command('BLOB.TCP_SET', key('exp_key'), make_payload(4096))
    c.execute_command('PEXPIRE', key('exp_key'), 200)
    time.sleep(0.5)
    # Polling EXISTS drives passive expiry
    exists = c.execute_command('EXISTS', key('exp_key'))
    check("Key expired after TTL", exists == 0, f"EXISTS={exists}")


def test_flushall(c):
    """FLUSHALL removes all module keys."""
    section("12. FLUSHALL cleanup")
    for i in range(5):
        c.execute_command('BLOB.TCP_SET', key(f'flush_{i}'), make_payload(4096))
    check("5 keys stored", c.execute_command('DBSIZE') >= 5)
    c.execute_command('FLUSHALL')
    check("DBSIZE=0 after FLUSHALL", c.execute_command('DBSIZE') == 0)


def test_tiered_promotion(c):
    """In Tiered mode, first GET(s) promote from NVMe to DRAM."""
    section("13. Tiered mode: NVMe → DRAM promotion")
    payload = make_payload(4096, b'T')
    c.execute_command('BLOB.TCP_SET', key('tier_key'), payload)
    tier_after_set = c.execute_command('BLOB.INFO', key('tier_key'), 'TIER')
    check("Tier after SET is 'nvme'", tier_after_set == b'nvme',
          f"got {tier_after_set!r}")
    # GET triggers promotion (promote-min-hits may require >1 GET)
    for _ in range(3):
        data = c.execute_command('BLOB.TCP_GET', key('tier_key'))
        check("GET returns correct data", data == payload) if _ == 0 else None
    tier_after_get = c.execute_command('BLOB.INFO', key('tier_key'), 'TIER')
    check("Tier after GET(s) is 'dram'", tier_after_get == b'dram',
          f"got {tier_after_get!r}")
    c.execute_command('DEL', key('tier_key'))


def test_multiple_objects(c):
    """Multiple objects coexist and are independently retrievable."""
    section("14. Multiple concurrent objects")
    count = 20
    for i in range(count):
        c.execute_command('BLOB.TCP_SET', key(f'multi_{i}'),
                          make_payload(4096, bytes([i % 256 or 0x42])))
    all_ok = True
    for i in range(count):
        data = c.execute_command('BLOB.TCP_GET', key(f'multi_{i}'))
        expected = make_payload(4096, bytes([i % 256 or 0x42]))
        if data != expected:
            all_ok = False
            print(f"    Key multi_{i} mismatch: len={len(data) if data else 'None'}")
    check(f"All {count} objects round-trip correctly", all_ok)
    for i in range(count):
        c.execute_command('DEL', key(f'multi_{i}'))


def test_info_section(c):
    """INFO largeobj section is populated."""
    section("15. INFO largeobj section")
    info = c.execute_command('INFO', 'largeobj')
    check("INFO largeobj returns data", info is not None and len(info) > 0)
    if isinstance(info, dict):
        keys = [k.decode() if isinstance(k, bytes) else k for k in info.keys()]
        check("  has largeobj_num_objects",
              any('num_objects' in k for k in keys), f"keys: {keys[:5]}...")
        check("  has largeobj_dram_segments",
              any('dram_segments' in k for k in keys))


def test_acl_category(c):
    """Module commands are listed under the @largeobj ACL category."""
    section("16. ACL category registration")
    # Use a raw connection to avoid the valkey-py COMMAND LIST parser bug.
    # Instead, verify via COMMAND INFO on each expected command.
    expected = ['BLOB.TCP_GET', 'BLOB.TCP_SET', 'BLOB.RDMA_HELLO',
                'BLOB.RDMA_GET', 'BLOB.RDMA_SET', 'BLOB.INFO']
    all_found = True
    for cmd in expected:
        try:
            info = c.execute_command('COMMAND', 'INFO', cmd)
            found = info is not None and len(info) > 0
            if not found:
                all_found = False
        except Exception:
            all_found = False
    check("All 6 BLOB commands registered", all_found)
    # Verify ACL enforcement: create a restricted user and confirm denial.
    try:
        c.execute_command('ACL', 'SETUSER', 'testdeny', 'on', '>pass123',
                          '+@all', '-@largeobj', '~*')
        c2 = valkey.Valkey(host=c.connection_pool.connection_kwargs['host'],
                           port=c.connection_pool.connection_kwargs['port'],
                           decode_responses=False)
        c2.execute_command('AUTH', 'testdeny', 'pass123')
        try:
            c2.execute_command('BLOB.TCP_GET', key('any_key'))
            check("ACL -@largeobj blocks BLOB.TCP_GET", False, "no error")
        except valkey.ResponseError as e:
            check("ACL -@largeobj blocks BLOB.TCP_GET",
                  'no permissions' in str(e).lower(), str(e))
        c.execute_command('ACL', 'DELUSER', 'testdeny')
    except Exception as e:
        check("ACL enforcement test", False, f"setup error: {e}")


def test_memory_usage(c):
    """MEMORY USAGE returns a value for module keys."""
    section("17. MEMORY USAGE reporting")
    payload_size = 4096
    c.execute_command('BLOB.TCP_SET', key('mem_key'), make_payload(payload_size))
    # Trigger promotion so DRAM reports the payload
    for _ in range(3):
        c.execute_command('BLOB.TCP_GET', key('mem_key'))
    mem = c.execute_command('MEMORY', 'USAGE', key('mem_key'))
    check("MEMORY USAGE is non-nil", mem is not None)
    check("MEMORY USAGE > 0", isinstance(mem, int) and mem > 0, f"got {mem}")
    c.execute_command('DEL', key('mem_key'))


def test_config_constraints(c):
    """CONFIG SET enforces cross-config constraints."""
    section("18. CONFIG SET constraint enforcement")
    # min-buffers-per-op > max-buffers-per-op should fail
    c.execute_command('CONFIG', 'SET', 'largeobj.max-buffers-per-op', '2')
    try:
        c.execute_command('CONFIG', 'SET', 'largeobj.min-buffers-per-op', '3')
        check("min > max rejected", False, "no error")
    except valkey.ResponseError as e:
        check("min > max rejected", 'min-buffers-per-op' in str(e).lower(), str(e))
    # Restore defaults
    c.execute_command('CONFIG', 'SET', 'largeobj.max-buffers-per-op', '8')
    c.execute_command('CONFIG', 'SET', 'largeobj.min-buffers-per-op', '2')


def test_wrong_arity(c):
    """Commands with wrong argument count return arity errors."""
    section("19. Arity validation")
    arity_cases = [
        ('BLOB.TCP_SET', ['k']),               # missing data arg
        ('BLOB.TCP_GET',  []),                   # missing key
        ('BLOB.INFO', ['k', 'LEN', 'CRC']),    # too many args
    ]
    for cmd, args in arity_cases:
        try:
            c.execute_command(cmd, *args)
            check(f"{cmd} wrong arity → error", False, "no error raised")
        except valkey.ResponseError as e:
            check(f"{cmd} wrong arity → error",
                  'wrong number of arguments' in str(e).lower() or 'arity' in str(e).lower(),
                  str(e))


def test_keyspace_events(c):
    """Keyspace notifications fire for module operations."""
    section("20. Keyspace event notifications")
    c.execute_command('CONFIG', 'SET', 'notify-keyspace-events', 'KEA')
    pubsub = c.pubsub()
    pubsub.psubscribe('__keyevent@*__:largeobj.*')
    # Consume subscribe confirmation
    msg = pubsub.get_message(timeout=1)
    check("Pubsub subscribed", msg is not None and msg['type'] == 'psubscribe')
    # CREATE event
    c.execute_command('BLOB.TCP_SET', key('evt_key'), make_payload(4096))
    msg = pubsub.get_message(timeout=1)
    if msg:
        event = msg['channel'].decode().split(':', 1)[1]
        check("largeobj.create event fires", event == 'largeobj.create',
              f"got {event}")
    else:
        check("largeobj.create event fires", False, "no message received")
    # UPDATE event (overwrite)
    c.execute_command('BLOB.TCP_SET', key('evt_key'), make_payload(8192))
    msg = pubsub.get_message(timeout=1)
    if msg:
        event = msg['channel'].decode().split(':', 1)[1]
        check("largeobj.update event fires", event == 'largeobj.update',
              f"got {event}")
    else:
        check("largeobj.update event fires", False, "no message received")
    pubsub.unsubscribe()
    c.execute_command('DEL', key('evt_key'))


def test_crc_changes_on_overwrite(c):
    """Overwriting a key updates CRC."""
    section("21. CRC changes on overwrite")
    c.execute_command('BLOB.TCP_SET', key('crc_key'), make_payload(4096, b'A'))
    crc1 = c.execute_command('BLOB.INFO', key('crc_key'), 'CRC')
    c.execute_command('BLOB.TCP_SET', key('crc_key'), make_payload(8192, b'B'))
    crc2 = c.execute_command('BLOB.INFO', key('crc_key'), 'CRC')
    check("CRC changes after overwrite", crc1 != crc2,
          f"crc1={crc1}, crc2={crc2}")
    c.execute_command('DEL', key('crc_key'))


def test_disk_maxmemory(c):
    """disk-maxmemory prevents exceeding NVMe capacity."""
    section("22. disk-maxmemory enforcement")
    # Disable RDB saves to avoid MISCONF errors during test.
    c.execute_command('CONFIG', 'SET', 'save', '')
    c.execute_command('CONFIG', 'SET', 'stop-writes-on-bgsave-error', 'no')
    # Save originals
    orig_disk = c.execute_command('CONFIG', 'GET', 'largeobj.disk-maxmemory')
    orig_disk_val = orig_disk[1] if orig_disk else b'10737418240'
    orig_obj = c.execute_command('CONFIG', 'GET', 'largeobj.max-object-size')
    orig_obj_val = orig_obj[1] if orig_obj else b'536870912'
    c.execute_command('FLUSHALL')
    time.sleep(1)  # let async teardown settle
    # Lower max-object-size first so disk-maxmemory can go below the original.
    cap = 4 * 1024 * 1024  # 4 MB disk cap
    obj_limit = 256 * 1024  # 256 KB max-object-size
    c.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(obj_limit))
    c.execute_command('CONFIG', 'SET', 'largeobj.disk-maxmemory', str(cap))
    # Each 256KB object → disk_len ≈ 4096 (header) + 256KB ≈ 266240 (~260 KB)
    # ~15 fit in 4 MB, so we'll hit the cap somewhere.
    payload = make_payload(obj_limit)
    filled = 0
    for i in range(30):
        try:
            c.execute_command('BLOB.TCP_SET', key(f'cap_{i}'), payload)
            filled += 1
        except valkey.ResponseError:
            break
    check("Some SETs succeeded before cap", filled > 0, f"filled={filled}")
    check("Cap was hit before 30 objects", filled < 30, f"filled={filled}")
    # Restore (order matters: raise disk-maxmemory before raising max-object-size)
    c.execute_command('FLUSHALL')
    time.sleep(1)
    c.execute_command('CONFIG', 'SET', 'largeobj.disk-maxmemory', orig_disk_val.decode())
    c.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', orig_obj_val.decode())


def test_concurrent_set_get(c_factory):
    """Concurrent SET/GET from separate connections doesn't corrupt data."""
    section("23. Concurrent SET/GET (multiple connections)")
    import threading
    errors = []
    def worker(idx):
        try:
            conn = c_factory()
            payload = make_payload(65536, bytes([idx % 256 or 0x42]))
            k = key(f'conc_{idx}')
            conn.execute_command('BLOB.TCP_SET', k, payload)
            data = conn.execute_command('BLOB.TCP_GET', k)
            if data != payload:
                errors.append(f"Key {k}: data mismatch")
            conn.execute_command('DEL', k)
        except Exception as e:
            errors.append(f"Worker {idx}: {e}")
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    check("10 concurrent workers: no corruption", len(errors) == 0,
          "; ".join(errors) if errors else "")


def test_steady_state(c_factory):
    """Simulate realistic usage: multiple clients doing SET/GET/overwrite/DEL
    concurrently with mixed object sizes, verifying data integrity throughout."""
    section("24. Steady-state mixed workload")
    import threading
    import random
    NUM_CLIENTS = 4
    OPS_PER_CLIENT = 50
    SIZES = [1024, 4096, 32768, 262144, 1024 * 1024]  # 1KB to 1MB
    errors = []
    ops_completed = [0]
    lock = threading.Lock()
    def worker(client_id):
        conn = c_factory()
        local_keys = {}
        try:
            for op in range(OPS_PER_CLIENT):
                action = random.choice(['set', 'set', 'get', 'get', 'overwrite',
                                        'info', 'del'])
                k = key(f'ss_c{client_id}_k{random.randint(0, 9)}')
                if action == 'set' or (action in ('get', 'info', 'del')
                                       and k not in local_keys):
                    size = random.choice(SIZES)
                    payload = make_payload(size, bytes([
                        (client_id * 37 + size) % 256 or 0x42]))
                    conn.execute_command('BLOB.TCP_SET', k, payload)
                    local_keys[k] = payload
                elif action == 'get':
                    data = conn.execute_command('BLOB.TCP_GET', k)
                    if data != local_keys[k]:
                        errors.append(
                            f"C{client_id} GET {k}: expected "
                            f"len={len(local_keys[k])}, got "
                            f"len={len(data) if data else 'None'}")
                elif action == 'overwrite':
                    size = random.choice(SIZES)
                    payload = make_payload(size, bytes([
                        (client_id * 41 + op) % 256 or 0x42]))
                    conn.execute_command('BLOB.TCP_SET', k, payload)
                    local_keys[k] = payload
                elif action == 'info' and k in local_keys:
                    info_len = conn.execute_command('BLOB.INFO', k, 'LEN')
                    if info_len != len(local_keys[k]):
                        errors.append(
                            f"C{client_id} INFO {k}: expected "
                            f"len={len(local_keys[k])}, got {info_len}")
                elif action == 'del' and k in local_keys:
                    conn.execute_command('DEL', k)
                    del local_keys[k]
                with lock:
                    ops_completed[0] += 1
            # Cleanup remaining keys
            for k in list(local_keys):
                conn.execute_command('DEL', k)
        except Exception as e:
            errors.append(f"C{client_id}: {e}")
    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(NUM_CLIENTS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    total_ops = NUM_CLIENTS * OPS_PER_CLIENT
    check(f"{ops_completed[0]}/{total_ops} ops completed",
          ops_completed[0] == total_ops, f"completed={ops_completed[0]}")
    check("No data corruption under mixed workload", len(errors) == 0,
          "; ".join(errors[:5]) if errors else "")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global KEY_PREFIX
    parser = argparse.ArgumentParser(description="valkey-large-object functional test")
    parser.add_argument('--host', default='127.0.0.1', help='Valkey server host')
    parser.add_argument('--port', type=int, default=6379, help='Valkey server port')
    parser.add_argument('--tls', action='store_true',
                        help='Enable TLS (required for ElastiCache/DubDevo)')
    parser.add_argument('--cluster-mode', action='store_true',
                        help='Enable cluster mode (uses hash tags to ensure same-slot keys)')
    args = parser.parse_args()

    if args.cluster_mode:
        KEY_PREFIX = "{t}"

    tls_note = ", TLS" if args.tls else ""
    cluster_note = ", cluster-mode" if args.cluster_mode else ""
    print(f"Connecting to {args.host}:{args.port}{tls_note}{cluster_note}...")
    c = valkey.Valkey(host=args.host, port=args.port, ssl=args.tls, decode_responses=False)
    try:
        c.ping()
    except Exception as e:
        print(f"ERROR: Cannot connect to server: {e}")
        sys.exit(1)

    # Verify module is loaded
    modules = c.execute_command('MODULE', 'LIST')
    mod_names = [m[1] for m in modules] if modules else []
    if b'largeobj' not in mod_names:
        print(f"ERROR: largeobj module not loaded. Modules: {mod_names}")
        sys.exit(1)

    print(f"Connected. Module loaded. Starting tests...\n")
    c.execute_command('FLUSHALL')
    # Disable RDB persistence to avoid MISCONF errors during destructive tests.
    c.execute_command('CONFIG', 'SET', 'save', '')
    c.execute_command('CONFIG', 'SET', 'stop-writes-on-bgsave-error', 'no')

    # Detect storage mode: Tiered (NVMe+DRAM) vs Dram-only
    c.execute_command('BLOB.TCP_SET', key('_mode_detect'), make_payload(1024))
    mode_tier = c.execute_command('BLOB.INFO', key('_mode_detect'), 'TIER')
    c.execute_command('DEL', key('_mode_detect'))
    is_tiered_mode = (mode_tier == b'nvme')
    if is_tiered_mode:
        print("Storage mode: Tiered (NVMe → DRAM)\n")
    else:
        print(f"Storage mode: Dram-only (tier={mode_tier!r})\n")

    def client_factory():
        return valkey.Valkey(host=args.host, port=args.port, ssl=args.tls, decode_responses=False)

    test_basic_set_get(c)
    test_get_nonexistent(c)
    test_overwrite(c)
    test_delete(c)
    test_blob_info(c)
    test_info_errors(c)
    test_wrong_type_errors(c)
    test_zero_length_rejected(c)
    test_max_object_size(c)
    test_copy(c)
    test_expiry(c)
    test_flushall(c)
    if is_tiered_mode:
        test_tiered_promotion(c)
    else:
        section("13. Tiered mode: NVMe → DRAM promotion")
        print("  ⊘ SKIPPED (Dram-only mode — no NVMe tier)")
    test_multiple_objects(c)
    test_info_section(c)
    test_acl_category(c)
    test_memory_usage(c)
    test_config_constraints(c)
    test_wrong_arity(c)
    test_keyspace_events(c)
    test_crc_changes_on_overwrite(c)
    if is_tiered_mode:
        test_disk_maxmemory(c)
    else:
        section("22. disk-maxmemory enforcement")
        print("  ⊘ SKIPPED (Dram-only mode — no disk cap)")
    test_concurrent_set_get(client_factory)
    test_steady_state(client_factory)

    # Cleanup
    c.execute_command('FLUSHALL')

    print(f"\n{'═' * 60}")
    print(f"  Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'═' * 60}")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == '__main__':
    main()
