//! DRAMPool scaling cron — proactive expand and shrink.
//!
//! Called on a configurable timer from the Valkey main event-loop thread.
//! Handles: draining completion, Dram-mode shrink key deletion, proactive
//! expand, proactive shrink.

use std::cell::RefCell;

use valkey_module::{raw, Context, KeysCursor};

use super::get_dram_pool;
use crate::data_type::{LoValue, LO_TYPE};

/// `RedisModule_Scan` calls per cron tick while reclaim-list keys remain.
/// Each call visits one keyspace hash bucket (a few keys), so this bounds the
/// main-thread time one tick spends scanning.
const RECLAIM_SCAN_CALLS_PER_TICK: usize = 1000;

/// Where the reclaim scan resumes on the next tick.
struct ReclaimScan {
    db: i32,
    cursor: KeysCursor,
}

thread_local! {
    // Scaling cron only, which always runs on the main event-loop thread.
    static RECLAIM_SCAN: RefCell<ReclaimScan> = RefCell::new(ReclaimScan { db: 0, cursor: KeysCursor::new() });
}

/// Scaling cron. Fires on the main event-loop thread via a Valkey module timer.
///
/// Performs these actions in order:
/// 1. Complete any segments whose drain finished (refcount reached 0).
/// 2. Delete keys on the reclaim list.
/// 3. Proactive expand: if pool utilization exceeds the expand watermark,
///    add a segment before the hot path stalls on segment creation.
/// 4. Proactive shrink: if server memory pressure exceeds the shrink watermark,
///    evict the least-used segment. Tiered: data stays on NVMe. Dram: its keys
///    go on the reclaim list for step 2.
pub fn scaling_cron(ctx: &Context) {
    let expand_watermark = crate::scaling_expand_watermark();
    let shrink_watermark = crate::scaling_shrink_watermark();
    let poll_ms = crate::scaling_poll_ms();

    let pool = get_dram_pool();

    // 1. Complete draining of any segments whose refcount hit 0.
    pool.release_drained_segments();

    // 2. Delete keys left pointing at reclaimed objects.
    delete_reclaimed_keys(ctx);

    // 3. Proactive expand: grow before the pool fills so promotions don't
    //    stall on segment creation + EFA registration on the hot path.
    let util = pool.utilization_ratio();
    let expanded = util > expand_watermark && pool.try_expand(ctx).is_some();
    if expanded {
        ctx.log_notice(&format!(
            "largeobj: scaling — pool utilization {:.1}% > {:.0}%, added one DRAM segment",
            util * 100.0,
            expand_watermark * 100.0
        ));
    }

    // 4. Proactive shrink: yield memory back to core when server is under pressure.
    //
    // Dram mode deletes keys (no NVMe copy to fall back on). Without it, core
    // data types cannot be written once LargeObjects fill memory: core evicting
    // an LO key only returns its buffer to the segment, so used_memory does not
    // drop until a whole segment is released. Skipped when every key is a LargeObject:
    // there is no other data type to make room for.
    //
    // Shrink is SERVER-scoped (crate::server_memory), not module-scoped: we give
    // DRAM back only under Valkey-wide pressure, so the module's own pool pressure
    // (which drives expand) must not trigger shrink.
    //
    // Expand takes priority within a tick: expand and shrink read different
    // denominators (module utilization vs server memory), so both can cross on
    // the same tick, and firing both would add a segment then immediately drain
    // one — pure churn. Gated on expand-SUCCEEDED, not merely wanted, so a pool
    // that can't grow still shrinks under pressure.
    if expanded {
        rearm_scaling_cron(ctx, poll_ms);
        return;
    }
    let (used, maxmemory) = crate::server_memory(ctx);
    if maxmemory == 0 {
        // No server-wide maxmemory configured — no shrink pressure signal exists.
        rearm_scaling_cron(ctx, poll_ms);
        return;
    }
    let ratio = used as f64 / maxmemory as f64;
    let dram_mode = crate::operating_mode() == crate::OperatingMode::Dram;
    if ratio > shrink_watermark && (!dram_mode || has_non_lo_keys(ctx)) {
        // Skip shrink if a segment is already draining — its memory hasn't
        // been freed yet. Draining completes asynchronously as Arc holders
        // drop; firing another shrink now would drain a second segment before
        // the first is even released. Check on the next tick after
        // release_drained_segments() has had a chance to finish it.
        let (_, draining, _) = pool.segment_counts();
        if draining == 0 && pool.try_shrink() {
            ctx.log_notice(&format!(
                "largeobj: scaling — memory pressure {:.1}% > {:.0}%, evicted one DRAM segment",
                ratio * 100.0,
                shrink_watermark * 100.0
            ));
            // Release now if no reader holds the victim.
            pool.release_drained_segments();
        }
    }

    rearm_scaling_cron(ctx, poll_ms);
}

/// Whether the keyspace holds any non-LargeObject keys: total keys across all
/// dbs exceeds the LargeObject count. Dram shrink deletes LargeObject keys to
/// free memory for other data types, so with none of those there is nothing
/// to make room for. `num_objects` decrements when `lo_free` drops the value (async), so
/// right after a delete it can read high and delay a shrink by one tick.
fn has_non_lo_keys(ctx: &Context) -> bool {
    let mut total_keys: u64 = 0;
    let mut db = 0;
    // SelectDb fails past the last db.
    while unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.get_raw(), db) }
        == raw::REDISMODULE_OK as i32
    {
        total_keys += unsafe { raw::RedisModule_DbSize.unwrap()(ctx.get_raw()) };
        db += 1;
    }
    total_keys > crate::info::LARGE_OBJECT_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Delete keys whose oids are on the reclaim list, one tick's scan budget
/// at a time. Scans db by db, resuming across ticks, until the reclaim list is
/// empty. An oid leaves the list when its key is deleted, here or by any other
/// path (`lo_free` -> `remove_object`), so the scan cannot outlive its keys.
///
/// Cross-slot deletion is safe here: a timer callback runs outside command
/// execution (`server.current_client` is NULL), so key lookups hash each key's
/// own slot instead of reusing a command's cached slot.
fn delete_reclaimed_keys(ctx: &Context) {
    let mut reclaim = super::reclaim::lock().clone();
    if reclaim.is_empty() {
        return;
    }
    RECLAIM_SCAN.with_borrow_mut(|scan| {
        for _ in 0..RECLAIM_SCAN_CALLS_PER_TICK {
            if reclaim.is_empty() {
                break;
            }
            // SelectDb fails past the last db: one full pass done. At most one
            // pass per tick so a key moved behind the cursor can't spin us.
            if unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.get_raw(), scan.db) }
                != raw::REDISMODULE_OK as i32
            {
                scan.db = 0;
                break;
            }
            // The scan callback's key handle is read-only: collect, then delete.
            let found = RefCell::new(Vec::new());
            let more = scan.cursor.scan(ctx, &|_ctx, name, key| {
                if let Some(Ok(Some(lo))) = key.map(|k| k.get_value::<LoValue>(&LO_TYPE)) {
                    if reclaim.contains(&lo.object_id) {
                        found
                            .borrow_mut()
                            .push((name.as_slice().to_vec(), lo.object_id));
                    }
                }
            });
            for (name, oid) in found.into_inner() {
                unlink_key(ctx, &name);
                // lo_free runs later on the BIO thread; clear now so the next
                // tick doesn't hunt for an already-deleted key.
                super::reclaim::remove(&oid);
                reclaim.remove(&oid);
            }
            if !more {
                scan.cursor.restart();
                scan.db += 1;
            }
        }
    });
}

/// UNLINK `name` in the selected db.
fn unlink_key(ctx: &Context, name: &[u8]) {
    let key_name = ctx.create_string(name);
    let key = ctx.open_key_writable(&key_name);
    let _ = key.unlink();
}

/// Re-arm the scaling cron for the next tick.
pub fn rearm_scaling_cron(ctx: &Context, poll_ms: u64) {
    ctx.create_timer(
        std::time::Duration::from_millis(poll_ms),
        |ctx, ()| {
            scaling_cron(ctx);
        },
        (),
    );
}
