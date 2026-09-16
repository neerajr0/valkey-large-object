//! DRAMPool scaling cron — proactive expand and shrink.
//!
//! Called on a configurable timer from the Valkey main event-loop thread.
//! Handles: draining completion, proactive expand, proactive shrink.

use valkey_module::Context;

use super::get_dram_pool;

/// Scaling cron. Fires on the main event-loop thread via a Valkey module timer.
///
/// Performs three actions in order:
/// 1. Complete any segments whose drain finished (refcount reached 0).
/// 2. Proactive expand: if pool utilization exceeds the expand watermark,
///    add a segment before the hot path stalls on segment creation.
/// 3. Proactive shrink: if server memory pressure exceeds the shrink watermark,
///    evict the least-used segment (Tiered mode: safe, data on NVMe).
pub fn scaling_cron(ctx: &Context) {
    let expand_watermark = crate::scaling_expand_watermark();
    let shrink_watermark = crate::scaling_shrink_watermark();
    let poll_ms = crate::scaling_poll_ms();

    let pool = get_dram_pool();

    // 1. Complete draining of any segments whose refcount hit 0.
    pool.complete_drained_segments();

    // 2. Proactive expand: grow before the pool fills so promotions don't
    //    stall on segment creation + EFA registration on the hot path.
    let util = pool.utilization_ratio();
    if util > expand_watermark && pool.try_expand().is_some() {
        ctx.log_notice(&format!(
            "largeobj: scaling — pool utilization {:.1}% > {:.0}%, added one DRAM segment",
            util * 100.0,
            expand_watermark * 100.0
        ));
    }

    // 3. Proactive shrink: yield memory back to core when server is under pressure.
    // try_shrink() is safe in both modes: in Dram mode it only drains segments
    // with zero allocated bytes, so no live data is ever lost.
    let info = ctx.server_info("memory");
    let used: u64 = info.field_unsigned("used_memory").unwrap_or(0);
    let maxmemory: u64 = info.field_unsigned("maxmemory").unwrap_or(0);

    let dram_max = crate::dram_maxmemory();
    let ceiling = if dram_max > 0 {
        dram_max
    } else if maxmemory > 0 {
        maxmemory
    } else {
        // No ceiling configured — nothing to shrink against.
        rearm_scaling_cron(ctx, poll_ms);
        return;
    };

    let ratio = used as f64 / ceiling as f64;
    if ratio > shrink_watermark && pool.try_shrink() {
        ctx.log_notice(&format!(
            "largeobj: scaling — memory pressure {:.1}% > {:.0}%, evicted one DRAM segment",
            ratio * 100.0,
            shrink_watermark * 100.0
        ));
    }

    rearm_scaling_cron(ctx, poll_ms);
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
