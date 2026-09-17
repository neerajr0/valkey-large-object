//! Error and operational metrics for the module.
//!
//! AtomicU64 counters exposed via the Valkey `INFO largeobj` section.
//!
//! Two sections:
//! - `error_metrics`: pre-panic integrity counters (incremented before a panic
//!   so the count is available in the crash report).
//! - `operational_metrics`: recoverable error counters for monitoring dashboards.

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::InfoContext;

// ─── Integrity Metrics (pre-panic) ───────────────────────────────────────────

pub static CRC_MISMATCH_COUNT: AtomicU64 = AtomicU64::new(0);
pub static HEADER_READ_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static HEADER_INVALID_COUNT: AtomicU64 = AtomicU64::new(0);
pub static POLLER_FAILURES: AtomicU64 = AtomicU64::new(0);

// ─── Operational Metrics (recoverable errors) ────────────────────────────────

pub static NVME_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static NVME_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static EFA_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static EFA_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static EFA_TIMEOUT_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static DRAM_POOL_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static NVME_BUFFER_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static NVME_CAPACITY_EXCEEDED: AtomicU64 = AtomicU64::new(0);
pub static SET_FINALIZE_STALE: AtomicU64 = AtomicU64::new(0);
pub static SET_VALUE_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static EFA_DRAIN_CEILING_FIRED: AtomicU64 = AtomicU64::new(0);
pub static EFA_DRAIN_BUFFERS_LEAKED: AtomicU64 = AtomicU64::new(0);

// ─── INFO Handler ────────────────────────────────────────────────────────────

pub fn largeobj_info_handler(
    ctx: &InfoContext,
    _for_crash_report: bool,
) -> Result<(), valkey_module::ValkeyError> {
    ctx.builder()
        .add_section("error_metrics")
        .field(
            "crc_mismatch_count",
            CRC_MISMATCH_COUNT.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "header_read_failures",
            HEADER_READ_FAILURES.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "header_invalid_count",
            HEADER_INVALID_COUNT.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "poller_failures",
            POLLER_FAILURES.load(Ordering::Relaxed) as i64,
        )?
        .build_section()?
        .add_section("operational_metrics")
        .field(
            "nvme_read_errors",
            NVME_READ_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "nvme_write_errors",
            NVME_WRITE_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_read_errors",
            EFA_READ_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_write_errors",
            EFA_WRITE_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_timeout_errors",
            EFA_TIMEOUT_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "dram_pool_exhausted",
            DRAM_POOL_EXHAUSTED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "nvme_buffer_exhausted",
            NVME_BUFFER_EXHAUSTED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "nvme_capacity_exceeded",
            NVME_CAPACITY_EXCEEDED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "set_finalize_stale",
            SET_FINALIZE_STALE.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "set_value_failures",
            SET_VALUE_FAILURES.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_drain_ceiling_fired",
            EFA_DRAIN_CEILING_FIRED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_drain_buffers_leaked",
            EFA_DRAIN_BUFFERS_LEAKED.load(Ordering::Relaxed) as i64,
        )?
        .build_section()?
        .build_info()?;
    Ok(())
}
