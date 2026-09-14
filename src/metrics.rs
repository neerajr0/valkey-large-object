//! Error metrics for unrecoverable conditions.
//!
//! AtomicU64 counters exposed via the Valkey `INFO largeobj` section.
//! Incremented before a panic so the count is available in the crash report
//! (`_for_crash_report` parameter in the info handler).

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::InfoContext;

pub static CRC_MISMATCH_COUNT: AtomicU64 = AtomicU64::new(0);
pub static HEADER_READ_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static HEADER_INVALID_COUNT: AtomicU64 = AtomicU64::new(0);
pub static POLLER_FAILURES: AtomicU64 = AtomicU64::new(0);

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
        .build_info()?;
    Ok(())
}
