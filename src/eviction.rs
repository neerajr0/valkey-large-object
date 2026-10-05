//! Eviction — freeing a budget by destroying resident objects.
//!
//! `walk` is the whole policy: search bound, sampling, ranking, skips, termination. Each mode
//! supplies a `Budget` (`claim` takes one victim, `satisfy` says whether enough is free):
//! - `Dram`, `Arena`: `satisfy` is `alloc_exact`, which refuses when the freed bytes are not in one
//!   segment, so victims are destroyed as they are claimed and a short walk has already spent them.
//! - `Tiered`, `DiskLedger`: `satisfy` is a comparison, so nothing is evicted until the claims
//!   cover the request and a short walk evicts nothing. The write task settles the bytes
//!   (`DiskReservation`).
//!
//! `eviction-tenacity` maps to a time limit as in core; 100 has no clock. A walk also stops at
//! `MAX_STEPS_WITHOUT_LO_KEY`, `unclaimable_rounds_limit` or `MAX_SATISFY_REFUSALS`. The clock is
//! read only after `MIN_CANDIDATES`, so with sparse `LO` keys a walk can overrun it (accepted).
//!
//! Candidates come from resumable per-DB `RM_Scan` cursors rather than random draws, and the scan
//! visits every key, not just `LO` keys. A walk takes one bucket per populated DB in turn, always
//! starting at DB 0, so a short one may never reach the higher DBs.
//!
//! In cluster mode core resolves every lookup during a command to that command's own slot, so a
//! victim in another slot cannot be opened or deleted from here: `delete` reports success and the
//! key survives with its data freed. A victim is therefore tombstoned first (see `tombstone`), then
//! deleted inline if the lookup can see it; otherwise `arm_sweep` schedules a timer, where no
//! command is executing, and `sweep` deletes it there. Until then the key still shows in commands
//! that do not read the value (`EXISTS`, `DBSIZE`, `KEYS`, `SCAN`).
//!
//! Selection runs on the event-loop thread and never awaits, so every drop returns its memory
//! inside the call stack; `alloc_by_evicting` relies on that to retry at once. An object with an
//! extra `Arc` (an in-flight GET, or a COPY reading its source) is pinned and skipped: claiming it
//! would report bytes that do not come back until the holder is done.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::os::raw::{c_int, c_longlong, c_void};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use valkey_module::key::verify_type;
use valkey_module::{raw, Context};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::storage::context::SegmentBuffer;
use crate::storage::ObjectFile;

// Counters exposed via `INFO largeobj`.

/// Objects evicted since module load.
pub static EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Arena bytes the victims held.
pub static RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// SETs that still failed after eviction ran: objects destroyed for nothing.
pub static EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Objects a walk passed over because someone still held them, in either mode.
pub static PINNED_SKIPS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Tiered counterparts. Reclaimed is each victim's whole `disk_len`; a failure is a walk that
/// claimed victims and still fell short, so nothing was evicted.
pub static DISK_EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DISK_RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DISK_EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// `Budget::satisfy` refusals of a fully-funded request: bytes free but not usable together.
pub static SATISFY_REFUSALS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Walks abandoned at `MAX_SATISFY_REFUSALS`: more victims will not fix the fragmentation.
pub static FRAGMENTATION_ABORTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Candidates a walk always draws before the clock may stop it, so tenacity 0 is a 0µs budget
/// rather than zero work. Core reads its clock every 16 evictions (`evict.c:578-605`).
const MIN_CANDIDATES: usize = 16;

/// Claim-nothing rounds tolerated at tenacity 0..20, doubling every `UNCLAIMABLE_TENACITY_STEP`
/// points (256 at 100).
const UNCLAIMABLE_ROUNDS_BASE: usize = 8;
const UNCLAIMABLE_TENACITY_STEP: i64 = 20;

/// Consecutive scan steps that yield no `LO` key before the walk stops looking. Bounds a walk over
/// a mostly foreign keyspace, including at tenacity 100.
const MAX_STEPS_WITHOUT_LO_KEY: usize = 256;

/// Refusals of a fully-funded request before the walk stops. Only `Arena` refuses. A refusal
/// resets the credit, so neither `unclaimable_rounds` nor a clock would end the walk.
const MAX_SATISFY_REFUSALS: usize = 3;

// Per-DB `RM_Scan` cursors, resumed across walks. Thread-local because every entry point runs on
// the main thread; one per DB because `RM_Scan` walks `ctx->client->db` only. Cursors survive
// writes, so deleting victims mid-walk cannot make one skip a key.
thread_local! {
    static CURSORS: RefCell<HashMap<c_int, Rc<ScanCursor>>> = RefCell::new(HashMap::new());
}

/// A resumable `RM_Scan` cursor. Not the crate's `KeysCursor`: the callback needs the raw key handle.
struct ScanCursor(*mut raw::RedisModuleScanCursor);

impl ScanCursor {
    fn new() -> Self {
        // SAFETY: creating a cursor touches no server state.
        Self(unsafe { raw::RedisModule_ScanCursorCreate.unwrap()() })
    }

    fn restart(&self) {
        // SAFETY: `self.0` is a live cursor until `drop`.
        unsafe { raw::RedisModule_ScanCursorRestart.unwrap()(self.0) };
    }
}

impl Drop for ScanCursor {
    fn drop(&mut self) {
        // SAFETY: `self.0` is a live cursor, destroyed exactly once.
        unsafe { raw::RedisModule_ScanCursorDestroy.unwrap()(self.0) };
    }
}

/// The cursor for `db`, cloned out so the walk (which deletes keys and may re-enter) never runs
/// inside the `RefCell` borrow.
fn cursor_for(db: c_int) -> Rc<ScanCursor> {
    CURSORS.with(|cursors| {
        Rc::clone(
            cursors
                .borrow_mut()
                .entry(db)
                .or_insert_with(|| Rc::new(ScanCursor::new())),
        )
    })
}

/// Point `ctx` at `db`, where `RM_Scan` and `RM_OpenKey` look. On a command's context this moves
/// the client itself, so entry points put it back with `restoring_db`.
pub(crate) fn select_db(ctx: &Context, db: c_int) -> bool {
    // SAFETY: `ctx.ctx` is a live module context.
    unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.ctx, db) == raw::REDISMODULE_OK as c_int }
}

/// Run `f` and leave `ctx` on the DB it started on.
fn restoring_db<T>(ctx: &Context, f: impl FnOnce() -> T) -> T {
    // SAFETY: `ctx.ctx` is a live module context.
    let home = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };
    let out = f();
    select_db(ctx, home);
    out
}

/// Every DB that has keys, with its cursor. `RM_SelectDb` refuses an id past the last DB.
fn populated_dbs(ctx: &Context) -> Vec<(c_int, Rc<ScanCursor>)> {
    let mut dbs = Vec::new();
    let mut db = 0;
    while select_db(ctx, db) {
        // SAFETY: `ctx.ctx` is a live module context.
        if unsafe { raw::RedisModule_DbSize.unwrap()(ctx.ctx) } > 0 {
            dbs.push((db, cursor_for(db)));
        }
        db += 1;
    }
    dbs
}

// ─── The search time limit ───────────────────────────────────────────────────

/// `eviction-tenacity` as a wall-clock limit, ported from `evictionTimeLimitUs`
/// (`evict.c:363-378`): 0..=10 linear to 500µs, then 15% per point, 100 unbounded.
fn search_time_limit(tenacity: i64) -> Duration {
    match tenacity {
        t if t <= 10 => Duration::from_micros(50 * t.max(0) as u64),
        t if t < 100 => Duration::from_micros((500.0 * 1.15f64.powi(t as i32 - 10)) as u64),
        _ => Duration::MAX,
    }
}

/// Consecutive claim-nothing rounds a walk tolerates before calling the keyspace unhelpful: the
/// only stop when every `LO` key is pinned at tenacity 100 (no clock). It scales with tenacity like
/// a confidence threshold: with a fraction `p` of keys unclaimable, a walk quits spuriously with
/// probability `p^(samples * rounds)`.
fn unclaimable_rounds_limit(tenacity: i64) -> usize {
    UNCLAIMABLE_ROUNDS_BASE << (tenacity.clamp(0, 100) / UNCLAIMABLE_TENACITY_STEP)
}

/// `Duration::MAX` cannot be added to an `Instant`: `None` is the deadline that never arrives.
fn deadline_from_now(limit: Duration) -> Option<Instant> {
    Instant::now().checked_add(limit)
}

// ─── Shared: candidate supply ────────────────────────────────────────────────

/// A sampled key and what ranking needs to know about it, read off the scan's own key handle:
/// reopening by name would miss for a key outside the running command's slot.
struct Candidate {
    db: c_int,
    name: Vec<u8>,
    object_id: ObjectId,
    idle_ms: i64, // RM_GetLRU; -1 under an LFU policy
    freq: i64,    // RM_GetLFU; -1 unless LFU
    ttl: i64,     // RM_GetExpire; REDISMODULE_NO_EXPIRE if none
    /// Tiered mode: the object's file, so a claim can test for a reader without opening the key.
    file: Option<Arc<ObjectFile>>,
}

/// The keyspace walk as an iterator: scored `LO` keys from where each DB's cursor last stopped, one
/// bucket of each populated DB in turn, wrapping while the budget allows. Keys the policy excludes
/// never surface, so they cannot pass for unclaimable ones. `deadline` only applies once
/// `MIN_CANDIDATES` have been offered: a zero limit would yield nothing.
struct Candidates<'a> {
    ctx: &'a Context,
    policy: Policy,
    dbs: Vec<(c_int, Rc<ScanCursor>)>,
    next_db: usize,
    batch: std::vec::IntoIter<(i64, Candidate)>,
    deadline: Option<Instant>,
    yielded: usize,
    steps_without_lo_key: usize,
    /// Set once a stop condition fires. The batch in hand is still drained.
    stop: bool,
}

impl<'a> Candidates<'a> {
    fn new(ctx: &'a Context, policy: Policy, deadline: Option<Instant>) -> Self {
        let dbs = populated_dbs(ctx);
        Self {
            ctx,
            policy,
            stop: dbs.is_empty(),
            dbs,
            next_db: 0,
            batch: Vec::new().into_iter(),
            deadline,
            yielded: 0,
            steps_without_lo_key: 0,
        }
    }

    fn out_of_time(&self) -> bool {
        self.deadline.is_some_and(|at| Instant::now() >= at)
    }
}

impl Iterator for Candidates<'_> {
    type Item = (i64, Candidate);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(candidate) = self.batch.next() {
                self.yielded += 1;
                return Some(candidate);
            }
            if self.stop {
                return None;
            }

            let (db, cursor) = &self.dbs[self.next_db];
            let (db, cursor) = (*db, Rc::clone(cursor));
            self.next_db = (self.next_db + 1) % self.dbs.len();
            select_db(self.ctx, db);
            let (found, at_end) = scan_step(self.ctx, db, &cursor);
            if at_end {
                cursor.restart();
            }
            // A tombstoned object has nothing left to give.
            let policy = self.policy;
            let found: Vec<_> = found
                .into_iter()
                .filter(|candidate| !tombstone::contains(candidate.object_id))
                .filter_map(|candidate| {
                    victim_score(&candidate, policy).map(|score| (score, candidate))
                })
                .collect();
            if found.is_empty() {
                self.steps_without_lo_key += 1;
            } else {
                self.steps_without_lo_key = 0;
            }

            if self.steps_without_lo_key >= MAX_STEPS_WITHOUT_LO_KEY
                || (self.yielded >= MIN_CANDIDATES && self.out_of_time())
            {
                self.stop = true;
            }
            self.batch = found.into_iter();
        }
    }
}

/// What `collect` fills in while `RM_Scan` runs.
struct ScanSink {
    db: c_int,
    found: Vec<Candidate>,
}

/// `RM_Scan` callback: record the key if it is one of ours. Raw FFI because the crate's `ValkeyKey`
/// hides the LRU, LFU and TTL getters. The handle is built from the dictionary entry, so it works
/// for a key in any slot.
///
/// # Safety
/// Only passed to `RM_Scan` by `scan_step`, with `privdata` the `&mut ScanSink` it owns; the
/// server calls back synchronously, inside that call.
unsafe extern "C" fn collect(
    _ctx: *mut raw::RedisModuleCtx,
    name: *mut raw::RedisModuleString,
    key: *mut raw::RedisModuleKey,
    privdata: *mut c_void,
) {
    let sink = &mut *privdata.cast::<ScanSink>();
    if key.is_null() {
        return; // no handle offered; skip rather than reopen mid-scan
    }
    // The scan is keyspace-wide; `RM_CreateDataType` refuses duplicate names, so this is exact.
    if verify_type(key, &LO_TYPE).is_err() {
        return;
    }
    let value = raw::RedisModule_ModuleTypeGetValue.unwrap()(key).cast::<LoValue>();
    if value.is_null() {
        return;
    }
    let lo = &*value;

    let mut len = 0usize;
    let bytes = raw::RedisModule_StringPtrLen.unwrap()(name, &mut len).cast::<u8>();
    let mut idle_ms: raw::mstime_t = -1;
    let mut freq: c_longlong = -1;
    raw::RedisModule_GetLRU.unwrap()(key, &mut idle_ms);
    raw::RedisModule_GetLFU.unwrap()(key, &mut freq);
    sink.found.push(Candidate {
        db: sink.db,
        name: std::slice::from_raw_parts(bytes, len).to_vec(),
        object_id: lo.object_id,
        idle_ms,
        freq,
        ttl: raw::RedisModule_GetExpire.unwrap()(key),
        file: lo.file.clone(),
    });
}

/// Advance `cursor` one bucket and return the `LO` keys it visited plus whether the table ended.
/// Victims are collected rather than acted on in the callback: `RM_Scan` permits deleting only the
/// key being visited, and we may delete a whole bucket.
fn scan_step(ctx: &Context, db: c_int, cursor: &ScanCursor) -> (Vec<Candidate>, bool) {
    let mut sink = ScanSink {
        db,
        found: Vec::new(),
    };
    // SAFETY: `ctx.ctx` is a live module context, `cursor.0` a live cursor, and `sink` outlives
    // the call that is the only user of the pointer.
    let more = unsafe {
        raw::RedisModule_Scan.unwrap()(
            ctx.ctx,
            cursor.0,
            Some(collect),
            std::ptr::addr_of_mut!(sink).cast(),
        )
    };
    (sink.found, more == 0)
}

// ─── Shared: victim ranking ──────────────────────────────────────────────────

/// What the `maxmemory-policy` ranks victims by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ranking {
    /// Idle time under LRU, access frequency under LFU.
    Idleness,
    /// Soonest expiry first (`volatile-ttl`).
    Ttl,
    /// No ranking: any candidate is as good as another.
    Random,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Policy {
    ranking: Ranking,
    /// `volatile-*`: only keys with a TTL may be evicted.
    volatile_only: bool,
}

impl Policy {
    fn parse(name: &str) -> Self {
        let ranking = if name.ends_with("-random") {
            Ranking::Random
        } else if name == "volatile-ttl" {
            Ranking::Ttl
        } else {
            Ranking::Idleness
        };
        Self {
            ranking,
            volatile_only: name.starts_with("volatile-"),
        }
    }

    fn current(ctx: &Context) -> Self {
        let info = ctx.server_info("memory");
        Self::parse(info.field_c("maxmemory_policy").unwrap_or_default())
    }
}

/// Core's victim score (`evictionPoolPopulate`, `evict.c:122-137`) over what the scan read. Higher
/// is a better victim. `None` means not a candidate: under `volatile-*`, a key with no TTL (core
/// samples `db->expires` there; we sample everything and must exclude it ourselves).
///
/// LRU and LFU need no second policy read: `VM_GetLRU` writes -1 under an LFU policy and
/// `VM_GetLFU` under an LRU one (`module.c:14731-14758`), and -1 is unreachable for a live value,
/// so taking the non-negative one *is* `objectGetIdleness` (`object.c:1689-1693`).
///
/// Sampling stamps nothing: the scan involves no lookup, so `val->lru` stays as the last real
/// access left it. (Reading the LFU frequency does write back the decayed counter, as in core.)
fn victim_score(candidate: &Candidate, policy: Policy) -> Option<i64> {
    if policy.volatile_only && candidate.ttl == raw::REDISMODULE_NO_EXPIRE as i64 {
        return None;
    }
    Some(match policy.ranking {
        Ranking::Idleness if candidate.freq >= 0 => i64::from(u8::MAX) - candidate.freq,
        Ranking::Idleness => candidate.idle_ms.max(0),
        Ranking::Ttl => i64::MAX - candidate.ttl,
        Ranking::Random => 0,
    })
}

/// The O(1) refusals every budget makes before walking: `need` above the whole budget can never be
/// served, and nothing resident leaves nothing to evict. A zero `ceiling` means unlimited.
fn cannot_be_satisfied(need: u64, ceiling: u64, resident: u64) -> bool {
    (ceiling != 0 && need > ceiling) || resident == 0
}

// ─── Shared: the driver ──────────────────────────────────────────────────────

/// What a walk does with a victim, and what it is trying to produce.
trait Budget {
    type Output;

    /// Percent of a victim's bytes credited toward `need` before `satisfy` is first asked.
    const CREDIT_PERCENT: u64;

    /// Take the victim's bytes, or `None` to leave it (pinned, gone, not convertible). `Arena`
    /// evicts here, since `satisfy` cannot answer until the bytes are free; `DiskLedger` only records.
    fn claim(&mut self, ctx: &Context, candidate: Candidate) -> Option<u64>;

    /// The authority on whether the request is served; `Some` ends the walk.
    fn satisfy(&mut self, need: u64) -> Option<Self::Output>;
}

/// Sample, score, claim, repeat until `budget` is satisfied or a stop fires. Returns the output and
/// how many objects were claimed, so callers can tell a failed run from a no-op one.
///
/// The final `satisfy` can succeed with the request already paid for: a refusal resets the credit,
/// so a walk can free well over `need` and still end below the threshold, and the victims freed
/// since may have coalesced into the run the allocator wanted. Core does the same at `cant_free`.
fn walk<B: Budget>(ctx: &Context, need: u64, budget: &mut B) -> (Option<B::Output>, usize) {
    // One read each: configs can change at runtime.
    let tenacity = crate::eviction_tenacity();
    let deadline = deadline_from_now(search_time_limit(tenacity));
    let unclaimable_limit = unclaimable_rounds_limit(tenacity);
    let samples = crate::maxmemory_samples();
    let mut candidates = Candidates::new(ctx, Policy::current(ctx), deadline);

    let mut credit = 0u64;
    let mut victims = 0usize;
    let mut examined = 0usize;
    let mut unclaimable_rounds = 0usize;
    let mut refusals = 0usize;

    loop {
        // Per round, not per candidate: a round is bounded work.
        if examined >= MIN_CANDIDATES && candidates.out_of_time() {
            break;
        }

        let mut ranked: Vec<(i64, Candidate)> = candidates.by_ref().take(samples).collect();
        if ranked.is_empty() {
            break; // out of time, or nothing left to find
        }

        // Best victim first; a key we cannot claim costs the next-best, not the walk.
        ranked.sort_by_key(|(score, _)| std::cmp::Reverse(*score));

        let mut claimed_any = false;
        for (_score, candidate) in ranked {
            examined += 1;
            let Some(bytes) = budget.claim(ctx, candidate) else {
                continue;
            };
            victims += 1;
            claimed_any = true;
            credit += bytes * B::CREDIT_PERCENT / 100;
            if credit < need {
                continue;
            }
            if let Some(output) = budget.satisfy(need) {
                return (Some(output), victims);
            }
            // Free but unusably placed. Restarting the credit lets the next victims coalesce;
            // repeated refusals are fragmentation, which more victims will not fix.
            SATISFY_REFUSALS_TOTAL.fetch_add(1, Ordering::Relaxed);
            refusals += 1;
            if refusals >= MAX_SATISFY_REFUSALS {
                FRAGMENTATION_ABORTS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return (None, victims);
            }
            credit = 0;
        }

        unclaimable_rounds = if claimed_any {
            0
        } else {
            unclaimable_rounds + 1
        };
        if unclaimable_rounds >= unclaimable_limit {
            break;
        }
    }

    (budget.satisfy(need), victims)
}

// ─── DRAM arena: evict (Dram) ─────────────────────────────────────────────────

/// Allocate `need` bytes, destroying resident objects to make room.
pub fn alloc_by_evicting(ctx: &Context, need: usize) -> Option<Vec<SegmentBuffer>> {
    debug_assert_eq!(
        crate::operating_mode(),
        crate::OperatingMode::Dram,
        "evicting for the arena destroys the object — in Tiered mode a promotion skips instead"
    );
    let dram_pool = crate::storage::get_dram_pool();

    // No object spans segments, so nothing larger than one fits. Nothing resident means nothing to
    // evict, which also covers an arena of in-flight SET buffers no key points at yet.
    let segment_size = crate::dram_segment_size() as u64;
    if cannot_be_satisfied(need as u64, segment_size, dram_pool.object_count() as u64) {
        return None;
    }

    if !crate::eviction_allowed(ctx) {
        return None;
    }

    let mut arena = Arena;
    let (buffers, victims) = restoring_db(ctx, || walk(ctx, need as u64, &mut arena));

    // Only runs that evicted something: they paid and got nothing.
    if buffers.is_none() && victims > 0 {
        EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    buffers
}

struct Arena;

impl Budget for Arena {
    type Output = Vec<SegmentBuffer>;

    // Exact as a byte count but not as a placement: the discount keeps a near-miss (talc overhead)
    // from costing a refusal, which resets the credit.
    const CREDIT_PERCENT: u64 = 90;

    fn claim(&mut self, ctx: &Context, candidate: Candidate) -> Option<u64> {
        // An in-flight request holds this object; its bytes free only when the holder lets go.
        if crate::storage::get_dram_pool().is_pinned(&candidate.object_id) {
            PINNED_SKIPS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        reclaim(ctx, &candidate)
    }

    fn satisfy(&mut self, need: u64) -> Option<Vec<SegmentBuffer>> {
        crate::storage::get_dram_pool().alloc_exact(need as usize)
    }
}

/// Evict the `LO` object behind `candidate` and report the arena bytes it freed. `None` if it is
/// already gone.
fn reclaim(ctx: &Context, candidate: &Candidate) -> Option<u64> {
    let dram_pool = crate::storage::get_dram_pool();

    // Take the reference, and so the size, before evicting: `lo_free` removes the pool entry once
    // the key is deleted, possibly from the lazyfree thread.
    let obj_ctx = dram_pool.get_object(&candidate.object_id)?;
    let bytes = arena_bytes(&obj_ctx);

    evict(ctx, candidate.db, &candidate.name, candidate.object_id);

    // The bytes must be free now: the caller retries before the lazyfree thread could get to
    // them. Dropping the map's reference, and ours at scope end, is a talc coalesce, no syscall.
    dram_pool.remove_object(&candidate.object_id);

    RECLAIMED_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
    Some(bytes)
}

/// The arena bytes an object holds: the buffers' aligned `len`, not the user length.
fn arena_bytes(obj_ctx: &crate::storage::context::ObjectContext) -> u64 {
    obj_ctx.buffers.iter().map(|b| b.len as u64).sum()
}

// ─── Tombstones ──────────────────────────────────────────────────────────────

pub mod tombstone {
    //! Tombstones — evicted objects whose keys are still in the keyspace, waiting for `sweep`.
    //!
    //! Every read of a `LoValue` treats a tombstoned object as absent (`LoValue::is_tombstoned`).
    //! Keyed by object id, not key name: the key can be renamed or moved before the sweep, and a SET
    //! that overwrites it mints a new id, which must not inherit the tombstone. The recorded
    //! location is a hint the sweep verifies. An entry leaves when the sweep deletes the key or the
    //! value is freed by any other route (`DEL`, overwrite, expiry, flush: `lo_free` calls `remove`).

    use std::collections::HashMap;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{LazyLock, Mutex};

    use crate::data_type::ObjectId;

    /// Where the object's key was when it was tombstoned.
    #[derive(Clone, Debug)]
    pub struct Location {
        pub db: c_int,
        pub name: Vec<u8>,
    }

    static TOMBSTONES: LazyLock<Mutex<HashMap<ObjectId, Location>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// `TOMBSTONES.len()` without the lock: every `BLOB.GET` asks `is_tombstoned`, and on a node with
    /// nothing tombstoned (every standalone node) that should cost a load.
    static COUNT: AtomicUsize = AtomicUsize::new(0);

    fn table() -> std::sync::MutexGuard<'static, HashMap<ObjectId, Location>> {
        TOMBSTONES.lock().expect("tombstone table lock unavailable")
    }

    /// Tombstone `object_id`, whose key was `name` in `db`. Main thread, under the GIL.
    pub fn add(object_id: ObjectId, db: c_int, name: &[u8]) {
        let mut table = table();
        table.insert(
            object_id,
            Location {
                db,
                name: name.to_vec(),
            },
        );
        COUNT.store(table.len(), Ordering::Release);
    }

    /// Whether `object_id` has been evicted and its key not yet removed.
    pub fn contains(object_id: ObjectId) -> bool {
        COUNT.load(Ordering::Acquire) != 0 && table().contains_key(&object_id)
    }

    /// Drop the entry for `object_id`, if any. Any thread: `lo_free` runs on a lazyfree thread.
    pub fn remove(object_id: ObjectId) {
        if COUNT.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut table = table();
        table.remove(&object_id);
        COUNT.store(table.len(), Ordering::Release);
    }

    /// Every entry, so the sweep need not hold the lock across keyspace calls.
    pub fn snapshot() -> Vec<(ObjectId, Location)> {
        table().iter().map(|(id, at)| (*id, at.clone())).collect()
    }

    /// How many objects are tombstoned.
    pub fn len() -> usize {
        COUNT.load(Ordering::Acquire)
    }
}

// ─── Evicting a key ──────────────────────────────────────────────────────────

/// Evict the object `object_id` held by key `name` in `db`: tombstone it, then delete the key if the
/// lookup can see it (which removes the tombstone), else arm the sweep.
fn evict(ctx: &Context, db: c_int, name: &[u8], object_id: ObjectId) {
    tombstone::add(object_id, db, name);
    if !delete_if_present(ctx, db, name, object_id) {
        arm_sweep(ctx);
    }
}

/// Delete the key `name` in `db` if it still holds `object_id`, and drop that tombstone. `false` if
/// it does not: absent (renamed, moved, expired), overwritten under that name, or in a slot this
/// lookup cannot see. The id is checked because the name may have been overwritten, and because
/// `Key::delete` returns `Ok` unconditionally, so this is the only way to know whether anything
/// was deleted.
fn delete_if_present(ctx: &Context, db: c_int, name: &[u8], object_id: ObjectId) -> bool {
    select_db(ctx, db);
    let key_name = ctx.create_string(name.to_vec());
    let key = ctx.open_key_writable(&key_name);
    match key.get_value::<LoValue>(&LO_TYPE) {
        Ok(Some(lo)) if lo.object_id == object_id => {
            let _ = key.delete();
            tombstone::remove(object_id);
            true
        }
        _ => false,
    }
}

// ─── Sweeping tombstones ─────────────────────────────────────────────────────

/// Whether a sweep is already scheduled, so a burst of evictions arms one timer.
static SWEEP_ARMED: AtomicBool = AtomicBool::new(false);

/// Schedule a sweep `tombstone-sweep-ms` from now, unless one is already pending.
fn arm_sweep(ctx: &Context) {
    if SWEEP_ARMED.swap(true, Ordering::AcqRel) {
        return;
    }
    ctx.create_timer(
        Duration::from_millis(crate::tombstone_sweep_ms()),
        |ctx, ()| {
            SWEEP_ARMED.store(false, Ordering::Release);
            sweep(ctx);
        },
        (),
    );
}

/// Delete the key of every tombstoned object. Runs from a timer, where no command is executing,
/// so lookups resolve their own slots.
///
/// Each entry's location is checked, not trusted: one whose key is not there (renamed, moved,
/// expired, or deleted with its free still queued) is found by object id with `delete_strays`. An
/// object no scan finds has no key, so its entry is dropped.
fn sweep(ctx: &Context) {
    let entries = tombstone::snapshot();
    restoring_db(ctx, || {
        let mut strays = HashSet::new();
        for (object_id, at) in entries {
            if !delete_if_present(ctx, at.db, &at.name, object_id) {
                strays.insert(object_id);
            }
        }
        if !strays.is_empty() {
            delete_strays(ctx, &mut strays);
        }
        for object_id in strays {
            tombstone::remove(object_id);
        }
    });
}

/// Find the keys holding `strays` by scanning every DB and delete them, removing each one found
/// from `strays`. A fresh cursor per DB leaves the walk's own undisturbed.
/// HAZARD: O(keyspace) on the event loop (~0.8 s per 3M keys), and every RENAME or MOVE of a
/// tombstoned key lands here until the tombstone's location follows the key.
fn delete_strays(ctx: &Context, strays: &mut HashSet<ObjectId>) {
    let mut db = 0;
    while !strays.is_empty() && select_db(ctx, db) {
        let cursor = ScanCursor::new();
        let mut found = Vec::new();
        loop {
            let (batch, at_end) = scan_step(ctx, db, &cursor);
            found.extend(
                batch
                    .into_iter()
                    .filter(|candidate| strays.contains(&candidate.object_id)),
            );
            if at_end {
                break;
            }
        }
        // After the scan: `RM_Scan` permits deleting only the key being visited. A key that cannot
        // be deleted (expired, hidden from a write open) keeps its tombstone until core reaps it.
        for candidate in found {
            strays.remove(&candidate.object_id);
            delete_if_present(ctx, db, &candidate.name, candidate.object_id);
        }
        db += 1;
    }
}

// ─── Tiered: the nvme-maxmemory budget ───────────────────────────────────────

/// Claim enough resident objects to pay for `need` disk bytes. `None` means the search came up
/// short and the SET must be refused, with nothing evicted: victims are evicted only once the
/// claims cover the request.
///
/// The claim is exclusive: victims are tombstoned with their bytes still charged, so no other
/// request can select them or spend those bytes, and the write task can settle without a second
/// capacity check (see `DiskReservation`). The returned handles are the keyspace's own, shared
/// rather than taken, because the key may be in a slot the running command cannot reach.
///
/// Main thread only, before the SET's tokio task is spawned.
pub fn claim_disk_victims(ctx: &Context, need: u64) -> Option<Vec<Arc<ObjectFile>>> {
    let budget = crate::nvme_maxmemory();

    // An unlimited budget cannot refuse a reservation: fail loudly rather than evict for a cap
    // that does not exist.
    if budget == 0 {
        unreachable!("nvme-maxmemory is unlimited, yet a reservation was refused");
    }

    if cannot_be_satisfied(need, budget, crate::storage::nvme::nvme_disk_usage()) {
        return None;
    }

    if !crate::eviction_allowed(ctx) {
        return None;
    }

    restoring_db(ctx, || {
        let mut ledger = DiskLedger::default();
        let (claimed, victims) = walk(ctx, need, &mut ledger);

        if let Some(claimed) = claimed {
            return Some(evict_claimed(ctx, claimed));
        }

        // Claims only record, so nothing was lost: this counts wasted search.
        if victims > 0 {
            DISK_EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        None
    })
}

/// Accumulates claimed handles until they cover `need`. The running total is exact, so `satisfy`
/// is a comparison and never refuses. Each claim keeps its DB and name: the key is dealt with only
/// once `satisfy` succeeds.
#[derive(Default)]
struct DiskLedger {
    claimed: Vec<Claim>,
    freed: u64,
}

struct Claim {
    db: c_int,
    name: Vec<u8>,
    file: Arc<ObjectFile>,
}

impl Budget for DiskLedger {
    type Output = Vec<Claim>;

    // Exact, with no placement: any discount would over-evict.
    const CREDIT_PERCENT: u64 = 100;

    fn claim(&mut self, _ctx: &Context, candidate: Candidate) -> Option<u64> {
        let file = candidate.file?;
        // A key the cursor offers twice is claimed once (its own claim would look like a pin).
        if self
            .claimed
            .iter()
            .any(|claim| claim.file.object_id() == file.object_id())
        {
            return None;
        }
        // An `Arc` beyond the keyspace's and this candidate's is a reader, whose reference holds the
        // file's blocks past the unlink: claiming it would report budget we never receive.
        if Arc::strong_count(&file) > 2 {
            PINNED_SKIPS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let bytes = file.disk_len();
        self.freed += bytes;
        self.claimed.push(Claim {
            db: candidate.db,
            name: candidate.name,
            file,
        });
        Some(bytes)
    }

    fn satisfy(&mut self, need: u64) -> Option<Self::Output> {
        (self.freed >= need).then(|| std::mem::take(&mut self.claimed))
    }
}

/// Evict the claimed objects and hand their handles on. Called only once the walk covered the
/// request, so this is where eviction becomes irreversible. The handles ride on the reservation, so
/// the `unlink(2)` and the credit stay off the event loop.
fn evict_claimed(ctx: &Context, claimed: Vec<Claim>) -> Vec<Arc<ObjectFile>> {
    let dram_pool = crate::storage::get_dram_pool();
    claimed
        .into_iter()
        .map(|claim| {
            let object_id = claim.file.object_id();
            evict(ctx, claim.db, &claim.name, object_id);
            // The promoted copy, which `lo_free` would have dropped but a tombstoned key keeps.
            dram_pool.remove_object(&object_id);
            DISK_RECLAIMED_BYTES_TOTAL.fetch_add(claim.file.disk_len(), Ordering::Relaxed);
            DISK_EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
            claim.file
        })
        .collect()
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_names_pick_a_ranking_and_a_scope() {
        let cases = [
            ("allkeys-lru", Ranking::Idleness, false),
            ("allkeys-lfu", Ranking::Idleness, false),
            ("volatile-lru", Ranking::Idleness, true),
            ("volatile-ttl", Ranking::Ttl, true),
            ("allkeys-random", Ranking::Random, false),
            ("volatile-random", Ranking::Random, true),
            ("volatile-lfu", Ranking::Idleness, true),
            ("noeviction", Ranking::Idleness, false),
            ("", Ranking::Idleness, false),
        ];
        for (name, ranking, volatile_only) in cases {
            let got = Policy::parse(name);
            assert_eq!(
                (got.ranking, got.volatile_only),
                (ranking, volatile_only),
                "{name}"
            );
        }
    }

    /// The endpoints matter most: tenacity 0 must not mean unbounded and 100 must.
    #[test]
    fn tenacity_bounds_the_search() {
        assert_eq!(search_time_limit(0), Duration::ZERO);
        assert_eq!(search_time_limit(10), Duration::from_micros(500));
        assert_eq!(search_time_limit(100), Duration::MAX);
        assert!(deadline_from_now(search_time_limit(100)).is_none());
        assert_eq!(unclaimable_rounds_limit(10), 8);
        assert_eq!(unclaimable_rounds_limit(100), 256);
    }
}
