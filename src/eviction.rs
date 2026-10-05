//! Eviction — freeing a budget by evicting resident objects.
//!
//! # Driver
//!
//! `walk` is the whole policy: how long to search, which candidates to sample, how to rank them,
//! what to skip, when to stop. Both modes go through it, so victim selection cannot drift apart.
//! What differs is what a freed byte *is*, which each mode supplies as a `Budget`: `claim` takes
//! one victim and reports the bytes it yielded, `satisfy` says whether that is enough yet.
//!
//! A node runs in one mode for its lifetime. Promotion and demotion between the tiers do not go
//! through here.
//!
//! - `[Dram]` — `alloc_by_evicting` frees arena bytes through `Arena`. `satisfy` is `alloc_exact`,
//!   so it can refuse even once enough bytes are free, when they are scattered rather than one
//!   contiguous run. That forces victims to be destroyed as they are claimed, so a walk that comes
//!   up short has spent them and still fails the SET.
//! - `[Tiered]` — `claim_disk_victims` claims `nvme-maxmemory` budget through `DiskLedger`.
//!   `satisfy` compares a running total and never refuses, so victims are evicted only once the
//!   claims cover the request: a walk that comes up short evicts nothing. The bytes are settled
//!   later, by the write task.
//!
//! # Search bound
//!
//! `eviction-tenacity` maps to a time limit, after core's `maxmemory-eviction-tenacity`. Tenacity
//! 100 removes the clock, so a walk also stops after `MAX_STEPS_WITHOUT_LO_KEY` scan steps that
//! find no `LO` key (foreign keys), `unclaimable_rounds_limit` rounds that claim nothing (`LO` keys
//! that are pinned or otherwise unclaimable), or `MAX_SATISFY_REFUSALS` refusals. The round limit
//! is not in core; the module API's sampling makes it necessary, and it can fail a SET when
//! sampling never finds the module's keys.
//!
//! The clock is only consulted once `MIN_CANDIDATES` have been offered, so where `LO` keys are
//! sparse among other types a walk can run past it, by up to `MAX_STEPS_WITHOUT_LO_KEY` scan steps
//! per candidate still needed. That is accepted for mixed keyspaces.
//!
//! # Victim ranking
//!
//! As in core: draw `maxmemory-samples` candidates, score each under the active policy (idleness
//! for LRU and LFU, soonest expiry for `volatile-ttl`, none for the random policies), claim the
//! best first. The module API forces two differences:
//!
//! - Candidates come from a resumable `RM_Scan` cursor, not a random draw, because `RANDOMKEY` via
//!   `ctx.call` costs a command dispatch per sample.
//! - The scan visits every key, not just `LO` keys, so a mostly non-module keyspace spends its
//!   budget on misses and `MAX_STEPS_WITHOUT_LO_KEY` ends the walk rather than hunting.
//!
//! The budgets are node-wide, so a walk draws from every DB that has keys: one bucket of each in
//! turn, ranked together.
//!
//! # Evicting a victim
//!
//! In cluster mode core resolves every key lookup made during a command to that command's own
//! slot, so a victim in another slot cannot be opened or deleted from here: `delete` reports
//! success and the key survives with its data freed. So a victim is first tombstoned (see
//! `tombstone`), then its key is deleted inline if the lookup can see it, which it always can
//! standalone or in the request's slot. Otherwise `arm_sweep` schedules a timer, where no command
//! is executing, and `sweep` deletes the key there.
//!
//! Until then a tombstoned key still shows in commands that do not read the value (`EXISTS`,
//! `DBSIZE`, `KEYS`, `SCAN`).
//!
//! # Threading
//!
//! Selection runs only on the event-loop thread and never awaits, so every drop returns its memory
//! inside the call stack; `alloc_by_evicting` relies on that to retry its allocation at once. A SET
//! allocates before spawning its tokio task and a GET decides promotion before spawning, so this
//! holds for every mode and transport.
//!
//! A pin is any extra `Arc` on an object: an in-flight GET from dispatch until its transfer
//! completes, or a COPY reading its source. Only `Dram` + TCP completes a GET on the event loop
//! (`engine::execute_get`), so a walk never sees that one mid-flight. A pinned object is skipped,
//! because claiming it would report bytes that do not come back until its holder is done. That is
//! safe in both directions: a new pin cannot appear between the check and the delete (making one
//! means dispatching a command), and a pin that disappears mid-walk only costs a candidate the
//! next round re-offers.

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

/// Objects evicted since module load. The counters below are all exposed via `INFO largeobj`.
pub static EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Arena bytes the victims held: exact as a count, says nothing about placement.
pub static RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// SETs that still failed after eviction ran — objects destroyed for nothing. Rising against
/// `EVICTIONS_TOTAL` means thrashing: raise `dram-maxmemory` or look at fragmentation.
pub static EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Objects a walk passed over because someone still held them. Rising alongside a failure
/// counter says the working set is busy rather than full.
pub static PINNED_SKIPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Disk counterparts, separate statics because `INFO` reports them under the NVMe section.
/// `PINNED_SKIPS_TOTAL` is shared — a pin means the same thing in both walks.
pub static DISK_EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Exact, unlike the DRAM figure: a victim's whole `disk_len` is what its handle owes and what
/// dropping it credits back.
pub static DISK_RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Tiered SETs that still could not reserve after destroying something.
pub static DISK_EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Times `Budget::satisfy` refused a request the walk had fully freed for. Each one is bytes that
/// are free but not usable together, so a rate here is fragmentation rather than scarcity.
pub static SATISFY_REFUSALS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Walks abandoned at `MAX_SATISFY_REFUSALS`: eviction cannot make the allocation work, and
/// retrying upstream will not either.
pub static FRAGMENTATION_ABORTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Candidates a walk always draws before the clock may stop it, so tenacity 0 means a 0µs
/// budget rather than zero work. Core has the same floor for the same reason (`evict.c:578`).
const MIN_CANDIDATES: usize = 16;

/// Consecutive claim-nothing rounds tolerated at tenacity 0..=19, doubling every
/// `UNCLAIMABLE_TENACITY_STEP` points above that. 8 at the default tenacity of 10.
const UNCLAIMABLE_ROUNDS_BASE: usize = 8;

/// Tenacity points per doubling of `unclaimable_rounds_limit`. 20 gives six settings across
/// 0..=100, ending at 256 rounds.
const UNCLAIMABLE_TENACITY_STEP: i64 = 20;

/// Consecutive `scan_step`s that yield no `LO` key before the walk stops looking. Bounds a walk
/// over a mostly non-module keyspace, including at tenacity 100.
const MAX_STEPS_WITHOUT_LO_KEY: usize = 256;

/// Times `Budget::satisfy` may refuse a fully-funded request before the walk stops. Only `Arena`
/// refuses (bytes free but not contiguous). A refusal resets the credit, so every round still
/// claims something and neither `unclaimable_rounds` nor a clock would end the walk.
const MAX_SATISFY_REFUSALS: usize = 3;

// Per-DB `RM_Scan` cursors, resumed across walks. Thread-local because every entry point is
// main-thread only; another thread would walk independently, which is unfair but not a race. One
// per DB because `RM_Scan` walks `ctx->client->db` only. Cursors survive writes, so deleting
// victims mid-walk cannot make one skip a key that was present throughout.
thread_local! {
    static CURSORS: RefCell<HashMap<c_int, Rc<ScanCursor>>> = RefCell::new(HashMap::new());
}

/// A resumable `RM_Scan` cursor. Owned here rather than the crate's `KeysCursor` because the scan
/// callback needs the raw key handle (see `collect`).
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

/// The resumable cursor for `db`. The `Rc` is cloned out so the walk, which deletes keys and may
/// re-enter, never runs inside the `RefCell` borrow.
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

/// Point `ctx` at `db`, where `RM_Scan` and `RM_OpenKey` look. Raw FFI because the crate wraps
/// neither `RM_SelectDb` nor `RM_GetSelectedDb`. On a command's context this moves the client
/// itself, so entry points put it back with `restoring_db`.
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

/// Every DB that has keys, with its cursor. `RM_SelectDb` refuses an id past the last DB, which
/// is how the end of the list is found.
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

/// Consecutive claim-nothing rounds a walk tolerates before calling the keyspace unhelpful.
///
/// The only stop when every `LO` key is pinned at tenacity 100: scans keep finding keys, so
/// `MAX_STEPS_WITHOUT_LO_KEY` resets, and there is no clock. It scales with tenacity because it is
/// a confidence threshold: with a fraction `p` of keys unclaimable, a walk quits spuriously with
/// probability `p^(samples * rounds)` — ~10% at 95% pinned and 5 samples with 8 rounds, ~2e-29
/// with 256.
fn unclaimable_rounds_limit(tenacity: i64) -> usize {
    UNCLAIMABLE_ROUNDS_BASE << (tenacity.clamp(0, 100) / UNCLAIMABLE_TENACITY_STEP)
}

/// `Duration::MAX` cannot be added to an `Instant`, so `None` is the deadline that never arrives.
fn deadline_from_now(limit: Duration) -> Option<Instant> {
    Instant::now().checked_add(limit)
}

// ─── Shared: candidate supply ────────────────────────────────────────────────

/// A sampled key and what ranking needs to know about it.
///
/// Everything is read off the scan's own key handle: reopening by name would miss for a key
/// outside the running command's slot (see the module docs) and score it as the hottest.
struct Candidate {
    db: c_int,
    name: Vec<u8>,
    object_id: ObjectId,
    /// `RM_GetLRU`: idle milliseconds, or -1 under an LFU policy.
    idle_ms: i64,
    /// `RM_GetLFU`: the access frequency, or -1 unless the policy is LFU.
    freq: i64,
    /// `RM_GetExpire`: milliseconds to live, or `REDISMODULE_NO_EXPIRE`.
    ttl: i64,
    /// Tiered mode: the object's file. Holding it lets a claim test for a reader and reach the
    /// file without opening the key.
    file: Option<Arc<ObjectFile>>,
}

/// The keyspace walk as an iterator: `LO` keys from where each DB's cursor last stopped, one
/// bucket of each populated DB in turn, wrapping while the budget allows.
///
/// `MAX_STEPS_WITHOUT_LO_KEY` bounds the search for candidates, even at tenacity 100. `deadline`
/// bounds the walk, but only once `MIN_CANDIDATES` have been offered: a zero limit would yield
/// nothing.
struct Candidates<'a> {
    ctx: &'a Context,
    /// The DBs that had keys when the walk began.
    dbs: Vec<(c_int, Rc<ScanCursor>)>,
    next_db: usize,
    /// One bucket's worth of keys from the last `scan_step`.
    batch: std::vec::IntoIter<Candidate>,
    /// `None` when the budget is unbounded.
    deadline: Option<Instant>,
    yielded: usize,
    steps_without_lo_key: usize,
    /// Set once a stop condition fires. The batch in hand is still drained.
    stop: bool,
}

impl<'a> Candidates<'a> {
    fn new(ctx: &'a Context, deadline: Option<Instant>) -> Self {
        let dbs = populated_dbs(ctx);
        Self {
            ctx,
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
    type Item = Candidate;

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
            let (mut found, at_end) = scan_step(self.ctx, db, &cursor);
            if at_end {
                cursor.restart();
            }
            // An evicted object whose key is awaiting the sweep has nothing left to give.
            found.retain(|candidate| !tombstone::contains(candidate.object_id));
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

/// `RM_Scan` callback: record the key if it is one of ours.
///
/// Raw FFI because the crate wraps the handle in a `ValkeyKey` whose raw pointer is `pub(crate)`,
/// which hides the LRU, LFU and TTL getters. The handle is built from the dictionary entry, so
/// it works for a key in any slot.
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
    // The scan is keyspace-wide, so other modules' keys pass through. The type comparison is a
    // real guarantee: `RM_CreateDataType` refuses duplicate names.
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
///
/// Victims are collected rather than acted on in the callback: `RM_Scan` permits deleting only
/// the key being visited, and we may delete a whole bucket. The candidates own their data (a
/// copied name, a counted file), so they outlive the callback.
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
/// is a better victim.
///
/// `None` means not a candidate: under `volatile-*`, a key with no TTL. Core samples
/// `db->expires` there; we sample the whole keyspace and must exclude it ourselves.
///
/// For LRU and LFU, no second policy read is needed. `VM_GetLRU` writes -1 under an LFU policy
/// and `VM_GetLFU` under an LRU one (`module.c:14731-14758`), and -1 is unreachable for a live
/// value, so taking the non-negative one *is* `objectGetIdleness` (`lrulfu.c:162-171`): idle
/// milliseconds, or `UINT8_MAX - freq`. Trust the sentinel, not the return code.
///
/// Sampling stamps nothing: the scan involves no lookup, so `val->lru` stays as the last real
/// access left it. A touch would make LO keys look hot to core's `performEvictions` and push it
/// toward other keys. Reading the LFU frequency does write back the decayed counter
/// (`object.c:1677-1680`), but that is decay, as in core; there is no read-only sample.
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

/// The O(1) refusals every budget makes before walking: `need` above the whole budget can never
/// be served, and nothing resident leaves nothing to evict, which a walk could only learn by
/// touching every key (core asks `kvstoreSize`, `evict.c:482`). A zero `ceiling` means unlimited.
fn cannot_be_satisfied(need: u64, ceiling: u64, resident: u64) -> bool {
    (ceiling != 0 && need > ceiling) || resident == 0
}

// ─── Shared: the driver ──────────────────────────────────────────────────────

/// What a walk does with a victim, and what it is trying to produce. `walk` owns the rest of the
/// search.
trait Budget {
    type Output;

    /// Freed bytes count at this percentage of face value. It only decides when to first ask
    /// `satisfy`, which stays the authority, so it is a discount for a budget whose credit
    /// overstates what the asker can use and 100 where it is exact.
    const CREDIT_PERCENT: u64;

    /// Take the object's bytes, at face value. `None` leaves it alone: pinned (the impl bumps
    /// `PINNED_SKIPS_TOTAL`), already gone, or not convertible into this budget.
    ///
    /// Evicting is the impl's business: `Arena` must now, since `satisfy` cannot answer until the
    /// bytes are free; `DiskLedger` only records. Hence the candidate by value, so a deferring impl
    /// keeps its name and DB.
    fn claim(&mut self, ctx: &Context, candidate: Candidate) -> Option<u64>;

    /// The authoritative check, asked once the discounted credit says it is worth asking. `Some`
    /// ends the walk; `None` keeps going, and for the arena means the bytes are free but unusably
    /// placed.
    fn satisfy(&mut self, need: u64) -> Option<Self::Output>;
}

/// Sample, score, claim, repeat until `budget` is satisfied or a stop fires. Returns the output and
/// how many objects were evicted, so callers can tell a failed run from a no-op one.
///
/// The final `satisfy` can succeed with the request already paid for: a refusal resets the credit,
/// so a walk can free well over `need` and still end below the threshold, and the victims freed
/// since may have coalesced into the run the allocator wanted. Core does the same at `cant_free`.
fn walk<B: Budget>(ctx: &Context, need: u64, budget: &mut B) -> (Option<B::Output>, usize) {
    // One read each, so a walk is internally consistent against a runtime-modifiable config.
    let tenacity = crate::eviction_tenacity();
    let deadline = deadline_from_now(search_time_limit(tenacity));
    let unclaimable_limit = unclaimable_rounds_limit(tenacity);
    let samples = crate::maxmemory_samples();
    let policy = Policy::current(ctx);
    let mut candidates = Candidates::new(ctx, deadline);

    let mut credit = 0u64;
    let mut victims = 0usize;
    let mut examined = 0usize;
    let mut unclaimable_rounds = 0usize;
    let mut refusals = 0usize;

    loop {
        // Checked per round, not per candidate: a round is bounded work.
        if examined >= MIN_CANDIDATES && candidates.out_of_time() {
            break;
        }

        let round: Vec<Candidate> = candidates.by_ref().take(samples).collect();
        if round.is_empty() {
            break; // out of time, or nothing left to find
        }

        // Best victim first; a key we cannot claim costs the next-best, not the walk.
        let offered = round.len();
        let mut ranked: Vec<(i64, Candidate)> = round
            .into_iter()
            .filter_map(|candidate| {
                victim_score(&candidate, policy).map(|score| (score, candidate))
            })
            .collect();
        ranked.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
        // Policy-excluded keys cost a sample too, so they count toward the `MIN_CANDIDATES` floor;
        // otherwise persistent keys under `volatile-*` would run to `unclaimable_limit` with no
        // deadline.
        examined += offered - ranked.len();

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
            // Free but unusably placed. Restarting the credit lets the next victims coalesce, but
            // repeated refusals with the whole request freed each time are fragmentation, which
            // more victims will not fix.
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

    // Bound by what emptying the arena could produce, not the config: eviction cannot add
    // segments, and `alloc_exact` may split a request across them. The residency arm also covers
    // an arena full of in-flight SET buffers no key points at yet.
    let (live_segments, _, _) = dram_pool.segment_counts();
    let achievable = (live_segments * crate::dram_segment_size()) as u64;
    if cannot_be_satisfied(need as u64, achievable, dram_pool.object_count() as u64) {
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

    // Exact as a byte count but not as a placement, and a doomed multi-chunk `alloc_exact`
    // allocates N−1 chunks before failing (`segment_pool.rs:143-151`). The discount spares those.
    const CREDIT_PERCENT: u64 = 90;

    fn claim(&mut self, ctx: &Context, candidate: Candidate) -> Option<u64> {
        // An in-flight request holds this object; the bytes free only when its holder drops the
        // last reference.
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
    // the key is deleted, possibly from the lazyfree thread. A scanned key is still in the
    // keyspace, so this is present and only this thread can pin it.
    let obj_ctx = dram_pool.get_object(&candidate.object_id)?;
    let bytes = arena_bytes(&obj_ctx);

    evict(ctx, candidate.db, &candidate.name, candidate.object_id);

    // The bytes must be free now: the retry runs before the lazyfree thread could get to them.
    // Dropping the map's reference, and ours at scope end, runs `ObjectContext::Drop` ->
    // `dram_pool.free`, a talc coalesce with no syscall.
    dram_pool.remove_object(&candidate.object_id);

    RECLAIMED_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
    Some(bytes)
}

/// The arena bytes an object holds: the buffers' aligned `len`, which is what freeing gives
/// back, rather than the user length it was asked for.
fn arena_bytes(obj_ctx: &crate::storage::context::ObjectContext) -> u64 {
    obj_ctx.buffers.iter().map(|b| b.len as u64).sum()
}

// ─── Tombstones ──────────────────────────────────────────────────────────────

pub mod tombstone {
    //! Tombstones — evicted objects whose keys are still in the keyspace, waiting for `sweep`.
    //!
    //! Eviction frees an object's bytes inline but cannot always delete its key (see the module
    //! docs), so the object id is tombstoned instead: every read of a `LoValue` treats a
    //! tombstoned object as absent (`LoValue::is_tombstoned`) until the sweep deletes the key.
    //!
    //! Keyed by object id, not key name: the key can be renamed or moved before the sweep, and a
    //! SET that overwrites it mints a new id, which must not inherit the tombstone. The recorded
    //! location is a hint the sweep verifies.
    //!
    //! An entry leaves when the sweep deletes the key or the value is freed by any other route
    //! (`DEL`, overwrite, expiry, flush — `lo_free` calls `remove`).

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

    /// `TOMBSTONES.len()`, readable without the lock. Every `BLOB.GET` asks `is_tombstoned`, and on a
    /// node that has nothing tombstoned (every standalone node, in practice) that should cost a
    /// load.
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

    /// Drop the entry for `object_id`, if any: its key is gone. Any thread — `lo_free` runs on a
    /// lazyfree thread.
    pub fn remove(object_id: ObjectId) {
        if COUNT.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut table = table();
        table.remove(&object_id);
        COUNT.store(table.len(), Ordering::Release);
    }

    /// Every entry, for the sweep to work through without holding the lock across keyspace calls.
    pub fn snapshot() -> Vec<(ObjectId, Location)> {
        table().iter().map(|(id, at)| (*id, at.clone())).collect()
    }

    /// How many objects are tombstoned.
    pub fn len() -> usize {
        COUNT.load(Ordering::Acquire)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// The table is process-wide, so the cases share one test and distinct ids.
        #[test]
        fn a_tombstone_lasts_until_the_object_is_removed() {
            let (a, b) = (ObjectId(u64::MAX - 101), ObjectId(u64::MAX - 102));
            assert!(!contains(a));

            add(a, 3, b"key");
            assert!(contains(a), "tombstoned by id");
            assert!(
                !contains(b),
                "another object, even under the same name, is not"
            );
            assert!(snapshot()
                .iter()
                .any(|(id, at)| *id == a && at.db == 3 && at.name == b"key"));

            remove(b);
            assert!(contains(a), "removing another object leaves the tombstone");
            remove(a);
            assert!(!contains(a));
            remove(a);
            assert!(!contains(a), "removing twice is harmless");
        }
    }
}

// ─── Evicting a key ──────────────────────────────────────────────────────────

/// Evict the object `object_id`, which the key `name` in `db` holds: tombstone it first, then
/// delete the key if the lookup can see it (which also removes the tombstone), else arm the sweep.
fn evict(ctx: &Context, db: c_int, name: &[u8], object_id: ObjectId) {
    tombstone::add(object_id, db, name);
    if !delete_if_present(ctx, db, name, object_id) {
        arm_sweep(ctx);
    }
}

/// Delete the key `name` in `db` if it still holds `object_id`, and drop that tombstone. `false`
/// if it does not: absent, overwritten under that name, or (in a cluster command) in a slot this
/// lookup cannot see.
///
/// The id is checked because the name may have been overwritten since. `Key::delete` returns `Ok`
/// unconditionally, so this is also the only way to know whether anything was deleted.
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

/// Whether a sweep is already scheduled, so a burst of evictions arms one timer, not one each.
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
/// Each entry's location is checked, not trusted. One whose key is not there (renamed or moved)
/// is found by object id through a scan of every DB — the reverse lookup. It is slow but rare: it
/// takes a RENAME or MOVE in the few milliseconds a key lingers.
///
/// An object no scan finds has no key, so its entry is dropped. That is how an entry whose value
/// is still being freed by the lazyfree thread goes away.
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

/// Find the keys holding `strays` by scanning every DB, delete them, and remove each object found
/// from `strays`. A fresh cursor per DB leaves the walk's own undisturbed.
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
        // Deleted once the scan is over: `RM_Scan` permits deleting the key being visited, not
        // others.
        for candidate in found {
            if delete_if_present(ctx, db, &candidate.name, candidate.object_id) {
                strays.remove(&candidate.object_id);
            }
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
/// request can select them or spend those bytes, which lets the write task settle without a
/// second capacity check (see `DiskReservation`). The returned handles are the keyspace's own,
/// shared rather than taken, because the key may be in a slot the running command cannot reach;
/// the reservation `release`s them when the write settles.
///
/// Main thread only, before the SET's tokio task is spawned.
pub fn claim_disk_victims(ctx: &Context, need: u64) -> Option<Vec<Arc<ObjectFile>>> {
    let budget = crate::nvme_maxmemory();

    // An unlimited budget cannot refuse a reservation, so nothing asks for victims. Fail loudly:
    // evicting to satisfy a cap that does not exist would be the worst way to find out.
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

        // Claims only record, so nothing was lost: this counts wasted search. A rise against
        // `disk_evictions_total` means the working set is too pinned or too large.
        if victims > 0 {
            DISK_EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        None
    })
}

/// Accumulates claimed handles until they cover `need`. The running total is exact, so `satisfy`
/// is a comparison and never refuses. Each claim carries its DB and name because the key is dealt
/// with only once `satisfy` succeeds.
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

    // The ledger's total is exact and has no placement, so anything less over-evicts.
    const CREDIT_PERCENT: u64 = 100;

    fn claim(&mut self, _ctx: &Context, candidate: Candidate) -> Option<u64> {
        let file = candidate.file?;
        // A key the scan cursor offers twice is claimed once; left to the pin check below, its own
        // claim would make it look pinned and inflate the skip count.
        if self
            .claimed
            .iter()
            .any(|claim| claim.file.object_id() == file.object_id())
        {
            return None;
        }
        // An `Arc` beyond the keyspace's and this candidate's is a reader, whose reference holds
        // the file's blocks past the unlink: claiming it would report budget we never receive.
        // Nothing is written to the value, so a pinned one is undisturbed (COPY holds its source
        // through a `&LoValue` while the walk runs).
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
/// request, so this is where eviction becomes irreversible. The handles ride on the reservation,
/// so the `unlink(2)` and the credit stay off the event loop and land just before the new file is
/// created.
fn evict_claimed(ctx: &Context, claimed: Vec<Claim>) -> Vec<Arc<ObjectFile>> {
    let dram_pool = crate::storage::get_dram_pool();
    claimed
        .into_iter()
        .map(|claim| {
            let object_id = claim.file.object_id();
            evict(ctx, claim.db, &claim.name, object_id);
            // The DRAM-resident copy of a promoted object, which `lo_free` would have dropped.
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

    fn candidate(idle_ms: i64, freq: i64, ttl: i64) -> Candidate {
        Candidate {
            db: 0,
            name: b"k".to_vec(),
            object_id: ObjectId(u64::MAX - 201),
            idle_ms,
            freq,
            ttl,
            file: None,
        }
    }

    const NO_TTL: i64 = raw::REDISMODULE_NO_EXPIRE as i64;

    #[test]
    fn policy_names_pick_a_ranking_and_a_scope() {
        let cases = [
            ("allkeys-lru", Ranking::Idleness, false),
            ("allkeys-lfu", Ranking::Idleness, false),
            ("volatile-lru", Ranking::Idleness, true),
            ("volatile-lfu", Ranking::Idleness, true),
            ("volatile-ttl", Ranking::Ttl, true),
            ("allkeys-random", Ranking::Random, false),
            ("volatile-random", Ranking::Random, true),
            ("noeviction", Ranking::Idleness, false),
            ("", Ranking::Idleness, false),
        ];
        for (name, ranking, volatile_only) in cases {
            assert_eq!(
                Policy::parse(name),
                Policy {
                    ranking,
                    volatile_only
                },
                "{name}"
            );
        }
    }

    /// Each policy has to order the same three keys its own way, and `volatile-*` has to refuse
    /// the one with no TTL instead of scoring it.
    #[test]
    fn each_policy_scores_what_it_ranks_by() {
        let (idle, busy) = (candidate(9_000, -1, NO_TTL), candidate(10, -1, NO_TTL));
        let lru = Policy::parse("allkeys-lru");
        assert!(
            victim_score(&idle, lru) > victim_score(&busy, lru),
            "idler first"
        );

        let (rare, common) = (candidate(-1, 2, NO_TTL), candidate(-1, 200, NO_TTL));
        let lfu = Policy::parse("allkeys-lfu");
        assert!(
            victim_score(&rare, lfu) > victim_score(&common, lfu),
            "rarer first"
        );

        let (soon, later) = (candidate(10, -1, 1_000), candidate(9_000, -1, 60_000));
        let ttl = Policy::parse("volatile-ttl");
        assert!(
            victim_score(&soon, ttl) > victim_score(&later, ttl),
            "soonest expiry first, whatever the idle time"
        );
        assert_eq!(
            victim_score(&candidate(10, -1, NO_TTL), ttl),
            None,
            "no TTL, no candidate"
        );
        assert_eq!(
            victim_score(&candidate(10, -1, 0), ttl),
            Some(i64::MAX),
            "already expired"
        );

        let random = Policy::parse("allkeys-random");
        assert_eq!(
            victim_score(&idle, random),
            victim_score(&busy, random),
            "no order"
        );
        assert_eq!(
            victim_score(&candidate(10, -1, NO_TTL), Policy::parse("volatile-lru")),
            None
        );
        assert_eq!(
            victim_score(&candidate(10, -1, NO_TTL), Policy::parse("volatile-random")),
            None
        );
    }

    /// Each arm is a refusal that saves the keyspace from a request no amount of work could serve.
    #[test]
    fn hopeless_request_refuses_only_what_a_walk_could_not_serve() {
        assert!(
            cannot_be_satisfied(2048, 1024, 10),
            "larger than the whole budget"
        );
        assert!(
            cannot_be_satisfied(64, 1024, 0),
            "nothing resident to evict"
        );
        assert!(
            !cannot_be_satisfied(2048, 0, 10),
            "a zero ceiling is unlimited, not a bound of zero"
        );
        assert!(
            !cannot_be_satisfied(64, 1024, 10),
            "fits, and something is here"
        );
    }

    /// Both halves of the search bound, over core's curve. The endpoints matter most: 0 must not
    /// mean "unbounded" and 100 must.
    #[test]
    fn tenacity_bounds_the_search() {
        assert_eq!(
            search_time_limit(0),
            Duration::ZERO,
            "no time, but see MIN_CANDIDATES"
        );
        assert_eq!(
            search_time_limit(10),
            Duration::from_micros(500),
            "the default"
        );
        assert_eq!(
            search_time_limit(11),
            Duration::from_micros(575),
            "15% per point past the ramp"
        );
        assert_eq!(
            search_time_limit(100),
            Duration::MAX,
            "tenacity 100 has no clock"
        );
        assert!(
            search_time_limit(50) > search_time_limit(20)
                && search_time_limit(20) > search_time_limit(10),
            "monotone, so raising the knob can only buy more search"
        );
        assert_eq!(unclaimable_rounds_limit(10), 8, "the default");
        assert_eq!(
            unclaimable_rounds_limit(0),
            8,
            "tenacity 0 is bounded by the clock, not this"
        );
        assert_eq!(
            unclaimable_rounds_limit(20),
            16,
            "one doubling per 20 points"
        );
        assert_eq!(
            unclaimable_rounds_limit(100),
            256,
            "no clock at 100, so this is the only stop"
        );
        assert!(
            unclaimable_rounds_limit(-5) == unclaimable_rounds_limit(0)
                && unclaimable_rounds_limit(500) == unclaimable_rounds_limit(100),
            "clamped, so an out-of-range config cannot shift the whole curve"
        );
        // `Duration::MAX` cannot be added to an `Instant`; handling that is what keeps tenacity 100
        // the loosest setting rather than a deadline in the past.
        assert!(
            deadline_from_now(search_time_limit(100)).is_none(),
            "no deadline at all, not a saturated one"
        );
        let at = deadline_from_now(search_time_limit(10)).expect("a finite budget has a deadline");
        assert!(at > Instant::now(), "and it is in the future");
    }
}
