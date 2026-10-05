//! Eviction — evicting resident objects to free a budget.
//!
//! # One driver
//!
//! `walk` is the entire policy: how long to search, which candidates to sample, how to rank them,
//! what to skip, when to give up. Both modes go through it, so victim selection cannot drift apart
//! between them, and both stay aligned with core's policy.
//!
//! What `walk` does not decide is what a freed byte *is*. That is where the two modes fork: each
//! passes in a `Budget` impl with two methods. `claim` takes one victim and reports the bytes it
//! yielded; `satisfy` says whether that is enough yet and hands back the result.
//!
//! # Two budgets
//!
//! A node runs in one mode for its lifetime, so it only ever takes one of these. Promotion and
//! demotion between the tiers do not go through here.
//!
//! `[Dram]` — `alloc_by_evicting`, from `engine::alloc_dram_or_make_room`, frees DRAM arena bytes
//! through `Arena`. Its `satisfy` is `alloc_exact`: asking *is* allocating, so it can refuse even
//! once enough bytes are free, when they are scattered instead of in one contiguous run. That
//! forces everything else. Victims must be destroyed as they are claimed, because the memory has to
//! be genuinely free before the allocator can answer — so a walk that comes up short has spent them
//! and still fails the SET. Pinned objects are skipped (see Threading). The bytes come back in our
//! own drop, inside this call stack.
//!
//! `[Tiered]` — `claim_disk_victims`, from `engine::reserve_nvme_or_make_room`, claims
//! `nvme-maxmemory` budget through `DiskLedger`. Its `satisfy` compares a running total against the
//! request and can never refuse, so the deletes can wait until the claims cover it: a walk that
//! comes up short evicts nothing. A pinned file keeps its blocks past the unlink, so it is skipped
//! too. The bytes are settled later, by the write task.
//!
//! # Search bound
//!
//! Time based search bound. Module's `eviction-tenacity` maps to a microsecond budget and is
//! inspired by core Valkey's `maxmemory-eviction-tenacity`. If tenacity reaches 100 there is
//! no time based timeout, so `barren_rounds` checks for consecutive sample rounds that claimed
//! nothing in order to exit. `barren_rounds` is not native to core Valkey and is necessary due
//! to the sampling limitations of the module API. It can cause error replies in cases where
//! eviction fails to sample items belonging to the module.
//!
//! # Victim ranking
//!
//! Valkey Core evictions are recreated within the module. Draw `maxmemory-samples` candidates,
//! score each with `objectGetIdleness` (based on eviction policy), claim best victims.
//!
//! Two limitations due to module API restrictions:
//!
//! - **Candidates come from a resumable `RM_Scan` cursor, not a random draw**, because `RANDOMKEY`
//!   via `ctx.call` costs a command dispatch per sample.
//! - **The scan visits every key, not just `LO` keys**, so a keyspace that is mostly non-module
//!   keys spends its budget on misses and `MAX_EMPTY_STEPS` gives up rather than hunting.
//!
//! Like core, a walk draws from every DB that has keys, whichever DB the request came from: the
//! budgets are node-wide. It scans one bucket of each in turn, and victims are ranked together.
//!
//! # Evicting a victim
//!
//! Core resolves every key lookup made while a command executes to that command's own slot, so in
//! cluster mode a victim in another slot cannot be opened or deleted from here: `delete` reports
//! success and the key survives with its data freed. A victim is therefore tombstoned by object
//! id first, and every read of the value treats a tombstoned object as a miss. Then its key
//! is deleted inline if that works, which it always does when standalone or in the request's slot.
//! Otherwise `arm_sweep` schedules a timer, where no command is executing, and `sweep` deletes the
//! key there, scanning every DB for the object id if the key was renamed or moved since.
//!
//! Until the sweep runs, a tombstoned key is still visible to core commands that do not read the
//! value (`EXISTS`, `DBSIZE`, `KEYS`, `SCAN`).
//!
//! # Threading
//!
//! Eviction selection runs only on the Valkey event-loop thread, and never awaits, so every drop
//! here returns its memory inside this call stack rather than on a later thread. `alloc_by_evicting`
//! depends on that: it frees victims and immediately retries the allocation.
//!
//! That holds for every mode and transport: a SET allocates before spawning its tokio task, and a
//! GET decides promotion before spawning, so no eviction ever runs off the event loop.
//!
//! A pin is any extra `Arc` on an object: an in-flight GET, from dispatch until its transfer
//! completes, or a COPY reading its source. `Dram` + TCP is the one GET that completes on
//! the event loop (`engine::execute_get`), so a walk never sees it mid-flight; the other three hand
//! the `Arc` to a tokio thread. A pinned object is skipped: claiming it would report bytes that do
//! not come back until its holder is done.
//!
//! Skipping is safe in both directions because we hold the event loop. A new pin cannot arrive
//! between the check and the delete, since making one means dispatching a command. A pin that
//! *disappears* mid-walk only costs us a candidate the next round re-offers.

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

/// Objects destroyed since module load. All of these are exposed via `INFO largeobj`.
pub static EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Arena bytes the victims held. Exact as a byte count; says nothing about placement, so
/// these bytes are in the arena but not necessarily in a run anyone can use.
pub static RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// SETs that still failed after eviction ran — objects destroyed for nothing. Rising against
/// `EVICTIONS_TOTAL` means thrashing: raise `dram-maxmemory` or look at fragmentation.
pub static EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Objects a walk passed over because someone still held them. Rising alongside a failure
/// counter says the working set is busy rather than full — those bytes come back on their own.
pub static PINNED_SKIPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Disk counterparts, separate statics because `INFO` reports them under the NVMe section.
/// `PINNED_SKIPS_TOTAL` is shared — a pin means the same thing in both walks.
pub static DISK_EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Exact, unlike the DRAM figure: a victim's whole `disk_len` is what its handle owes and what
/// dropping it credits back.
pub static DISK_RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Tiered SETs that still could not reserve after destroying something.
pub static DISK_EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Candidates a walk always draws before the clock may stop it, so tenacity 0 means a 0µs
/// budget rather than zero work. Core has the same floor for the same reason (`evict.c:578`).
const MIN_CANDIDATES: usize = 16;

/// Consecutive claim-nothing rounds tolerated at tenacity 0..=19, doubling every
/// `BARREN_TENACITY_STEP` points above that. 8 at the default tenacity of 10.
const BARREN_ROUNDS_BASE: usize = 8;

/// Tenacity points per doubling of `barren_rounds`. 20 gives six settings across 0..=100,
/// ending at 256 rounds.
const BARREN_TENACITY_STEP: i64 = 20;

/// Consecutive `scan_step`s that yield no `LO` key before the walk gives up looking. The scan is
/// keyspace-wide, so this is what bounds a walk over a keyspace that is mostly other people's
/// keys — including at tenacity 100, where there is no clock to fall back on.
const MAX_EMPTY_STEPS: usize = 256;

/// Times `Budget::satisfy` may refuse a fully-funded request before the walk gives up. Only
/// `Arena` can refuse (the bytes are free but not contiguous), and a refusal resets the credit,
/// so without this the walk has no stop at all in the case that matters: every round claims
/// something, so `barren` never rises, and at tenacity 100 there is no clock either.
const MAX_SATISFY_REFUSALS: usize = 3;

/// Freed bytes are credited at this percentage of face value, in both walks.
///
/// One constant for both budgets, and not 100. The discount only decides *when to first ask* —
/// `Budget::satisfy` is still the authority — and what it buys is fewer doomed attempts, which are
/// not free: a multi-chunk `alloc_exact` allocates the first N−1 chunks before it fails and frees
/// them again (`segment_pool.rs:143-151`).
///
/// It is a blunt instrument, because neither credit is imprecise in a way a percentage models. The
/// DRAM one is exact as a byte count and imprecise only in placement, and the NVMe ledger has no
/// placement dimension at all, so 90% there just deletes ~11% more keys than the cap requires.
const CREDIT_PERCENT: u64 = 90;

// Keyspace cursors, resumed across calls so a request continues the previous walk rather than
// restarting it. A thread-local rather than a lock because every entry point here is main-thread
// only; a second thread would get its own cursors and walk independently, which is a fairness
// regression rather than a data race.
//
// `RM_Scan` cursors survive writes — the reverse-binary bucket order that gives `SCAN` its
// guarantees survives rehashing — so deleting victims mid-walk cannot make the cursor skip a key
// that was present throughout.
//
// One cursor per DB, because `RM_Scan` walks `ctx->client->db` only: a single shared position
// applied to differently sized kvstores makes "every key offered once per pass" hold for neither.
thread_local! {
    static CURSORS: RefCell<HashMap<c_int, Rc<ScanCursor>>> = RefCell::new(HashMap::new());
}

/// A resumable `RM_Scan` cursor. Owned here rather than the crate's `KeysCursor` because the scan
/// callback needs the raw key handle it is given, which the crate does not pass on (see `collect`).
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

/// The resumable cursor for `db`. The `Rc` is cloned out of the map so the walk — which deletes
/// keys, and may re-enter through anything a notification handler does — never runs inside the
/// `RefCell` borrow.
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

/// Point `ctx` at `db`, which is where `RM_Scan` and `RM_OpenKey` look. Raw FFI because the crate
/// wraps neither `RM_SelectDb` nor `RM_GetSelectedDb`. On a command's context this moves the
/// calling client itself, so an entry point puts it back with `restoring_db`.
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

// ─── The search budget ───────────────────────────────────────────────────────

/// `eviction-tenacity` as a wall-clock budget, ported from `evictionTimeLimitUs`
/// (`evict.c:363-378`): 0..=10 linear to 500µs, then 15% per point, 100 unbounded.
fn search_budget(tenacity: i64) -> Duration {
    match tenacity {
        t if t <= 10 => Duration::from_micros(50 * t.max(0) as u64),
        t if t < 100 => Duration::from_micros((500.0 * 1.15f64.powi(t as i32 - 10)) as u64),
        _ => Duration::MAX,
    }
}

/// Consecutive claim-nothing rounds a walk tolerates before calling the keyspace unhelpful.
///
/// The termination guard rather than the clock, and the only stop in the case that matters: a
/// keyspace of `LO` keys that are all pinned resets `MAX_EMPTY_STEPS` on every scan step and, at
/// tenacity 100, has no deadline either. It scales with tenacity because a rejection is a
/// property of the key, not of the supply — the cap is really a confidence threshold, since with
/// a fraction `p` of keys unclaimable a walk gives up spuriously with probability
/// `p^(samples * rounds)`. At 95% pinned and 5 samples that is ~10% per walk at 8 rounds and
/// ~2e-29 at 256.
fn barren_rounds(tenacity: i64) -> usize {
    BARREN_ROUNDS_BASE << (tenacity.clamp(0, 100) / BARREN_TENACITY_STEP)
}

/// `Duration::MAX` cannot be added to an `Instant`, which is the honest shape of "tenacity 100
/// has no clock": a deadline that never arrives, leaving the other stop conditions to terminate.
fn deadline_from_now(budget: Duration) -> Option<Instant> {
    Instant::now().checked_add(budget)
}

// ─── Shared: candidate supply ────────────────────────────────────────────────

/// A sampled key, the DB it lives in, and what ranking needs to know about it.
///
/// Everything here is read off the scan's own key handle, never by reopening the key by name.
/// In cluster mode a lookup made inside a command resolves to the slot of the command's own key,
/// so reopening a victim in any other slot would miss — scoring it as idle 0, the worst victim
/// there is, and leaving the walk nothing to claim it through.
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
    /// Tiered mode: the object's file. Holding it is what lets a claim test for a reader and
    /// reach the file without opening the key (see `DiskLedger::claim`).
    file: Option<Arc<ObjectFile>>,
}

/// The keyspace walk as an iterator: `LO` keys from wherever each DB's cursor stopped last time,
/// one bucket of each populated DB in turn, wrapping as often as the budget allows.
///
/// Two stop conditions, each with one job. `MAX_EMPTY_STEPS` bounds the *search* for candidates
/// and must hold even at tenacity 100, where no deadline ever arrives. `deadline` bounds the
/// *walk*, and only once `MIN_CANDIDATES` have been offered — without that gate a zero budget
/// would yield nothing at all.
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
    empty_steps: usize,
    /// Set once a stop condition fires. The batch in hand is still drained first — those
    /// keys were offered by the same scan that ended the walk.
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
            empty_steps: 0,
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
                self.empty_steps += 1;
            } else {
                self.empty_steps = 0;
            }

            if self.empty_steps >= MAX_EMPTY_STEPS
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
/// Why raw FFI: the handle the scan passes is built straight from the dictionary entry, so it
/// works for a key in any slot, where a lookup by name made inside a command does not. The
/// crate's trampoline wraps that handle in a `ValkeyKey` whose raw pointer is `pub(crate)`, so
/// the LRU, LFU and TTL getters are unreachable through it.
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
    // Scans are keyspace-wide and we may not be the only module loaded, so confirm the key
    // is ours. The type comparison is a real guarantee: `RM_CreateDataType` refuses duplicate
    // names and mints a fresh `moduleType` per call.
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

/// Advance `cursor` one step — one bucket, so zero to several keys — and return the `LO` keys it
/// visited plus whether it reached the end of the table.
///
/// Victims are collected rather than acted on in the callback because `RM_Scan` permits deleting
/// the key currently being visited but not others, and we may delete a whole bucket's worth.
/// Outliving the callback is sound because the candidates own their data: the name is copied and
/// the file is a counted reference.
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

/// Core's `objectGetIdleness` (`lrulfu.c:162-171`) over what the scan read, plus the one
/// eligibility rule the getters cannot express. Higher is a better victim: idle milliseconds
/// under an LRU policy, `UINT8_MAX - freq` under an LFU one.
///
/// **`None` means "not a candidate", which under `volatile-*` is a key with no TTL.** Core samples
/// `db->expires` there, so a persistent key is never a victim; we sample the whole keyspace and
/// have to exclude it ourselves. The policy itself is read once per walk, by the caller.
///
/// **No `maxmemory-policy` read is needed** — the getters are self-identifying. `VM_GetLRU`
/// returns OK but writes `-1` under an LFU policy (`module.c:14731-14737`), `VM_GetLFU` likewise
/// unless `lrulfu_isUsingLFU()` (`:14753-14758`), and `-1` is unreachable for a live value. So
/// "call both, take the non-negative" *is* `objectGetIdleness`. Never trust the return code, only
/// the sentinel. Units differ from core's seconds — a monotone transform of a score only ever
/// compared against itself.
///
/// **Nothing is stamped by sampling.** The scan hands over a handle without a lookup, so `val->lru`
/// stays what the last real access left it. That matters beyond our own ranking: it is what core's
/// `performEvictions` reads, so a sampling pass that touched LO keys would make them look hot and
/// push core's eviction toward everyone else's keys.
///
/// Reading the LFU frequency does still mutate: `objectGetLFUFrequency` writes the decayed
/// counter back (`object.c:1677-1680`). That is decay, not a touch, and core does the same while
/// sampling. There is no read-only sample.
fn idleness(candidate: &Candidate, volatile_only: bool) -> Option<i64> {
    if volatile_only && candidate.ttl == raw::REDISMODULE_NO_EXPIRE as i64 {
        return None;
    }
    Some(if candidate.freq >= 0 {
        i64::from(u8::MAX) - candidate.freq
    } else {
        candidate.idle_ms.max(0)
    })
}

/// The O(1) refusals every budget makes before walking. `need` above the whole budget can never
/// be served by emptying the node, and a budget with nothing resident has nothing to give — a
/// fact the walk could only learn by touching every key. Core answers the same question off
/// `kvstoreSize` (`evict.c:482`). A zero `ceiling` means unlimited.
fn hopeless_request(need: u64, ceiling: u64, resident: u64) -> bool {
    (ceiling != 0 && need > ceiling) || resident == 0
}

// ─── Shared: the driver ──────────────────────────────────────────────────────

/// What a walk does with a victim, and what it is trying to produce. One impl per budget;
/// everything else about the search is in `walk`.
trait Budget {
    type Output;

    /// Take the object's bytes, at face value. `None` leaves it alone — pinned, already gone, or
    /// not convertible into this budget. Impls own their own notion of a pin, and bump
    /// `PINNED_SKIPS_TOTAL` when that is why they declined.
    ///
    /// Giving the object up is the impl's business: `Arena` must do it now, since `satisfy` cannot
    /// answer until the bytes are really free; `DiskLedger` only records the claim and defers.
    /// Hence the candidate by value, so a deferring impl can keep its name and DB. `ctx` is already
    /// on the candidate's DB.
    fn claim(&mut self, ctx: &Context, candidate: Candidate) -> Option<u64>;

    /// The authoritative check, asked only once the discounted credit says it is worth
    /// asking. `Some` ends the walk successfully; `None` means keep going, and for the arena
    /// it means the bytes are free but unusably placed.
    fn satisfy(&mut self, need: u64) -> Option<Self::Output>;
}

/// Sample, score, claim, repeat until `budget` is satisfied or a stop condition fires. Returns
/// the output and how many objects were evicted, which callers need to tell a failed run from
/// a no-op one.
///
/// The final `satisfy` is reachable with the request already paid for. A refusal resets the
/// credit, so a walk can free well over `need` in total and still end below the threshold that
/// triggers an attempt — and the victims freed since that refusal may have coalesced into the run
/// the allocator wanted. Core does the same, re-checking `getMaxmemoryState` at `cant_free`.
fn walk<B: Budget>(ctx: &Context, need: u64, budget: &mut B) -> (Option<B::Output>, usize) {
    // One read each, so a walk is internally consistent against a runtime-modifiable config.
    let tenacity = crate::eviction_tenacity();
    let deadline = deadline_from_now(search_budget(tenacity));
    let barren_limit = barren_rounds(tenacity);
    let samples = crate::maxmemory_samples();
    let volatile_only = crate::volatile_policy(ctx);
    let mut candidates = Candidates::new(ctx, deadline);

    let mut credit = 0u64;
    let mut victims = 0usize;
    let mut examined = 0usize;
    let mut barren = 0usize;
    let mut refusals = 0usize;

    loop {
        // Once per round rather than per candidate: a round is bounded work and this is where
        // the next one would begin.
        if examined >= MIN_CANDIDATES && candidates.out_of_time() {
            break;
        }

        let round: Vec<Candidate> = candidates.by_ref().take(samples).collect();
        if round.is_empty() {
            break; // the supply gave up first — out of time, or nothing left to find
        }

        // Best victim first. Claiming in score order is what makes this a policy rather than an
        // ordering, and a key we could not claim costs us the next-best rather than the walk.
        let offered = round.len();
        let mut ranked: Vec<(i64, Candidate)> = round
            .into_iter()
            .filter_map(|candidate| {
                idleness(&candidate, volatile_only).map(|score| (score, candidate))
            })
            .collect();
        ranked.sort_unstable_by_key(|(score, _)| std::cmp::Reverse(*score));
        // A key the policy excluded cost a sample even though nothing claims it, so the clock's
        // `MIN_CANDIDATES` floor counts it — otherwise a keyspace of persistent keys under
        // `volatile-*` would walk to `barren_limit` with the deadline never armed.
        examined += offered - ranked.len();

        let mut claimed_any = false;
        for (_score, candidate) in ranked {
            examined += 1;
            select_db(ctx, candidate.db);
            let Some(bytes) = budget.claim(ctx, candidate) else {
                continue;
            };
            victims += 1;
            // A claim that yielded nothing is not progress, whatever it destroyed: crediting it
            // would reset `barren` and let the walk run until the keyspace is empty.
            claimed_any |= bytes > 0;
            credit += bytes * CREDIT_PERCENT / 100;
            if credit < need {
                continue;
            }
            if let Some(output) = budget.satisfy(need) {
                return (Some(output), victims);
            }
            // The bytes are free but unusably placed. Starting the credit over lets the next
            // victims coalesce into a run — but being refused this often with the whole request
            // freed each time is fragmentation, not scarcity, and more victims will not fix it.
            refusals += 1;
            if refusals >= MAX_SATISFY_REFUSALS {
                return (None, victims);
            }
            credit = 0;
        }

        barren = if claimed_any { 0 } else { barren + 1 };
        if barren >= barren_limit {
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

    // The bound is what emptying the arena could produce, not what the config permits: eviction
    // frees inside the segments that exist and cannot add one, and `try_expand` is separately
    // gated on the server watermark, so a pool may sit far below `dram-maxmemory` for good. Not
    // one segment either — `alloc_exact` may split a request across segments. The residency arm
    // also covers an arena full of in-flight SET buffers no key points at yet.
    let (live_segments, _, _) = dram_pool.segment_counts();
    let achievable = (live_segments * crate::dram_segment_size()) as u64;
    if hopeless_request(need as u64, achievable, dram_pool.object_count() as u64) {
        return None;
    }

    if !crate::eviction_allowed(ctx) {
        return None;
    }

    let mut arena = Arena;
    let (buffers, victims) = restoring_db(ctx, || walk(ctx, need as u64, &mut arena));

    // Count only runs that gave something up — they paid and got nothing.
    if buffers.is_none() && victims > 0 {
        EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    buffers
}

struct Arena;

impl Budget for Arena {
    type Output = Vec<SegmentBuffer>;

    fn claim(&mut self, ctx: &Context, candidate: Candidate) -> Option<u64> {
        // An in-flight request holds this object, so taking the entry would leave it gone and the
        // bytes still unavailable — the freeing drop belongs to whoever holds the last reference.
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

/// Give up the `LO` object behind `candidate` and report the arena bytes it freed. `None` if the
/// object is already gone, which is the walk's signal that nothing happened.
fn reclaim(ctx: &Context, candidate: &Candidate) -> Option<u64> {
    let dram_pool = crate::storage::get_dram_pool();

    // Take the reference, and so the size, *before* giving the object up: once the key is deleted
    // `lo_free` removes the entry, possibly from the lazyfree thread, so it may be gone by the time
    // we look. Crediting 0 there counts as progress while making none, which resets the walk's
    // termination guards.
    let obj_ctx = dram_pool.get_object(&candidate.object_id)?;
    let bytes = arena_bytes(&obj_ctx);

    evict(ctx, candidate.db, &candidate.name, candidate.object_id);

    // The bytes have to be free *now*: the retry that motivated this eviction runs before the
    // lazyfree thread could get to them. Dropping the map's reference here, and ours at the end of
    // this scope, runs `ObjectContext::Drop` -> `dram_pool.free` — a talc coalesce, no syscall.
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
    //! Tombstones — objects eviction has evicted whose keys are still in the keyspace.
    //!
    //! Eviction frees an object's bytes inline, because the SET that needed them is waiting, but it
    //! cannot always delete the object's key there. In cluster mode the core resolves every key
    //! lookup made while a command executes to the slot of that command's own key, so a victim in
    //! another slot is not found, and `delete` reports success without removing it. The key then
    //! outlives its data.
    //!
    //! A tombstone keeps that key safe to leave behind. The object id goes in this table, every
    //! read of a `LoValue` treats a tombstoned object as absent (`LoValue::is_tombstoned`), and
    //! `eviction::sweep` deletes the key later from a timer, where no command is executing and
    //! lookups resolve their own slots.
    //!
    //! Keyed by object id, not key name: the key can be renamed or moved before the sweep, and a
    //! SET that overwrites it mints a new id, which must not inherit the tombstone. The entry
    //! remembers where the key was when it was tombstoned, as a hint the sweep verifies rather
    //! than trusts.
    //!
    //! An entry leaves the table when the sweep deletes the key, or when the value is freed by any
    //! other route (`DEL`, overwrite, expiry, flush — `lo_free` calls `remove`).

    use std::collections::HashMap;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{LazyLock, Mutex};

    use crate::data_type::ObjectId;

    /// Where a tombstoned object's key was when it was tombstoned.
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

/// Evict the object `object_id`, which the key `name` in `db` holds: tombstone it, then try to
/// delete the key.
///
/// The tombstone comes first and is what makes eviction safe. The delete is best effort, because
/// in cluster mode a lookup from inside a command cannot find a key in another slot. Where it does
/// find the key the delete removes the tombstone with it, and nothing lingers (every standalone
/// delete, and same-slot ones in a cluster). Where it does not, the sweep finishes the job.
fn evict(ctx: &Context, db: c_int, name: &[u8], object_id: ObjectId) {
    tombstone::add(object_id, db, name);
    if !delete_if_present(ctx, db, name, object_id) {
        arm_sweep(ctx);
    }
}

/// Delete the key `name` in `db` if it still holds `object_id`, and drop that object's tombstone.
/// `false` if it does not — not there, or not under this name any more, or (in a command, in
/// cluster mode) in a slot this lookup cannot see.
///
/// Checks the id because a key found is not necessarily the object: the name may have been
/// overwritten since it was tombstoned. `Key::delete` returns `Ok` unconditionally, so this is
/// also the only way to know whether anything was deleted.
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

/// Delete the key of every tombstoned object.
///
/// Runs from a timer, where no command is executing, so lookups resolve their own slots. Each
/// entry says where its key was when it was tombstoned; that is a hint and is checked, because
/// the key may have been renamed or moved since. An entry whose key is not where it was is looked
/// up by object id instead, through a scan of every DB — the reverse lookup. It is slow, and it
/// is the rare path: a key has to be renamed or moved during the few milliseconds it lingers.
///
/// An object no scan finds has no key, so its entry is dropped. That is how an entry whose value
/// is still being freed by the lazyfree thread, which has not reached `lo_free` yet, goes away.
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
/// from `strays`. A fresh cursor per DB, so the walk's own cursors are undisturbed.
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
/// short and the SET must be refused — having destroyed nothing, because objects are evicted only
/// once the claims cover the request.
///
/// The claim is exclusive: the objects are tombstoned and their bytes still charged, so no other
/// request can select them or spend those bytes. That is what lets the write task settle up
/// without a second capacity check (see `DiskReservation`).
///
/// The returned handles are the keyspace's own, shared, not taken out of it: the key may be in
/// another slot than the running command's, where it cannot be reached. The reservation
/// `release`s them when the write settles.
///
/// Main thread only, before the SET's tokio task is spawned.
pub fn claim_disk_victims(ctx: &Context, need: u64) -> Option<Vec<Arc<ObjectFile>>> {
    let budget = crate::nvme_maxmemory();

    // Unreachable in practice — an unlimited budget cannot refuse a reservation, so the caller
    // never asks — but destroying data to satisfy a cap that does not exist would be the worst
    // way to find that out. `hopeless_request` reads a zero ceiling as "nothing to compare
    // against", which is right for the arena and wrong here.
    if budget == 0 {
        return None;
    }

    if hopeless_request(need, budget, crate::storage::nvme::nvme_disk_usage()) {
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

        // A claim only records, so there is nothing to put back. A walk that claimed nothing is not
        // thrashing: this counts wasted search, not lost data, and a rise against
        // `disk_evictions_total` means the working set is too pinned or too large.
        if victims > 0 {
            DISK_EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        None
    })
}

/// Accumulates claimed handles until they cover `need`. Its running total is exact rather than an
/// allocator's guess, so `satisfy` is a comparison and can never refuse.
///
/// Each entry carries its DB and key name because the key is dealt with only once `satisfy`
/// succeeds.
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
        // A second `Arc` beyond the keyspace's and this candidate's *is* the pin check — a
        // reader's reference holds the file's blocks past the unlink, so claiming it would report
        // budget we never receive. Nothing is written to the value, so a pinned one is never
        // disturbed: COPY holds its own source through a `&LoValue` while the walk runs.
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

/// Spend the claims: evict the objects and hand the handles on. Called only once the walk covered
/// the request, so this is where eviction becomes irreversible.
///
/// The handles are not released here. They ride on the reservation, so the `unlink(2)` and the
/// credit stay off the event loop and land just before the new file is created.
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

    /// Each arm is a refusal that saves the keyspace from a request no amount of work could serve.
    #[test]
    fn hopeless_request_refuses_only_what_a_walk_could_not_serve() {
        assert!(
            hopeless_request(2048, 1024, 10),
            "larger than the whole budget"
        );
        assert!(hopeless_request(64, 1024, 0), "nothing resident to evict");
        assert!(
            !hopeless_request(2048, 0, 10),
            "a zero ceiling is unlimited, not a bound of zero"
        );
        assert!(
            !hopeless_request(64, 1024, 10),
            "fits, and something is here"
        );
    }

    /// Both halves of the search bound, over core's curve. `eviction-tenacity` is the one number an
    /// operator tunes and the clock and the round count have to move together, so they are asserted
    /// together. The endpoints matter most: 0 must not mean "unbounded" and 100 must.
    #[test]
    fn tenacity_bounds_the_search() {
        assert_eq!(
            search_budget(0),
            Duration::ZERO,
            "no time, but see MIN_CANDIDATES"
        );
        assert_eq!(
            search_budget(10),
            Duration::from_micros(500),
            "the default, 20x tighter than the 10ms this replaced"
        );
        assert_eq!(
            search_budget(11),
            Duration::from_micros(575),
            "15% per point past the ramp"
        );
        assert_eq!(
            search_budget(100),
            Duration::MAX,
            "tenacity 100 has no clock"
        );
        assert!(
            search_budget(50) > search_budget(20) && search_budget(20) > search_budget(10),
            "monotone, so raising the knob can only buy more search"
        );
        assert_eq!(barren_rounds(10), 8, "the default is what this started as");
        assert_eq!(
            barren_rounds(0),
            8,
            "tenacity 0 is bounded by the clock, not this"
        );
        assert_eq!(barren_rounds(20), 16, "one doubling per 20 points");
        assert_eq!(
            barren_rounds(100),
            256,
            "no clock at 100, so this is the only stop"
        );
        assert!(
            barren_rounds(-5) == barren_rounds(0) && barren_rounds(500) == barren_rounds(100),
            "clamped, so an out-of-range config cannot shift the whole curve"
        );
        // `Duration::MAX` cannot be added to an `Instant`; handling that is what keeps tenacity 100
        // the loosest setting rather than a deadline in the past.
        assert!(
            deadline_from_now(search_budget(100)).is_none(),
            "no deadline at all, not a saturated one"
        );
        let at = deadline_from_now(search_budget(10)).expect("a finite budget has a deadline");
        assert!(at > Instant::now(), "and it is in the future");
    }
}
