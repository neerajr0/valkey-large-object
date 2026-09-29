//! Eviction — giving up resident objects to free a budget.
//!
//! # One driver
//!
//! `walk` is the entire policy: how long to search, which candidates to sample, how to rank them,
//! what to skip, when to give up. Both modes go through it, so victim selection cannot drift apart
//! between them, and both stay aligned with core's policy.
//!
//! What `walk` does not decide is what a freed byte *is*. That is where the two modes fork: each
//! passes in a `Budget` impl with two methods. `claim` gives up one victim and reports the bytes it
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
//! and still fails the SET. A pin is the NIC reading the buffer. The bytes come back in our own
//! drop, inside this call stack.
//!
//! `[Tiered]` — `claim_disk_victims`, from `engine::reserve_nvme_or_make_room`, claims
//! `nvme-maxmemory` budget through `DiskLedger`. Its `satisfy` compares a running total against the
//! request and can never refuse, so the deletes can wait until the claims cover it: a walk that
//! comes up short puts every key back and destroys nothing. A pin is a reader holding the unlinked
//! file open. The bytes are settled later, by the write task.
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
//! - **The scan visits every key in the DB, not just `LO` keys**, so a keyspace that is mostly
//!   non-module keys spends its budget on misses and `MAX_EMPTY_STEPS` gives up rather than
//!   hunting.
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
//! Serving a GET is another matter. `Dram` + TCP is the one path that stays on the event loop
//! (`engine::execute_get`); the other three block the client and move the object's `Arc` onto a
//! tokio thread — EFA because the NIC is reading the buffer, `Tiered` because an NVMe read holds
//! the file handle open. That live `Arc` is a pin, and a pinned object is skipped: claiming it
//! would report bytes that do not come back until the reader is done.
//!
//! Skipping is safe in both directions because we hold the event loop. A new pin cannot arrive
//! between the check and the delete, since making one means dispatching a command. A pin that
//! *disappears* mid-walk only costs us a candidate the next round re-offers.

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::{c_int, c_longlong};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use valkey_module::key::ValkeyKey;
use valkey_module::{raw, Context, KeysCursor, ValkeyString};

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
//
// The arena, the NVMe ledger and `object_count()` are process-global while the scan is per-DB, so
// a client on DB 0 cannot evict DB 5's objects even when they are the whole budget.
thread_local! {
    static CURSORS: RefCell<HashMap<c_int, Rc<KeysCursor>>> = RefCell::new(HashMap::new());
}

/// Run `f` against the resumable cursor for the calling client's selected DB.
///
/// Raw FFI because the crate wraps neither `RM_GetSelectedDb` nor the `selected_db` it reads.
/// The `Rc` is cloned out before `f` runs so the walk — which deletes keys, and may re-enter
/// through anything a notification handler does — never runs inside the `RefCell` borrow.
fn with_cursor<T>(ctx: &Context, f: impl FnOnce(&KeysCursor) -> T) -> T {
    // SAFETY: `ctx.ctx` is a live module context; we are inside a command.
    let db = unsafe { raw::RedisModule_GetSelectedDb.unwrap()(ctx.ctx) };
    let cursor = CURSORS.with(|cursors| {
        Rc::clone(
            cursors
                .borrow_mut()
                .entry(db)
                .or_insert_with(|| Rc::new(KeysCursor::new())),
        )
    });
    f(&cursor)
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

/// The keyspace walk as an iterator: `LO` keys from wherever `cursor` stopped last time, in
/// cursor order, wrapping as often as the budget allows.
///
/// Two stop conditions, each with one job. `MAX_EMPTY_STEPS` bounds the *search* for candidates
/// and must hold even at tenacity 100, where no deadline ever arrives. `deadline` bounds the
/// *walk*, and only once `MIN_CANDIDATES` have been offered — without that gate a zero budget
/// would yield nothing at all.
struct Candidates<'a> {
    ctx: &'a Context,
    cursor: &'a KeysCursor,
    /// One bucket's worth of keys from the last `scan_step`.
    batch: std::vec::IntoIter<(ValkeyString, ObjectId)>,
    /// `None` when the budget is unbounded.
    deadline: Option<Instant>,
    yielded: usize,
    empty_steps: usize,
    /// Set once a stop condition fires. The batch in hand is still drained first — those
    /// keys were offered by the same scan that ended the walk.
    stop: bool,
}

impl<'a> Candidates<'a> {
    fn new(ctx: &'a Context, cursor: &'a KeysCursor, deadline: Option<Instant>) -> Self {
        Self {
            ctx,
            cursor,
            batch: Vec::new().into_iter(),
            deadline,
            yielded: 0,
            empty_steps: 0,
            stop: false,
        }
    }

    fn out_of_time(&self) -> bool {
        self.deadline.is_some_and(|at| Instant::now() >= at)
    }
}

impl Iterator for Candidates<'_> {
    type Item = (ValkeyString, ObjectId);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(candidate) = self.batch.next() {
                self.yielded += 1;
                return Some(candidate);
            }
            if self.stop {
                return None;
            }

            let (found, at_end) = scan_step(self.ctx, self.cursor);
            if at_end {
                self.cursor.restart();
            }
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

/// Advance `cursor` one step — one bucket, so zero to several keys — and return the `LO` keys it
/// visited plus whether it reached the end of the table.
///
/// Victims are collected rather than acted on in the callback because `RM_Scan` permits deleting
/// the key currently being visited but not others, and we may delete a whole bucket's worth.
/// Outliving the callback is sound because the names are owned, not borrowed: the crate's
/// trampoline builds each one with `ValkeyString::new`, which retains.
fn scan_step(ctx: &Context, cursor: &KeysCursor) -> (Vec<(ValkeyString, ObjectId)>, bool) {
    // `RefCell` rather than a captured `&mut`: the crate hands the trampoline a `*mut F`
    // built from a `&F`, so a closure that mutates its captures through a shared
    // reference is the shape that wrapper actually supports.
    let found = RefCell::new(Vec::new());

    let collect = |_ctx: &Context, key_name: ValkeyString, key: Option<&ValkeyKey>| {
        // Scans are keyspace-wide and we may not be the only module loaded, so confirm the key
        // is ours. `get_value` compares module-type pointers, which is a real guarantee:
        // `RM_CreateDataType` refuses duplicate names and mints a fresh `moduleType` per call.
        let Some(key) = key else {
            return; // no handle offered; skip rather than reopen mid-scan
        };
        if let Ok(Some(lo)) = key.get_value::<LoValue>(&LO_TYPE) {
            found.borrow_mut().push((key_name, lo.object_id));
        }
    };

    let more = cursor.scan(ctx, &collect);
    (found.into_inner(), !more)
}

// ─── Shared: victim ranking ──────────────────────────────────────────────────

/// Core's `objectGetIdleness` (`lrulfu.c:162-171`) through the module API, plus the one
/// eligibility rule the getters cannot express. Higher is a better victim: idle milliseconds
/// under an LRU policy, `UINT8_MAX - freq` under an LFU one.
///
/// **`None` means "not a candidate", which under `volatile-*` is a key with no TTL.** Core samples
/// `db->expires` there, so a persistent key is never a victim; we sample the whole keyspace and
/// have to exclude it ourselves. The `RM_GetExpire` rides on the handle opened below. The policy
/// itself is read once per walk, by the caller.
///
/// **No `maxmemory-policy` read is needed** — the getters are self-identifying. `VM_GetLRU`
/// returns OK but writes `-1` under an LFU policy (`module.c:14731-14737`), `VM_GetLFU` likewise
/// unless `lrulfu_isUsingLFU()` (`:14753-14758`), and `-1` is unreachable for a live value. So
/// "call both, take the non-negative" *is* `objectGetIdleness`. Never trust the return code, only
/// the sentinel. Units differ from core's seconds — a monotone transform of a score only ever
/// compared against itself.
///
/// **`NOTOUCH` is load-bearing.** `RM_OpenKey` otherwise stamps `val->lru` through `lookupKey`
/// *before* we read it (`db.c:105-116`), so every sampled key reports idle ≈ 0 and the ranking
/// measures our own sampling. That is erasure rather than bias, and it leaks: `val->lru` is what
/// core's `performEvictions` reads, so sampled LO keys would look hot to core and push its
/// eviction toward everyone else's keys. `NONOTIFY` and `NOSTATS` are the same argument for the
/// rest of the lookup's side effects (`module.c:4310-4314`) — without them sampling shows up as
/// hits in `INFO stats` and fires `keymiss` notifications for keys nobody asked for.
///
/// Reading the LFU frequency does still mutate: `objectGetLFUFrequency` writes the decayed
/// counter back (`object.c:1677-1680`). That is decay, not a touch, and core does the same while
/// sampling. There is no read-only sample.
///
/// Raw FFI because the crate wraps neither getter and `ValkeyKey.key_inner` is `pub(crate)`.
/// A missing key scores 0 — the least attractive victim, which is right for something gone.
fn idleness(ctx: &Context, key_name: &ValkeyString, volatile_only: bool) -> Option<i64> {
    const MODE: c_int = (raw::REDISMODULE_READ
        | raw::REDISMODULE_OPEN_KEY_NOTOUCH
        | raw::REDISMODULE_OPEN_KEY_NONOTIFY
        | raw::REDISMODULE_OPEN_KEY_NOSTATS) as c_int;

    // SAFETY: `ctx.ctx` is a live module context (we are inside a command) and
    // `key_name.inner` a live `RedisModuleString` owned by the caller. `RM_OpenKey` either
    // returns a handle we close below or null.
    let key = unsafe { raw::RedisModule_OpenKey.unwrap()(ctx.ctx, key_name.inner, MODE) };
    if key.is_null() {
        return Some(0);
    }

    let mut idle_ms: raw::mstime_t = -1;
    let mut freq: c_longlong = -1;
    // SAFETY: `key` is non-null; the two getters only write through their out-params and
    // `RM_GetExpire` only reads.
    let ttl = unsafe {
        raw::RedisModule_GetLRU.unwrap()(key, &mut idle_ms);
        raw::RedisModule_GetLFU.unwrap()(key, &mut freq);
        raw::RedisModule_GetExpire.unwrap()(key)
    };
    raw::close_key(key);

    if volatile_only && ttl == raw::REDISMODULE_NO_EXPIRE as raw::mstime_t {
        return None;
    }

    Some(if freq >= 0 {
        i64::from(u8::MAX) - freq as i64
    } else {
        (idle_ms as i64).max(0)
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
    /// Deleting the key is the impl's business: `Arena` must destroy now, since `satisfy` cannot
    /// answer until the bytes are really free; `DiskLedger` empties the value and defers. Hence
    /// `key_name` by value, so a deferring impl can keep it.
    fn claim(&mut self, ctx: &Context, key_name: ValkeyString, object_id: &ObjectId)
        -> Option<u64>;

    /// The authoritative check, asked only once the discounted credit says it is worth
    /// asking. `Some` ends the walk successfully; `None` means keep going, and for the arena
    /// it means the bytes are free but unusably placed.
    fn satisfy(&mut self, need: u64) -> Option<Self::Output>;
}

/// Sample, score, claim, repeat until `budget` is satisfied or a stop condition fires. Returns
/// the output and how many objects were given up, which callers need to tell a failed run from
/// a no-op one.
///
/// The final `satisfy` is reachable with the request already paid for. A refusal resets the
/// credit, so a walk can free well over `need` in total and still end below the threshold that
/// triggers an attempt — and the victims freed since that refusal may have coalesced into the run
/// the allocator wanted. Core does the same, re-checking `getMaxmemoryState` at `cant_free`.
fn walk<B: Budget>(
    ctx: &Context,
    cursor: &KeysCursor,
    need: u64,
    budget: &mut B,
) -> (Option<B::Output>, usize) {
    // One read each, so a walk is internally consistent against a runtime-modifiable config.
    let tenacity = crate::eviction_tenacity();
    let deadline = deadline_from_now(search_budget(tenacity));
    let barren_limit = barren_rounds(tenacity);
    let samples = crate::maxmemory_samples();
    let volatile_only = crate::volatile_policy(ctx);
    let mut candidates = Candidates::new(ctx, cursor, deadline);

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

        let round: Vec<(ValkeyString, ObjectId)> = candidates.by_ref().take(samples).collect();
        if round.is_empty() {
            break; // the supply gave up first — out of time, or nothing left to find
        }

        // Best victim first. Claiming in score order is what makes this a policy rather than an
        // ordering, and a key we could not claim costs us the next-best rather than the walk.
        let offered = round.len();
        let mut ranked: Vec<(i64, ValkeyString, ObjectId)> = round
            .into_iter()
            .filter_map(|(name, oid)| idleness(ctx, &name, volatile_only).map(|s| (s, name, oid)))
            .collect();
        ranked.sort_unstable_by_key(|(score, _, _)| std::cmp::Reverse(*score));
        // A key the policy excluded cost a sample even though nothing claims it, so the clock's
        // `MIN_CANDIDATES` floor counts it — otherwise a keyspace of persistent keys under
        // `volatile-*` would walk to `barren_limit` with the deadline never armed.
        examined += offered - ranked.len();

        let mut claimed_any = false;
        for (_score, key_name, object_id) in ranked {
            examined += 1;
            let Some(bytes) = budget.claim(ctx, key_name, &object_id) else {
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
///
/// The caller checks the eviction policy (`engine::alloc_dram_or_make_room`).
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

    let mut arena = Arena;
    let (buffers, victims) = with_cursor(ctx, |cursor| walk(ctx, cursor, need as u64, &mut arena));

    // Count only runs that gave something up — they paid and got nothing.
    if buffers.is_none() && victims > 0 {
        EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    buffers
}

struct Arena;

impl Budget for Arena {
    type Output = Vec<SegmentBuffer>;

    fn claim(
        &mut self,
        ctx: &Context,
        key_name: ValkeyString,
        object_id: &ObjectId,
    ) -> Option<u64> {
        // A transfer is reading this buffer, so taking the entry would leave it gone and the
        // bytes still unavailable — the freeing drop belongs to whoever holds the last
        // reference. Not worth waiting for either: an EFA read is a network round trip.
        if crate::storage::get_dram_pool().is_pinned(object_id) {
            PINNED_SKIPS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        reclaim(ctx, &key_name, object_id)
    }

    fn satisfy(&mut self, need: u64) -> Option<Vec<SegmentBuffer>> {
        crate::storage::get_dram_pool().alloc_exact(need as usize)
    }
}

/// Destroy the `LO` object at `key_name` and report the arena bytes it gave up. `None` if the
/// key could not be taken, which is the walk's signal that nothing happened.
fn reclaim(ctx: &Context, key_name: &ValkeyString, object_id: &ObjectId) -> Option<u64> {
    let key = ctx.open_key_writable(key_name);

    // Take the reference, and so the size, *before* unlinking: `delete` frees the `LoValue`
    // either inline or on the lazyfree thread (`lazyfree-lazy-server-del`, on by default), and
    // `lo_free` calls `remove_object`, so the entry may be gone by the time we look. Crediting 0
    // there counts as progress while making none, which resets the walk's termination guards.
    let obj_ctx = crate::storage::get_dram_pool().get_object(object_id)?;
    let bytes = arena_bytes(&obj_ctx);

    if key.delete().is_err() {
        return None;
    }

    // `delete` alone is not enough: `free_effort()` returns 0, so the core may defer the value to
    // a BIO thread and the retry that motivated this eviction would run long before it. Dropping
    // the map's reference here, and ours at the end of this scope, runs `ObjectContext::Drop` ->
    // `dram_pool.free` now — a talc coalesce, no syscall.
    crate::storage::get_dram_pool().remove_object(object_id);

    RECLAIMED_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
    Some(bytes)
}

/// The arena bytes an object holds: the buffers' aligned `len`, which is what freeing gives
/// back, rather than the user length it was asked for.
fn arena_bytes(obj_ctx: &crate::storage::context::ObjectContext) -> u64 {
    obj_ctx.buffers.iter().map(|b| b.len as u64).sum()
}

// ─── Tiered: the nvme-maxmemory budget ───────────────────────────────────────

/// Claim enough resident objects to pay for `need` disk bytes. `None` means the search came up
/// short and the SET must be refused — having destroyed nothing, because keys are deleted only once
/// the claims cover the request.
///
/// The claim is exclusive: the objects are out of the keyspace and their bytes still charged, so no
/// other request can select them or spend those bytes. That is what lets the write task settle up
/// without a second capacity check (see `DiskReservation`).
///
/// Main thread only, before the SET's tokio task is spawned.
pub fn claim_disk_victims(ctx: &Context, need: u64) -> Option<Vec<ObjectFile>> {
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

    let mut ledger = DiskLedger::default();
    let (claimed, victims) = with_cursor(ctx, |cursor| walk(ctx, cursor, need, &mut ledger));

    if let Some(claimed) = claimed {
        return Some(delete_claimed(ctx, claimed));
    }

    restore_claimed(ctx, ledger.claimed);

    // A walk that claimed nothing is not thrashing. The keys are back, so this counts wasted
    // search, not lost data: a rise against `disk_evictions_total` means the working set is too
    // pinned or too large.
    if victims > 0 {
        DISK_EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    None
}

/// Accumulates claimed handles until they cover `need`. Its running total is exact rather than an
/// allocator's guess, so `satisfy` is a comparison and can never refuse.
///
/// Each entry carries its key name because the delete is deferred until `satisfy` succeeds.
#[derive(Default)]
struct DiskLedger {
    claimed: Vec<(ValkeyString, ObjectFile)>,
    freed: u64,
}

impl Budget for DiskLedger {
    type Output = Vec<(ValkeyString, ObjectFile)>;

    fn claim(
        &mut self,
        ctx: &Context,
        key_name: ValkeyString,
        _object_id: &ObjectId,
    ) -> Option<u64> {
        let file = take_file(ctx, &key_name)?;
        let bytes = file.disk_len();
        self.freed += bytes;
        self.claimed.push((key_name, file));
        Some(bytes)
    }

    fn satisfy(&mut self, need: u64) -> Option<Self::Output> {
        (self.freed >= need).then(|| std::mem::take(&mut self.claimed))
    }
}

/// Take the handle out of the `LO` object at `key_name`, leaving the key in place. `None` if there
/// is no handle or a reader holds one.
///
/// Ownership, not a clone, so the caller picks the moment of the `unlink(2)`. `free_effort` returns
/// 0, so under the default `lazyfree-lazy-server-del yes` the keyspace's reference outlives
/// `delete()` by however long a BIO thread takes; emptying the value leaves that thread nothing to
/// drop.
///
/// `try_unwrap` failing *is* the pin check — a reader's `Arc` holds the file's blocks past the
/// unlink, so claiming it would report budget we never receive.
///
/// The emptied value never escapes: the walk holds the event loop, and every handle is deleted with
/// its key or put back before it returns. A key the scan cursor offers twice finds nothing left to
/// take the second time, which is how the walk avoids counting it twice.
fn take_file(ctx: &Context, key_name: &ValkeyString) -> Option<ObjectFile> {
    let key = ctx.open_key_writable(key_name);
    let Ok(Some(lo)) = key.get_value::<LoValue>(&LO_TYPE) else {
        return None;
    };
    match Arc::try_unwrap(lo.file.take()?) {
        Ok(file) => Some(file),
        Err(pinned) => {
            lo.file = Some(pinned);
            PINNED_SKIPS_TOTAL.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

/// Spend the claims: delete the keys and hand the handles on. Called only once the walk covered the
/// request, so this is where eviction becomes irreversible.
///
/// The values are already emptied, so the deletes free nothing — the bytes and the `unlink(2)` ride
/// on the handles instead. `Key::delete` returns `Ok` unconditionally, hence the discard.
fn delete_claimed(ctx: &Context, claimed: Vec<(ValkeyString, ObjectFile)>) -> Vec<ObjectFile> {
    claimed
        .into_iter()
        .map(|(key_name, file)| {
            let _ = ctx.open_key_writable(&key_name).delete();
            DISK_RECLAIMED_BYTES_TOTAL.fetch_add(file.disk_len(), Ordering::Relaxed);
            DISK_EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
            file
        })
        .collect()
}

/// Undo the claims of a walk that came up short, so a refused SET costs the keyspace nothing — the
/// object is as it was, still charged, still readable, `lru` untouched because sampling was
/// `NOTOUCH`.
///
/// A key that expired mid-walk has no value left to take its handle back. Letting the handle drop
/// here is right: the key is gone, so the file and its bytes should go with it.
fn restore_claimed(ctx: &Context, claimed: Vec<(ValkeyString, ObjectFile)>) {
    for (key_name, file) in claimed {
        let key = ctx.open_key_writable(&key_name);
        if let Ok(Some(lo)) = key.get_value::<LoValue>(&LO_TYPE) {
            lo.file = Some(Arc::new(file));
        }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::context::ObjectContext;
    use crate::storage::DRAMPool;

    /// The credit a walk gets is the aligned size the arena gave out, not the user length, since
    /// the former is what freeing gives back.
    #[test]
    fn credit_is_the_aligned_size_the_arena_gave_out() {
        let pool = DRAMPool::new(1, 1024 * 1024);
        let buffers = pool.alloc_exact(4096).expect("fresh pool must serve 4096");
        let placed: u64 = buffers.iter().map(|b| b.len as u64).sum();
        let obj_ctx = ObjectContext::new_ready(buffers);

        assert_eq!(arena_bytes(&obj_ctx), placed);
        assert!(arena_bytes(&obj_ctx) >= 4096, "never under the request");
    }

    /// Each arm is a refusal that saves the keyspace from a request no amount of work could serve.
    #[test]
    fn hopeless_request_refuses_only_what_a_walk_could_not_serve() {
        assert!(
            hopeless_request(2048, 1024, 10),
            "larger than the whole budget"
        );
        assert!(hopeless_request(64, 1024, 0), "nothing resident to give up");
        assert!(
            !hopeless_request(2048, 0, 10),
            "a zero ceiling is unlimited, not a bound of zero"
        );
        assert!(
            !hopeless_request(64, 1024, 10),
            "fits, and something is here"
        );
    }

    /// Each segment of core's curve, since this is the one number an operator tunes. The
    /// endpoints matter most: 0 must not mean "unbounded" and 100 must.
    #[test]
    fn search_budget_matches_cores_curve() {
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
    }

    /// The other half of the search bound, and the only one that binds when every candidate is
    /// pinned. Asserted alongside the clock because the two have to move together.
    #[test]
    fn barren_rounds_scales_with_tenacity() {
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
    }

    /// `Duration::MAX` cannot be added to an `Instant`, and handling that is what keeps tenacity
    /// 100 the loosest setting rather than a deadline in the past.
    #[test]
    fn an_unbounded_budget_yields_a_deadline_that_never_arrives() {
        assert!(
            deadline_from_now(search_budget(100)).is_none(),
            "no deadline at all, not a saturated one"
        );
        let at = deadline_from_now(search_budget(10)).expect("a finite budget has a deadline");
        assert!(at > Instant::now(), "and it is in the future");
    }
}
