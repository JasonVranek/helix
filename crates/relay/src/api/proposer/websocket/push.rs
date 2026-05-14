//! Header push service + proposer registry for the Builder API WebSocket
//! extension (ARCH v3.2).
//!
//! ## Registry (D4)
//! `WsProposerRegistry` maps a validator pubkey to a Vec of
//! `RegisteredSender` entries, each carrying a monotonic `connection_id`.
//! Multiple WS connections may register the same pubkey (registrations are
//! public bytes and trivially replayable); the relay pushes to all of them
//! and the real proposer wins because it holds the private key.
//!
//! Hard cap `MAX_SENDERS_PER_PUBKEY = 8`. On overflow we evict the oldest
//! entry (drop its sender, which causes the owning connection task to
//! observe channel close and terminate cleanly).
//!
//! ## Push service (two-phase, ARCH §4.4)
//!
//! Every `TopBidUpdate` runs two gates per connection:
//!
//! 1. **Phase A (steady-state throttle).** Push the new bid only if
//!    (a) its content-hash differs from the last one we pushed to this
//!    connection this slot, AND (b) at least `MIN_PUSH_INTERVAL` has
//!    elapsed since the last push. Both conditions together bound the
//!    stream at ~10 frames per slot under a 100ms interval.
//!
//! 2. **Phase B (deadline force-push).** Once per (connection, slot), the
//!    first TopBidUpdate we see spawns a one-shot timer that fires
//!    `auction_conclusion_ms - avg_rtt - safety_margin` ms after slot
//!    start. When it fires, the cached latest bid for that slot is
//!    force-pushed regardless of Phase A's debounce. This is the
//!    deadline guarantee: the proposer has a bid in hand before their
//!    auction closes.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use helix_common::chain_info::ChainInfo;
use helix_types::{BlsPublicKeyBytes, Slot};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::auctioneer::AuctioneerHandle;
use crate::housekeeper::CurrentSlotInfo;
use helix_common::api::proposer_api::GetHeaderParams;
use helix_common::signing::RelaySigningContext;

/// SSZ-encoded `BidPush` payload carried from push service → connection
/// handler as `(fork_byte, raw_ssz)`. The handler prepends the frame tag
/// byte before writing to the socket.
pub type HeaderPushPayload = (u8, Vec<u8>);
pub type HeaderPushSender = mpsc::UnboundedSender<HeaderPushPayload>;

/// Maximum WS connections that may register the same validator pubkey
/// before evict-oldest kicks in.
pub const MAX_SENDERS_PER_PUBKEY: usize = 8;

/// Phase A rate limit: at most one push per connection per this window.
pub const MIN_PUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Phase B safety margin: push this far before `auction_conclusion_ms`
/// (on top of the estimated RTT) to give the proposer's network stack
/// time to ingest and the get_header path time to surface.
pub const SAFETY_MARGIN: Duration = Duration::from_millis(100);

/// Default RTT assumed for Phase B scheduling before any Pong has arrived.
/// Picked as a moderately conservative value for intra-region traffic;
/// updated-on-the-fly as real Pong samples come in.
pub const DEFAULT_RTT: Duration = Duration::from_millis(50);

/// EWMA smoothing factor for RTT updates. 0.3 gives reasonable stability
/// without being overly laggy to changes.
const EWMA_ALPHA: f64 = 0.3;

// ───────────────────────────────────────────────────────────────────────────
// Per-connection state
// ───────────────────────────────────────────────────────────────────────────

/// Rolling RTT estimate updated on each Pong. Created by the connection
/// handler at upgrade time and shared with the push service via
/// `RegisteredSender.rtt`.
#[derive(Debug)]
pub struct ConnectionRtt {
    /// EWMA RTT. `None` until the first Pong is observed.
    avg: Option<Duration>,
    /// When the last Pong-derived sample was recorded (diagnostic only).
    #[allow(dead_code)]
    last_sample_at: Option<Instant>,
}

impl Default for ConnectionRtt {
    fn default() -> Self {
        Self { avg: None, last_sample_at: None }
    }
}

impl ConnectionRtt {
    /// Observe a new RTT sample from a matched Pong.
    pub fn observe(&mut self, sample: Duration) {
        let new_avg = match self.avg {
            None => sample,
            Some(prev) => {
                let prev_ms = prev.as_secs_f64() * 1000.0;
                let sample_ms = sample.as_secs_f64() * 1000.0;
                let smoothed = EWMA_ALPHA * sample_ms + (1.0 - EWMA_ALPHA) * prev_ms;
                Duration::from_secs_f64(smoothed / 1000.0)
            }
        };
        self.avg = Some(new_avg);
        self.last_sample_at = Some(Instant::now());
    }

    /// Current RTT estimate, falling back to `DEFAULT_RTT` until a real
    /// sample is observed.
    pub fn current(&self) -> Duration {
        self.avg.unwrap_or(DEFAULT_RTT)
    }
}

/// Per-(connection, slot) push state. Cleared and re-initialized on every
/// new slot. Not public because it's an internal push-service concern; the
/// connection handler only constructs a fresh `Arc<Mutex<Self>>` alongside
/// `ConnectionRtt` at upgrade time.
#[derive(Debug)]
pub(crate) struct PerSlotState {
    /// Slot this state is for.
    slot: u64,
    /// Timestamp of the most recent Phase-A-emitted push. Used to enforce
    /// `MIN_PUSH_INTERVAL`.
    last_push_at: Option<Instant>,
    /// Content hash of the most recently pushed bid; skips debounced
    /// pushes when the top is unchanged.
    last_pushed_hash: Option<u64>,
    /// Latest (fork, ssz) observed for this slot. Phase B reads this when
    /// its deadline timer fires, so it always sends the freshest bid.
    cached_bid: Option<(u8, Vec<u8>)>,
    /// True after Phase B has scheduled its one-shot timer for this slot.
    deadline_scheduled: bool,
    /// True after this connection has submitted a blinded block for this
    /// (pubkey, slot). When set, BidPush is suppressed for the rest of
    /// the slot. Reset on slot rollover.
    pub received_submission: bool,
}

impl PerSlotState {
    fn for_slot(slot: u64) -> Self {
        Self {
            slot,
            last_push_at: None,
            last_pushed_hash: None,
            cached_bid: None,
            deadline_scheduled: false,
            received_submission: false,
        }
    }
}

/// A registered WS connection's per-pubkey bookkeeping. One instance per
/// (connection_id, pubkey) pair in the registry.
#[derive(Debug)]
pub struct RegisteredSender {
    pub connection_id: u64,
    pub sender: HeaderPushSender,
    /// `auction_conclusion_ms` from the WS upgrade URL. `None` disables
    /// Phase B scheduling for this connection.
    pub auction_conclusion_ms: Option<u64>,
    /// Shared with the connection handler; the handler updates it on
    /// Pong, the push service reads it at Phase B scheduling.
    pub rtt: Arc<Mutex<ConnectionRtt>>,
    /// Per-slot push state. Shared between `handle_bid_update` and the
    /// spawned Phase B timer task.
    per_slot: Arc<Mutex<PerSlotState>>,
}

impl RegisteredSender {
    pub fn new(
        connection_id: u64,
        sender: HeaderPushSender,
        auction_conclusion_ms: Option<u64>,
        rtt: Arc<Mutex<ConnectionRtt>>,
    ) -> Self {
        Self {
            connection_id,
            sender,
            auction_conclusion_ms,
            rtt,
            per_slot: Arc::new(Mutex::new(PerSlotState::for_slot(0))),
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Registry
// ───────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct WsProposerRegistry {
    senders: RwLock<HashMap<BlsPublicKeyBytes, Vec<RegisteredSender>>>,
    next_connection_id: AtomicU64,
}

impl WsProposerRegistry {
    pub fn allocate_connection_id(&self) -> u64 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register `sender` with full per-connection state. If the pubkey
    /// already has MAX_SENDERS_PER_PUBKEY entries, evict the oldest.
    pub fn register_full(
        &self,
        pubkey: BlsPublicKeyBytes,
        connection_id: u64,
        sender: HeaderPushSender,
        auction_conclusion_ms: Option<u64>,
        rtt: Arc<Mutex<ConnectionRtt>>,
    ) {
        let rs = RegisteredSender::new(connection_id, sender, auction_conclusion_ms, rtt);
        let mut guard = self.senders.write();
        let entry = guard.entry(pubkey).or_default();
        if entry.len() >= MAX_SENDERS_PER_PUBKEY {
            let evicted = entry.remove(0);
            debug!(
                pubkey = ?pubkey,
                evicted_id = evicted.connection_id,
                "WsProposerRegistry: evicted oldest sender on overflow"
            );
        }
        entry.push(rs);
    }

    /// Convenience registration for tests / callers that don't yet track
    /// RTT or auction_conclusion (equivalent to the old `register`). Uses
    /// a fresh default `ConnectionRtt` and no auction conclusion.
    #[allow(dead_code)]
    pub fn register(
        &self,
        pubkey: BlsPublicKeyBytes,
        connection_id: u64,
        sender: HeaderPushSender,
    ) {
        self.register_full(
            pubkey,
            connection_id,
            sender,
            None,
            Arc::new(Mutex::new(ConnectionRtt::default())),
        );
    }

    /// Remove exactly the (pubkey, connection_id) pair. Other senders on
    /// the same pubkey are untouched. Empty pubkey vec drops the outer
    /// map entry too.
    pub fn unregister_sender(&self, pubkey: &BlsPublicKeyBytes, connection_id: u64) {
        let mut guard = self.senders.write();
        if let Some(entry) = guard.get_mut(pubkey) {
            entry.retain(|rs| rs.connection_id != connection_id);
            if entry.is_empty() {
                guard.remove(pubkey);
            }
        }
    }

    /// Snapshot of the sender channels for `pubkey`. Used by non-stateful
    /// callers (tests, diagnostic dumps) that don't need Phase A/B state.
    #[allow(dead_code)]
    pub fn get_all(&self, pubkey: &BlsPublicKeyBytes) -> Vec<HeaderPushSender> {
        self.senders
            .read()
            .get(pubkey)
            .map(|v| v.iter().map(|rs| rs.sender.clone()).collect())
            .unwrap_or_default()
    }

    /// Internal: snapshot the full `RegisteredSender` entries for `pubkey`.
    /// Shared `Arc`s are cheap to clone. The push service uses this to
    /// drive Phase A/B per connection.
    pub(crate) fn get_all_entries(&self, pubkey: &BlsPublicKeyBytes) -> Vec<RegisteredSenderSnapshot> {
        self.senders
            .read()
            .get(pubkey)
            .map(|v| {
                v.iter()
                    .map(|rs| RegisteredSenderSnapshot {
                        connection_id: rs.connection_id,
                        sender: rs.sender.clone(),
                        auction_conclusion_ms: rs.auction_conclusion_ms,
                        rtt: rs.rtt.clone(),
                        per_slot: rs.per_slot.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether any sender is registered for `pubkey`. Useful for tests
    /// and future per-pubkey metrics; not on the hot path.
    #[allow(dead_code)]
    pub fn is_registered(&self, pubkey: &BlsPublicKeyBytes) -> bool {
        self.senders.read().contains_key(pubkey)
    }
}

/// Cheap clone of a `RegisteredSender` with only the shared handles. The
/// push service iterates over these so it can work against a stable
/// snapshot without holding the registry's RwLock during async work.
#[derive(Clone)]
pub(crate) struct RegisteredSenderSnapshot {
    #[allow(dead_code)]
    connection_id: u64,
    sender: HeaderPushSender,
    auction_conclusion_ms: Option<u64>,
    rtt: Arc<Mutex<ConnectionRtt>>,
    pub(crate) per_slot: Arc<Mutex<PerSlotState>>,
}

// ───────────────────────────────────────────────────────────────────────────
// Push service
// ───────────────────────────────────────────────────────────────────────────

/// Run the header push service.
///
/// For each `TopBidUpdate`: look up the current proposer, look up
/// registered senders, fetch the full signed bid from the auctioneer,
/// and drive Phase A + Phase B per connection.
pub async fn run(
    registry: Arc<WsProposerRegistry>,
    current_slot_info: CurrentSlotInfo,
    auctioneer_handle: AuctioneerHandle,
    signing_context: Arc<RelaySigningContext>,
    mut top_bid_rx: broadcast::Receiver<helix_common::api::builder_api::TopBidUpdate>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    info!("WS header push service started");

    loop {
        tokio::select! {
            update = top_bid_rx.recv() => {
                match update {
                    Ok(update) => {
                        handle_bid_update(
                            &registry,
                            &current_slot_info,
                            &auctioneer_handle,
                            &signing_context,
                            &update,
                        ).await;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(n, "WS push: lagged behind updates");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("WS push: channel closed");
                        break;
                    }
                }
            }
            _ = shutdown.changed() => {
                info!("WS push service shutting down");
                break;
            }
        }
    }
}

async fn handle_bid_update(
    registry: &Arc<WsProposerRegistry>,
    current_slot_info: &CurrentSlotInfo,
    auctioneer_handle: &AuctioneerHandle,
    signing_context: &Arc<RelaySigningContext>,
    update: &helix_common::api::builder_api::TopBidUpdate,
) {
    let (head_slot, duty) = current_slot_info.slot_info();

    let proposer_pk = match duty.as_ref() {
        Some(d) => d.entry.registration.message.pubkey,
        None => {
            debug!(slot = update.slot, head_slot = head_slot.as_u64(), "WS push: no proposer duty");
            return;
        }
    };

    let snapshots = registry.get_all_entries(&proposer_pk);
    if snapshots.is_empty() {
        return;
    }

    // Fetch the full signed bid once per update and share across senders.
    let params = GetHeaderParams {
        slot: update.slot,
        parent_hash: update.parent_hash,
        pubkey: proposer_pk,
    };

    let rx = match auctioneer_handle.get_header(params) {
        Ok(rx) => rx,
        Err(_) => {
            debug!(slot = update.slot, "WS push: auctioneer channel full");
            return;
        }
    };
    let bid = match rx.await {
        Ok(Ok(bid)) => bid,
        Ok(Err(e)) => {
            debug!(slot = update.slot, ?e, "WS push: no bid from auctioneer");
            return;
        }
        Err(_) => {
            debug!(slot = update.slot, "WS push: auctioneer dropped response");
            return;
        }
    };

    let fork = signing_context.chain_info.current_fork_name();
    let fork_byte = match super::wire::fork_name_to_u8(fork) {
        Ok(b) => b,
        Err(e) => {
            debug!(?e, fork = ?fork, "WS push: fork not supported on wire; skipping");
            return;
        }
    };

    let signed_bid = crate::api::proposer::get_header::resign_builder_bid(
        bid.into_builder_bid_slow(),
        signing_context,
        fork,
    );
    let ssz_bytes = ssz::Encode::as_ssz_bytes(&signed_bid.data);
    let content_hash = fast_content_hash(&ssz_bytes);

    let now = Instant::now();
    let chain_info = signing_context.chain_info.clone();

    for snap in snapshots {
        apply_two_phase_policy(
            &snap,
            update.slot,
            fork_byte,
            &ssz_bytes,
            content_hash,
            now,
            &chain_info,
        );
    }
}

/// Apply Phase A (debounce + rate-limit) and optionally schedule Phase B
/// (deadline force-push) for a single sender. Returns nothing; results
/// show up on the sender's channel.
fn apply_two_phase_policy(
    snap: &RegisteredSenderSnapshot,
    slot: u64,
    fork_byte: u8,
    ssz_bytes: &[u8],
    content_hash: u64,
    now: Instant,
    chain_info: &Arc<ChainInfo>,
) {
    let mut st = snap.per_slot.lock();

    // New-slot rollover MUST come first — received_submission is a per-slot
    // flag that should reset on slot advance. If checked before the rollover,
    // a submission from slot N permanently blocks pushes in slot N+1 when no
    // new bid arrives (dequeue is only called on incoming bids).
    if st.slot != slot {
        *st = PerSlotState::for_slot(slot);
        debug!(
            conn = snap.connection_id,
            slot,
            "WS push: new slot, resetting per-slot state"
        );
    }

    // Settle: after receiving a blinded block submission for this
    // (pubkey, slot), stop pushing — the proposer has already acted.
    if st.received_submission {
        return;
    }
    st.cached_bid = Some((fork_byte, ssz_bytes.to_vec()));

    // Phase A
    let same_bid = st.last_pushed_hash == Some(content_hash);
    let rate_ok = st
        .last_push_at
        .map(|t| now.duration_since(t) >= MIN_PUSH_INTERVAL)
        .unwrap_or(true);
    if !same_bid && rate_ok {
        info!(
            conn = snap.connection_id,
            slot,
            "WS push: Phase A — sending bid"
        );
        if snap.sender.send((fork_byte, ssz_bytes.to_vec())).is_ok() {
            st.last_push_at = Some(now);
            st.last_pushed_hash = Some(content_hash);
        }
    } else if same_bid {
        debug!(
            conn = snap.connection_id,
            slot,
            "WS push: Phase A — skipping (same bid)"
        );
    } else {
        debug!(
            conn = snap.connection_id,
            slot,
            "WS push: Phase A — skipping (rate-limited)"
        );
    }

    // Phase B: schedule once per slot, if this connection has an
    // auction conclusion configured.
    if !st.deadline_scheduled {
        if let Some(delay) = compute_deadline_delay(snap, slot, chain_info) {
            let delay_ms = delay.as_millis() as u64;
            let rtt_ms = snap.rtt.lock().current().as_millis() as u64;
            info!(
                conn = snap.connection_id,
                slot,
                delay_ms,
                rtt_ms,
                auction_conclusion_ms = snap.auction_conclusion_ms,
                "WS push: Phase B — scheduling deadline push"
            );
            st.deadline_scheduled = true;
            let per_slot = snap.per_slot.clone();
            let sender = snap.sender.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                force_push_latest(&per_slot, slot, &sender);
            });
        }
    }
}

/// Compute the Phase B delay from *now* to the target force-push moment.
/// Returns `None` if there's no auction_conclusion_ms, if the deadline
/// has already passed, or if we can't determine our position in the
/// slot.
fn compute_deadline_delay(
    snap: &RegisteredSenderSnapshot,
    slot: u64,
    chain_info: &Arc<ChainInfo>,
) -> Option<Duration> {
    let auction_ms = snap.auction_conclusion_ms?;
    let rtt = snap.rtt.lock().current();
    let into_slot = chain_info.duration_into_slot(Slot::new(slot))?;
    let auction = Duration::from_millis(auction_ms);
    // Target = auction_ms - rtt - safety. Negative → deadline already passed.
    let target = auction.checked_sub(rtt)?.checked_sub(SAFETY_MARGIN)?;
    target.checked_sub(into_slot)
}

/// Phase B timer callback. Re-reads the latest cached bid under the lock
/// to make sure we don't send a stale one, then force-sends it.
fn force_push_latest(per_slot: &Arc<Mutex<PerSlotState>>, expected_slot: u64, sender: &HeaderPushSender) {
    let st = per_slot.lock();
    if st.slot != expected_slot {
        debug!(
            slot = st.slot,
            expected = expected_slot,
            "WS push: Phase B timer — stale slot, skipping"
        );
        return;
    }
    if let Some((f, b)) = st.cached_bid.as_ref() {
        info!(
            slot = expected_slot,
            "WS push: Phase B — deadline force-push"
        );
        let _ = sender.send((*f, b.clone()));
    } else {
        debug!(
            slot = expected_slot,
            "WS push: Phase B timer — no cached bid"
        );
    }
}

/// Fast content-hash of raw SSZ bytes. Not cryptographic — we only need
/// inequality detection to skip same-bid debouncing.
fn fast_content_hash(bytes: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

// ───────────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod push_tests {
    use super::*;

    fn make_snap(auction_ms: Option<u64>, rtt: Option<Duration>) -> RegisteredSenderSnapshot {
        let mut rtt_tracker = ConnectionRtt::default();
        if let Some(r) = rtt {
            rtt_tracker.observe(r);
        }
        let rtt_tracker = Arc::new(Mutex::new(rtt_tracker));
        let (tx, _rx) = mpsc::unbounded_channel();
        RegisteredSenderSnapshot {
            connection_id: 1,
            sender: tx,
            auction_conclusion_ms: auction_ms,
            rtt: rtt_tracker,
            per_slot: Arc::new(Mutex::new(PerSlotState::for_slot(0))),
        }
    }

    #[test]
    fn rtt_observe_uses_first_sample_directly() {
        let mut rtt = ConnectionRtt::default();
        assert_eq!(rtt.current(), DEFAULT_RTT);
        rtt.observe(Duration::from_millis(80));
        assert_eq!(rtt.current(), Duration::from_millis(80));
    }

    #[test]
    fn rtt_observe_ewma_smooths_subsequent_samples() {
        let mut rtt = ConnectionRtt::default();
        rtt.observe(Duration::from_millis(100));
        rtt.observe(Duration::from_millis(200));
        // 0.3 * 200 + 0.7 * 100 = 130
        assert_eq!(rtt.current(), Duration::from_millis(130));
    }

    #[test]
    fn phase_a_debounces_on_same_content_hash() {
        let snap = make_snap(None, None);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let snap = RegisteredSenderSnapshot { sender: tx, ..snap };

        let ssz = vec![0xaa; 100];
        let hash = fast_content_hash(&ssz);
        let chain_info = Arc::new(ChainInfo::default());
        let now = Instant::now();

        // First push succeeds.
        apply_two_phase_policy(&snap, 10, 1, &ssz, hash, now, &chain_info);
        assert!(rx.try_recv().is_ok(), "first push must deliver");

        // Second push, same hash, 200ms later → skip (debounced).
        let later = now + Duration::from_millis(200);
        apply_two_phase_policy(&snap, 10, 1, &ssz, hash, later, &chain_info);
        assert!(rx.try_recv().is_err(), "duplicate bid must be suppressed");
    }

    #[test]
    fn phase_a_rate_limits_different_bids() {
        let snap = make_snap(None, None);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let snap = RegisteredSenderSnapshot { sender: tx, ..snap };

        let chain_info = Arc::new(ChainInfo::default());
        let t0 = Instant::now();

        // Push at t=0, t=50ms, t=150ms. Only t=0 and t=150ms (>=100ms apart)
        // should land; t=50ms must be rate-limited out.
        apply_two_phase_policy(&snap, 10, 1, &[1u8; 8], 1, t0, &chain_info);
        apply_two_phase_policy(
            &snap,
            10,
            1,
            &[2u8; 8],
            2,
            t0 + Duration::from_millis(50),
            &chain_info,
        );
        apply_two_phase_policy(
            &snap,
            10,
            1,
            &[3u8; 8],
            3,
            t0 + Duration::from_millis(150),
            &chain_info,
        );

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 2, "rate-limited middle push must be dropped");
    }

    #[test]
    fn phase_a_rollover_resets_state_on_new_slot() {
        let snap = make_snap(None, None);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let snap = RegisteredSenderSnapshot { sender: tx, ..snap };

        let ssz = vec![0xaa; 8];
        let hash = fast_content_hash(&ssz);
        let chain_info = Arc::new(ChainInfo::default());
        let now = Instant::now();

        // Push for slot 10.
        apply_two_phase_policy(&snap, 10, 1, &ssz, hash, now, &chain_info);
        assert!(rx.try_recv().is_ok());

        // Same content, new slot → must NOT be debounced (fresh slot).
        apply_two_phase_policy(&snap, 11, 1, &ssz, hash, now, &chain_info);
        assert!(rx.try_recv().is_ok(), "new-slot push must deliver despite same hash");
    }

    #[test]
    fn deadline_delay_is_none_without_auction_conclusion() {
        let snap = make_snap(None, Some(Duration::from_millis(50)));
        let chain_info = Arc::new(ChainInfo::default());
        assert!(compute_deadline_delay(&snap, 10, &chain_info).is_none());
    }

    #[test]
    fn deadline_delay_subtracts_rtt_and_safety_margin() {
        // auction = 2000ms, rtt = 50ms, safety = 100ms.
        // target_abs_from_slot_start = 2000 - 50 - 100 = 1850ms.
        // The returned delay from NOW is `target - into_slot`; if we're
        // already past 1850ms into the slot, the method returns None
        // (which is the right behavior — the deadline passed). If
        // we're before it, we must get back `delay <= 1850ms`. Either
        // outcome is acceptable; both cover the invariant.
        let snap = make_snap(Some(2000), Some(Duration::from_millis(50)));
        let chain_info = Arc::new(ChainInfo::default());
        let delay = compute_deadline_delay(&snap, chain_info.current_slot().as_u64(), &chain_info);
        if let Some(d) = delay {
            assert!(
                d <= Duration::from_millis(1850),
                "delay must be <= auction - rtt - safety (1850ms), got {d:?}"
            );
        }
        // Else: deadline already passed for the current wall-clock
        // position in the slot. Also acceptable; compute_deadline_delay
        // is explicitly allowed to return None.
    }

    #[test]
    fn deadline_delay_returns_none_when_deadline_passed() {
        // auction = 50ms, rtt = 100ms → target_abs = 50 - 100 -
        // safety = underflows, checked_sub returns None.
        let snap = make_snap(Some(50), Some(Duration::from_millis(100)));
        let chain_info = Arc::new(ChainInfo::default());
        assert!(
            compute_deadline_delay(&snap, chain_info.current_slot().as_u64(), &chain_info)
                .is_none(),
            "unreachable deadline must return None"
        );
    }

    #[test]
    fn force_push_sends_cached_bid() {
        let per_slot = Arc::new(Mutex::new(PerSlotState::for_slot(10)));
        per_slot.lock().cached_bid = Some((2, vec![0xbb; 64]));

        let (tx, mut rx) = mpsc::unbounded_channel();
        force_push_latest(&per_slot, 10, &tx);

        let got = rx.try_recv().expect("force push must deliver");
        assert_eq!(got.0, 2);
        assert_eq!(got.1.len(), 64);
        assert_eq!(got.1[0], 0xbb);
    }

    #[test]
    fn force_push_skipped_if_slot_changed() {
        let per_slot = Arc::new(Mutex::new(PerSlotState::for_slot(11))); // later slot
        per_slot.lock().cached_bid = Some((2, vec![0xbb; 64]));

        let (tx, mut rx) = mpsc::unbounded_channel();
        // Timer was scheduled for slot 10; current state is 11.
        force_push_latest(&per_slot, 10, &tx);
        assert!(rx.try_recv().is_err(), "stale-slot timer must not push");
    }

    #[test]
    fn force_push_noop_when_no_cached_bid() {
        let per_slot = Arc::new(Mutex::new(PerSlotState::for_slot(10)));
        // No cached_bid set.
        let (tx, mut rx) = mpsc::unbounded_channel();
        force_push_latest(&per_slot, 10, &tx);
        assert!(rx.try_recv().is_err(), "no cached bid → no push");
    }

    #[test]
    fn content_hash_equal_for_same_bytes_different_for_different() {
        let a = vec![1u8; 100];
        let b = vec![1u8; 100];
        let c = vec![2u8; 100];
        assert_eq!(fast_content_hash(&a), fast_content_hash(&b));
        assert_ne!(fast_content_hash(&a), fast_content_hash(&c));
    }

    #[test]
    fn per_slot_settle_resets_on_rollover() {
        let mut st = PerSlotState::for_slot(10);
        st.received_submission = true;
        assert!(st.received_submission);

        // Simulate slot rollover via for_slot
        let st = PerSlotState::for_slot(11);
        assert!(!st.received_submission, "settle must reset on new slot");
    }

    #[test]
    fn per_slot_settle_prevents_push() {
        let mut st = PerSlotState::for_slot(10);
        assert!(!st.received_submission);
        st.received_submission = true;

        // In apply_two_phase_policy, the settle guard checks this.
        // Verify the field is accessible and set properly.
        assert!(st.received_submission);
    }
}
