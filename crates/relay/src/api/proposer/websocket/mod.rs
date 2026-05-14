//! WebSocket endpoint for the Builder API extension (ARCH v3.2).
//!
//! The WS connection is just a persistent pipe between commit-boost and
//! Helix. REST flows unchanged alongside. No auth handshake.
//! Validator registrations arrive as the specs-SSZ
//! `SignedValidatorRegistrationV1` inside a `SubscriptionBatch`.
//! The relay verifies the BLS signature and WS-only timestamp freshness
//! (±1 epoch), inserts accepted pubkeys into the `WsProposerRegistry`, and
//! returns a single `RegistrationAck { accepted, rejected }`.

pub mod push;
pub mod wire;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Extension,
    extract::{
        Query,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::StreamExt;
use helix_common::utils::utcnow_sec;
use helix_types::{BlsPublicKeyBytes, ForkName, SignedBlindedBeaconBlock, SignedBlindedBeaconBlockElectra, SignedBlindedBeaconBlockFulu, SignedBlindedBeaconBlockGloas};
use parking_lot::Mutex;
use serde::Deserialize;
use ssz::Decode;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::api::Api;
use crate::api::proposer::ProposerApi;
use crate::api::proposer::get_payload::ProposerApiVersion;
use helix_common::traces::proposer_api::GetPayloadTrace;
use push::{ConnectionRtt, WsProposerRegistry};
use ws_wire::WsMessage;
use ws_wire::framing;
use ws_wire::messages::{RegistrationAck, SubmitBlindedBlockAck, SubmitStatus};

/// Interval at which the relay sends Pings to measure RTT / detect dead
/// connections. The connection handler stamps the nonce and records the
/// send time; when the matching Pong arrives it feeds the delta into the
/// connection's `ConnectionRtt` EWMA.
const PING_INTERVAL: Duration = Duration::from_secs(10);

// ───────────────────────────────────────────────────────────────────────────
// Query parameters (ARCH §3.2, D7)
// ───────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct WsConnectParams {
    pub auction_conclusion_ms: Option<u64>,
}

/// Clamp `auction_conclusion_ms` to `[100, 11000]` per D7. Missing → None
/// (push service will use relay-configured default).
pub fn clamp_auction_conclusion(raw: Option<u64>) -> Option<u64> {
    raw.map(|v| v.clamp(100, 11_000))
}

// ───────────────────────────────────────────────────────────────────────────
// Timestamp freshness (ARCH §5.1)
// ───────────────────────────────────────────────────────────────────────────

/// WS-only freshness: `[genesis_time, now + 10s]`. Matches the REST
/// check's lower bound (`genesis_time_in_secs`) but keeps the same
/// +10s future tolerance. The ±1-epoch replay harden was too
/// aggressive for Kurtosis environments where registrations may be
/// generated several minutes before WS connection startup.
/// The 1:N registry design (ARCH §5.2) already handles replay by
/// pushing to all registered connections.
const WS_TIMESTAMP_MAX_FUTURE_SEC: u64 = 10;

pub fn check_ws_timestamp_freshness(timestamp: u64, now: u64) -> bool {
    // Upper bound: `now + 10s` — rejects future-dated registrations.
    // Lower bound: effectively none — matches REST's `genesis_time_in_secs`.
    timestamp <= now + WS_TIMESTAMP_MAX_FUTURE_SEC
}

// ───────────────────────────────────────────────────────────────────────────
// Handler
// ───────────────────────────────────────────────────────────────────────────

/// Handle WebSocket upgrade at `/eth/v1/builder/ws`.
pub async fn connect_ws<A: Api>(
    Extension(api): Extension<Arc<ProposerApi<A>>>,
    Extension(registry): Extension<Arc<WsProposerRegistry>>,
    Query(params): Query<WsConnectParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let auction_conclusion_ms = clamp_auction_conclusion(params.auction_conclusion_ms);
    ws.on_upgrade(move |socket| handle_connection(socket, api, registry, auction_conclusion_ms))
}

async fn handle_connection<A: Api>(
    mut socket: WebSocket,
    api: Arc<ProposerApi<A>>,
    registry: Arc<WsProposerRegistry>,
    auction_conclusion_ms: Option<u64>,
) {
    let connection_id = registry.allocate_connection_id();
    info!(connection_id, ?auction_conclusion_ms, "WS: new connection");

    // Outbound push channel for this connection. push::run sends
    // (fork_byte, ssz_bytes) payloads; the handler prepends the tag byte
    // and forwards to the socket.
    let (header_tx, mut header_rx) = mpsc::unbounded_channel::<(u8, Vec<u8>)>();

    // Pubkeys this connection has registered — used for cleanup on drop.
    let mut registered_pubkeys: Vec<BlsPublicKeyBytes> = Vec::new();

    let builder_domain = api.signing_context.chain_info.builder_domain;

    // RTT state shared with the push service via `register_full`. Updated
    // here on every matched Pong.
    let rtt_tracker = Arc::new(Mutex::new(ConnectionRtt::default()));

    // Relay-side periodic Ping. On each tick we send a Ping with a new
    // monotonic nonce and record its send instant. When the matching
    // Pong arrives, `rtt_tracker.observe(send_instant.elapsed())` feeds
    // the EWMA.
    let mut ping_timer = tokio::time::interval(PING_INTERVAL);
    // Skip the first tick so we don't Ping immediately on upgrade before
    // the proposer has authenticated.
    ping_timer.tick().await;
    let mut next_ping_nonce: u64 = 1;
    // In-flight Pings awaiting their Pong. Bounded in practice by the
    // Ping interval × pong-timeout; we cap at 8 to avoid unbounded
    // growth on a stuck proposer.
    let mut in_flight_pings: Vec<(u64, Instant)> = Vec::with_capacity(4);

    loop {
        tokio::select! {
            _ = ping_timer.tick() => {
                let nonce = next_ping_nonce;
                next_ping_nonce = next_ping_nonce.wrapping_add(1);
                let ping = framing::encode(&WsMessage::Ping(ws_wire::messages::Ping { nonce }));
                if socket.send(Message::Binary(ping.into())).await.is_err() {
                    warn!(connection_id, "WS: ping send failed — closing");
                    break;
                }
                in_flight_pings.push((nonce, Instant::now()));
                // Drop the oldest in-flight ping if we're holding too
                // many — implies the proposer stopped responding.
                if in_flight_pings.len() > 8 {
                    in_flight_pings.remove(0);
                }
            }

            msg = socket.next() => {
                let data = match msg {
                    Some(Ok(Message::Binary(d))) => d,
                    Some(Ok(Message::Close(_))) | None => {
                        info!(connection_id, "WS: connection closed");
                        break;
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                    Some(Err(e)) => {
                        warn!(connection_id, ?e, "WS: error");
                        break;
                    }
                    _ => continue,
                };

                let decoded = match framing::decode(&data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(connection_id, ?e, "WS: decode error — closing");
                        break;
                    }
                };

                match decoded {
                    WsMessage::Subscribe(batch) => {
                        let (accepted, rejected) = process_registration_batch(
                            &batch,
                            &registry,
                            &header_tx,
                            connection_id,
                            auction_conclusion_ms,
                            &rtt_tracker,
                            &mut registered_pubkeys,
                            builder_domain,
                        );
                        let ack = RegistrationAck { accepted, rejected };
                        let reply = framing::encode(&WsMessage::RegistrationAck(ack));
                        if socket.send(Message::Binary(reply.into())).await.is_err() {
                            break;
                        }
                        info!(
                            connection_id,
                            total = batch.registrations.len(),
                            accepted,
                            rejected,
                            "WS: processed registration batch"
                        );
                    }
                    WsMessage::SubmitBlindedBlock { fork, body_ssz } => {
                        // Settle: stop pushing bids for all registered pubkeys
                        // — the proposer has already acted on the winning bid.
                        for pubkey in &registered_pubkeys {
                            for snap in registry.get_all_entries(pubkey) {
                                snap.per_slot.lock().received_submission = true;
                            }
                        }
                        let status = route_submit_blinded_block(&api, fork, &body_ssz).await;
                        let ack = SubmitBlindedBlockAck { status: status as u8 };
                        let reply = framing::encode(&WsMessage::SubmitBlindedBlockAck(ack));
                        let _ = socket.send(Message::Binary(reply.into())).await;
                    }
                    WsMessage::Ping(p) => {
                        let pong = framing::encode(&WsMessage::Pong(ws_wire::messages::Pong {
                            nonce: p.nonce,
                        }));
                        let _ = socket.send(Message::Binary(pong.into())).await;
                    }
                    WsMessage::Pong(p) => {
                        // Look up the matching in-flight ping and observe
                        // its round-trip time. Unmatched Pongs (duplicate
                        // or stale) are silently ignored.
                        if let Some(idx) =
                            in_flight_pings.iter().position(|(n, _)| *n == p.nonce)
                        {
                            let (_, sent_at) = in_flight_pings.remove(idx);
                            let sample = sent_at.elapsed();
                            rtt_tracker.lock().observe(sample);
                            let current_rtt_ms =
                                rtt_tracker.lock().current().as_millis() as u64;
                            info!(
                                connection_id,
                                sample_ms = sample.as_millis() as u64,
                                ewma_rtt_ms = current_rtt_ms,
                                "WS: observed RTT sample"
                            );
                        }
                    }
                    _ => {
                        debug!(connection_id, "WS: unexpected message type");
                    }
                }
            }

            // Outbound: push bid payloads to the proposer.
            push = header_rx.recv() => {
                match push {
                    Some((fork, ssz_bytes)) => {
                        let msg = framing::encode(&WsMessage::BidPush {
                            fork,
                            payload: ssz_bytes,
                        });
                        if socket.send(Message::Binary(msg.into())).await.is_err() {
                            warn!(connection_id, "WS: send failed — closing");
                            break;
                        }
                    }
                    None => { break; }
                }
            }
        }
    }

    // Cleanup: drop every registration this connection owned.
    for pk in &registered_pubkeys {
        registry.unregister_sender(pk, connection_id);
    }
    if !registered_pubkeys.is_empty() {
        info!(
            connection_id,
            count = registered_pubkeys.len(),
            "WS: connection cleaned up"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Registration processing
// ───────────────────────────────────────────────────────────────────────────

/// Process a `Subscribe` batch. Per-entry: timestamp freshness
/// + BLS signature verification. Accepted pubkeys are inserted into the
/// registry under `connection_id`. Returns `(accepted, rejected)` counters.
///
/// Applies identically to the initial batch and any mid-stream batch
/// (D5).
#[allow(clippy::too_many_arguments)]
fn process_registration_batch(
    batch: &ws_wire::messages::SubscriptionBatch,
    registry: &Arc<WsProposerRegistry>,
    header_tx: &mpsc::UnboundedSender<(u8, Vec<u8>)>,
    connection_id: u64,
    auction_conclusion_ms: Option<u64>,
    rtt_tracker: &Arc<Mutex<ConnectionRtt>>,
    registered_pubkeys: &mut Vec<BlsPublicKeyBytes>,
    builder_domain: alloy_primitives::B256,
) -> (u32, u32) {
    let now = utcnow_sec();
    let mut accepted = 0u32;
    let mut rejected = 0u32;

    for wire_reg in batch.registrations.iter() {
        // 1. Timestamp freshness (WS-only tighter window)
        if !check_ws_timestamp_freshness(wire_reg.message.timestamp, now) {
            rejected += 1;
            continue;
        }

        // 2. BLS signature
        let native = wire::wire_to_native_registration(wire_reg);
        if native.verify_signature(builder_domain).is_err() {
            rejected += 1;
            continue;
        }

        // 3. Register with full per-connection state so the push service
        //    can drive Phase A/B policy. For mid-stream re-registration
        //    of an already-registered pubkey, `register_full` pushes
        //    another entry; MAX_SENDERS_PER_PUBKEY caps growth. Per D5,
        //    we count it as accepted.
        let pk = BlsPublicKeyBytes::new(wire_reg.message.pubkey);
        registry.register_full(
            pk,
            connection_id,
            header_tx.clone(),
            auction_conclusion_ms,
            rtt_tracker.clone(),
        );
        registered_pubkeys.push(pk);
        accepted += 1;
    }

    (accepted, rejected)
}

// ───────────────────────────────────────────────────────────────────────────
// Submit blinded block routing (D1, B4)
// ───────────────────────────────────────────────────────────────────────────

/// Decode + route a `SubmitBlindedBlock` frame to `_get_payload(V2)`. Maps
/// the `Result` to a `SubmitStatus` byte. Unknown/unsupported fork byte →
/// `InternalError` without closing the connection (D3).
/// Route a SubmitBlindedBlock from a WS client to the auctioneer.
/// Returns SubmitStatus. Duplicate submissions are idempotent — if the
/// auctioneer already has this payload, it returns Accepted without
/// re-processing.
async fn route_submit_blinded_block<A: Api>(
    api: &Arc<ProposerApi<A>>,
    fork_byte: u8,
    body_ssz: &[u8],
) -> SubmitStatus {
    let fork = match wire::fork_name_from_u8(fork_byte) {
        Ok(f) => f,
        Err(e) => {
            debug!(?e, fork_byte, "WS submit: invalid fork byte, returning InternalError");
            return SubmitStatus::InternalError;
        }
    };

    let signed_blinded_block: SignedBlindedBeaconBlock = match fork {
        ForkName::Electra => match SignedBlindedBeaconBlockElectra::from_ssz_bytes(body_ssz) {
            Ok(b) => b.into(),
            Err(err) => {
                warn!(?err, "WS submit: Electra SSZ decode failed");
                return SubmitStatus::InternalError;
            }
        },
        ForkName::Fulu => match SignedBlindedBeaconBlockFulu::from_ssz_bytes(body_ssz) {
            Ok(b) => b.into(),
            Err(err) => {
                warn!(?err, "WS submit: Fulu SSZ decode failed");
                return SubmitStatus::InternalError;
            }
        },
        ForkName::Gloas => match SignedBlindedBeaconBlockGloas::from_ssz_bytes(body_ssz) {
            Ok(b) => b.into(),
            Err(err) => {
                warn!(?err, "WS submit: Gloas SSZ decode failed");
                return SubmitStatus::InternalError;
            }
        },
        other => {
            debug!(?other, "WS submit: fork not supported on submit-block path");
            return SubmitStatus::InternalError;
        }
    };

    let mut trace = GetPayloadTrace::default();
    match api
        ._get_payload(signed_blinded_block, &mut trace, None, ProposerApiVersion::V2)
        .await
    {
        Ok(_) => SubmitStatus::Accepted,
        // Already delivering = already accepted (redundant WS submit after REST).
        Err(crate::api::proposer::error::ProposerApiError::DeliveringPayload) => {
            SubmitStatus::Accepted
        }
        Err(crate::api::proposer::error::ProposerApiError::ProposerNotRegistered) => {
            SubmitStatus::Rejected
        }
        Err(e) => {
            warn!(?e, "WS submit: _get_payload returned error");
            SubmitStatus::InternalError
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Pure-logic unit tests (integration-level tests live in tests.rs)
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod handler_unit_tests {
    use super::*;

    const NOW: u64 = 1_700_000_000;
    const SLOT_SEC: u64 = 12;
    const EPOCH_SEC: u64 = 32 * SLOT_SEC;

    #[test]
    fn test_accepts_fresh_registration_timestamp() {
        assert!(check_ws_timestamp_freshness(NOW, NOW));
    }

    #[test]
    fn test_accepts_stale_registration_timestamp() {
        // Lower bound is now effectively genesis_time (no WS-specific
        // age check). Old registrations pass as long as not future-dated.
        let old = NOW - 10 * EPOCH_SEC;
        assert!(check_ws_timestamp_freshness(old, NOW));
    }

    #[test]
    fn test_rejects_future_registration_timestamp() {
        // 30s into the future exceeds the +10s tolerance.
        assert!(!check_ws_timestamp_freshness(NOW + 30, NOW));
    }

    #[test]
    fn test_accepts_exactly_at_upper_bound() {
        // +10s exactly: accepted (window is inclusive).
        assert!(check_ws_timestamp_freshness(NOW + 10, NOW));
    }

    #[test]
    fn test_rejects_just_past_upper_bound() {
        // 1 second past the +10s future tolerance.
        assert!(!check_ws_timestamp_freshness(NOW + 11, NOW));
    }

    #[test]
    fn test_timestamp_check_genesis_era_accepted() {
        // Helix REST accepts timestamps back to genesis_time; WS now
        // matches that behavior.
        let genesis_ish = 1_606_824_023u64;
        assert!(check_ws_timestamp_freshness(genesis_ish, NOW));
    }

    #[test]
    fn test_auction_conclusion_clamp_low() {
        assert_eq!(clamp_auction_conclusion(Some(0)), Some(100));
        assert_eq!(clamp_auction_conclusion(Some(50)), Some(100));
        assert_eq!(clamp_auction_conclusion(Some(99)), Some(100));
    }

    #[test]
    fn test_auction_conclusion_clamp_high() {
        assert_eq!(clamp_auction_conclusion(Some(99_999)), Some(11_000));
        assert_eq!(clamp_auction_conclusion(Some(u64::MAX)), Some(11_000));
    }

    #[test]
    fn test_auction_conclusion_passes_through_in_range() {
        assert_eq!(clamp_auction_conclusion(Some(100)), Some(100));
        assert_eq!(clamp_auction_conclusion(Some(2_000)), Some(2_000));
        assert_eq!(clamp_auction_conclusion(Some(11_000)), Some(11_000));
    }

    #[test]
    fn test_auction_conclusion_missing_stays_none() {
        assert_eq!(clamp_auction_conclusion(None), None);
    }
}
