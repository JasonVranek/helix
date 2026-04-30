//! Debug API endpoints — only mounted when `--debug-api` is set.
//! Used by the local-interop test harness (Phase 3.5).

use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};

use alloy_primitives::{Address, B256, U256};
use axum::{
    Extension, Router,
    extract::Json,
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::debug;

use helix_common::api::builder_api::TopBidUpdate;
use helix_types::BlsPublicKeyBytes;

// ---------------------------------------------------------------------------
// Shared debug state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DebugState {
    /// Whether the WebSocket endpoint accepts new connections.
    pub ws_enabled: Arc<AtomicBool>,

    /// Total count of successful WS validator registrations since startup.
    pub ws_registration_count: Arc<AtomicU64>,

    /// Sender for injecting synthetic top-bid updates.
    pub top_bid_tx: broadcast::Sender<TopBidUpdate>,
}

impl DebugState {
    pub fn new(top_bid_tx: broadcast::Sender<TopBidUpdate>) -> Self {
        Self {
            ws_enabled: Arc::new(AtomicBool::new(true)),
            ws_registration_count: Arc::new(AtomicU64::new(0)),
            top_bid_tx,
        }
    }
}

// ---------------------------------------------------------------------------
// Request/response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct InjectBidRequest {
    slot: u64,
    parent_hash: String,   // hex-encoded 32 bytes
    value_gwei: u64,
}

#[derive(Deserialize)]
struct ToggleWsRequest {
    enabled: bool,
}

#[derive(Serialize, Deserialize)]
struct WsRegistrationsResponse {
    count: u64,
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn inject_bid(
    Extension(state): Extension<Arc<DebugState>>,
    Json(req): Json<InjectBidRequest>,
) -> StatusCode {
    let parent_hash = match alloy_primitives::hex::decode(&req.parent_hash) {
        Ok(v) if v.len() == 32 => B256::from_slice(&v),
        _ => {
            debug!("invalid parent_hash in inject_bid");
            return StatusCode::BAD_REQUEST;
        }
    };

    let update = TopBidUpdate {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        slot: req.slot,
        block_number: 0,
        block_hash: B256::ZERO,
        parent_hash,
        builder_pubkey: BlsPublicKeyBytes::ZERO,
        fee_recipient: Address::ZERO,
        value: U256::from(req.value_gwei) * U256::from(1_000_000_000u64),
    };

    debug!(slot = req.slot, value_gwei = req.value_gwei, "injecting synthetic bid");
    let _ = state.top_bid_tx.send(update);
    StatusCode::OK
}

async fn toggle_ws(
    Extension(state): Extension<Arc<DebugState>>,
    Json(req): Json<ToggleWsRequest>,
) -> Json<serde_json::Value> {
    state.ws_enabled.store(req.enabled, Ordering::Relaxed);
    debug!(enabled = req.enabled, "WebSocket toggled");
    Json(serde_json::json!({ "ws_enabled": req.enabled }))
}

async fn ws_registrations(
    Extension(state): Extension<Arc<DebugState>>,
) -> Json<WsRegistrationsResponse> {
    Json(WsRegistrationsResponse { count: state.ws_registration_count.load(Ordering::Relaxed) })
}

// ---------------------------------------------------------------------------
// Router mounting
// ---------------------------------------------------------------------------

pub fn mount(router: Router, state: Arc<DebugState>) -> Router {
    router
        .route("/debug/inject_bid", post(inject_bid))
        .route("/debug/toggle_ws", post(toggle_ws))
        .route("/debug/ws_registrations", get(ws_registrations))
        .layer(Extension(state))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn make_test_state() -> Arc<DebugState> {
        let (tx, _) = broadcast::channel(4);
        Arc::new(DebugState::new(tx))
    }

    async fn body_string(body: Body) -> String {
        let bytes = axum::body::to_bytes(body, 1024).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn debug_endpoints_available_when_mounted() {
        let state = make_test_state();
        let app = mount(axum::Router::new(), state);

        // GET /debug/ws_registrations
        let resp = app
            .clone()
            .oneshot(Request::builder().uri("/debug/ws_registrations").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn debug_ws_registrations_counter_increments() {
        let state = make_test_state();
        state.ws_registration_count.store(5, Ordering::Relaxed);

        let app = mount(axum::Router::new(), state);
        let resp = app
            .oneshot(Request::builder().uri("/debug/ws_registrations").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp.into_body()).await;
        let parsed: WsRegistrationsResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed.count, 5);
    }

    #[tokio::test]
    async fn debug_toggle_ws_disables_and_enables() {
        let state = make_test_state();
        let app = mount(axum::Router::new(), state.clone());

        // Disable
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/debug/toggle_ws")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"enabled":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!state.ws_enabled.load(Ordering::Relaxed));

        // Enable
        let resp2 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/debug/toggle_ws")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"enabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        assert!(state.ws_enabled.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn debug_inject_bid_sends_top_bid_update() {
        let (tx, mut rx) = broadcast::channel(4);
        let state = Arc::new(DebugState::new(tx));
        let app = mount(axum::Router::new(), state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/debug/inject_bid")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"slot":100,"parent_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value_gwei":42}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Should have received the broadcast
        let update = rx.try_recv().unwrap();
        assert_eq!(update.slot, 100);
        assert_eq!(update.value, U256::from(42) * U256::from(1_000_000_000u64));
    }
}
