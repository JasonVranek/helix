//! WebSocket endpoint for the Builder API extension.

pub mod auth;
pub mod push;
pub mod wire;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::{
    Extension,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
};
use futures::StreamExt;
use tracing::{info, warn};

use crate::api::proposer::ProposerApi;
use crate::api::Api;
use auth::{AuthStrategy, ValidatorRegistrationAuth};
use ws_wire::framing;
use ws_wire::messages::*;

/// Handle WebSocket upgrade at `/eth/v1/builder/ws`.
pub async fn connect_ws<A: Api>(
    Extension(api): Extension<Arc<ProposerApi<A>>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, api))
}

async fn handle_connection<A: Api>(
    mut socket: WebSocket,
    api: Arc<ProposerApi<A>>,
) {
    let auth = ValidatorRegistrationAuth::new(
        api.local_cache.clone(),
        api.signing_context.clone(),
    );

    // Step 1: read first message (must be ValidatorRegistration)
    let first_msg: ValidatorRegistration = loop {
        match socket.next().await {
            Some(Ok(Message::Binary(data))) => {
                match framing::decode(&data) {
                    Ok(WsMessage::ValidatorRegistration(reg)) => break reg,
                    Ok(_) => { warn!("first WS msg not ValidatorRegistration"); return; }
                    Err(e) => { warn!(?e, "decode error"); return; }
                }
            }
            Some(Ok(Message::Close(_))) | None => return,
            Some(Err(e)) => { warn!(?e, "ws error"); return; }
            _ => continue,
        }
    };

    // Step 2: authenticate
    let acks = match auth.authenticate(first_msg).await {
        Ok(acks) => {
            for ack in &acks {
                let reply = framing::encode(&WsMessage::RegistrationAck(ack.clone()));
                if socket.send(Message::Binary(reply.into())).await.is_err() {
                    return;
                }
            }
            acks
        }
        Err(e) => {
            warn!(?e, "WS auth failed");
            return;
        }
    };

    let accepted: Vec<[u8; 48]> = acks.iter().filter(|a| a.accepted).map(|a| a.pubkey).collect();
    if accepted.is_empty() {
        warn!("no validators accepted");
        return;
    }
    info!(count = accepted.len(), "WS proposers connected");

    // Step 3: read loop
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(data))) => {
                let decoded = match framing::decode(&data) {
                    Ok(d) => d,
                    Err(e) => { warn!(?e, "decode"); continue; }
                };
                match decoded {
                    WsMessage::ValidatorRegistration(reg) => {
                        if let Ok(acks) = auth.authenticate(reg).await {
                            for ack in &acks {
                                let reply = framing::encode(&WsMessage::RegistrationAck(ack.clone()));
                                let _ = socket.send(Message::Binary(reply.into())).await;
                            }
                        }
                    }
                    WsMessage::Ping(p) => {
                        let pong = framing::encode(&WsMessage::Pong(Pong { nonce: p.nonce }));
                        let _ = socket.send(Message::Binary(pong.into())).await;
                    }
                    WsMessage::SubmitBlockRequest(_req) => {
                        let ack = SubmitBlockAck { status: SubmitStatus::Accepted as u8 };
                        let reply = framing::encode(&WsMessage::SubmitBlockAck(ack));
                        let _ = socket.send(Message::Binary(reply.into())).await;
                    }
                    WsMessage::ProposerPreference(_) => {
                        // Store per-connection preference later
                    }
                    _ => {}
                }
            }
            Some(Ok(Message::Close(_))) | None => {
                info!("WS connection closed");
                break;
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Err(e)) => {
                warn!(?e, "ws error");
                break;
            }
            _ => continue,
        }
    }
}
