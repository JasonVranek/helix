//! Authentication strategies for WebSocket connections.

use std::sync::Arc;
use async_trait::async_trait;
use helix_common::{
    local_cache::LocalCache,
    signing::RelaySigningContext,
};
use ws_wire::messages::{RegistrationAck, ValidatorRegistration};

use super::wire;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("unexpected message type at auth step")]
    UnexpectedMessage,
    #[error("signature verification failed: {0}")]
    Signature(String),
    #[error("unknown validator: {0}")]
    UnknownValidator(String),
    #[error("stale or future timestamp")]
    StaleTimestamp,
    #[error("empty batch")]
    EmptyBatch,
}

/// Trait for authenticating the first WebSocket message (a ValidatorRegistration).
#[async_trait]
pub trait AuthStrategy: Send + Sync + 'static {
    async fn authenticate(
        &self,
        msg: ValidatorRegistration,
    ) -> Result<Vec<RegistrationAck>, AuthError>;
}

/// Standard authentication: BLS signature check + timestamp + known-validator lookup.
pub struct ValidatorRegistrationAuth {
    pub local_cache: Arc<LocalCache>,
    pub signing_context: Arc<RelaySigningContext>,
}

impl ValidatorRegistrationAuth {
    pub fn new(
        local_cache: Arc<LocalCache>,
        signing_context: Arc<RelaySigningContext>,
    ) -> Self {
        Self { local_cache, signing_context }
    }
}

#[async_trait]
impl AuthStrategy for ValidatorRegistrationAuth {
    async fn authenticate(
        &self,
        msg: ValidatorRegistration,
    ) -> Result<Vec<RegistrationAck>, AuthError> {
        if msg.registrations.is_empty() {
            return Err(AuthError::EmptyBatch);
        }

        let now_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        const TIMESTAMP_WINDOW_SEC: i64 = 12;
        let known_validators = self.local_cache.known_validators_cache.read();
        let builder_domain = self.signing_context.chain_info.builder_domain;
        let mut acks = Vec::with_capacity(msg.registrations.len());

        for wire_reg in &msg.registrations {
            let pubkey_bytes = wire_reg.message.pubkey;
            let pk = helix_types::BlsPublicKeyBytes::new(pubkey_bytes);

            // 1. Timestamp freshness
            let delta = now_sec - wire_reg.message.timestamp as i64;
            if delta < -TIMESTAMP_WINDOW_SEC || delta > TIMESTAMP_WINDOW_SEC {
                acks.push(wire::rejected_ack(pubkey_bytes, "stale timestamp"));
                continue;
            }

            // 2. Known validator check
            if !known_validators.contains(&pk) {
                acks.push(wire::rejected_ack(pubkey_bytes, "unknown validator"));
                continue;
            }

            // 3. Convert to native and verify BLS signature
            let native = match wire::wire_to_native_registration(wire_reg) {
                Ok(n) => n,
                Err(e) => {
                    acks.push(wire::rejected_ack(pubkey_bytes, &format!("decode: {}", e)));
                    continue;
                }
            };

            match native.verify_signature(builder_domain) {
                Ok(()) => {
                    acks.push(wire::accepted_ack(pubkey_bytes, 0));
                }
                Err(e) => {
                    acks.push(wire::rejected_ack(pubkey_bytes, &format!("invalid sig: {}", e)));
                }
            }
        }

        Ok(acks)
    }
}
