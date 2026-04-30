//! Bid push dispatch service.

use std::collections::HashMap;
use std::sync::Arc;

use helix_types::BlsPublicKeyBytes;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::debug;
use ws_wire::messages::BidPush;

pub type BidPushSender = mpsc::Sender<BidPush>;

#[derive(Default)]
pub struct WsProposerRegistry {
    senders: RwLock<HashMap<BlsPublicKeyBytes, BidPushSender>>,
}

impl WsProposerRegistry {
    pub fn register(&self, pubkey: BlsPublicKeyBytes, sender: BidPushSender) {
        self.senders.write().insert(pubkey, sender);
    }

    pub fn unregister(&self, pubkey: &BlsPublicKeyBytes) {
        self.senders.write().remove(pubkey);
    }

    pub fn get(&self, pubkey: &BlsPublicKeyBytes) -> Option<BidPushSender> {
        self.senders.read().get(pubkey).cloned()
    }

    pub fn is_registered(&self, pubkey: &BlsPublicKeyBytes) -> bool {
        self.senders.read().contains_key(pubkey)
    }

    pub fn len(&self) -> usize {
        self.senders.read().len()
    }
}

/// Spawn the bid push service as a background task.
pub async fn run(
    _registry: Arc<WsProposerRegistry>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    debug!("ws push service started (Phase 2)");
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                debug!("ws push service shutting down");
                break;
            }
        }
    }
}
