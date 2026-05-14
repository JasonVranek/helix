#[cfg(test)]
mod wire_tests {
    use super::super::wire as wire;
    use alloy_primitives::Address;
    use helix_types::{
        BlsPublicKeyBytes, BlsSignatureBytes, ForkName, SignedValidatorRegistrationData,
        ValidatorRegistrationData,
    };
    use ws_wire::WireError;

    fn gen_native_signed_vr() -> SignedValidatorRegistrationData {
        SignedValidatorRegistrationData {
            message: ValidatorRegistrationData {
                fee_recipient: Address::new([0xaa; 20]),
                gas_limit: 30_000_000,
                timestamp: 1_700_000_000,
                pubkey: BlsPublicKeyBytes::new([0xbb; 48]),
            },
            signature: BlsSignatureBytes::from([0xcc; 96]),
        }
    }

    #[test]
    fn fork_name_to_u8_electra_fulu_gloas_only() {
        assert_eq!(wire::fork_name_to_u8(ForkName::Electra).unwrap(), 1);
        assert_eq!(wire::fork_name_to_u8(ForkName::Fulu).unwrap(), 2);
        assert_eq!(wire::fork_name_to_u8(ForkName::Gloas).unwrap(), 3);
    }

    #[test]
    fn fork_name_pre_electra_rejected() {
        for fork in [
            ForkName::Base,
            ForkName::Altair,
            ForkName::Bellatrix,
            ForkName::Capella,
            ForkName::Deneb,
        ] {
            assert!(
                matches!(wire::fork_name_to_u8(fork), Err(WireError::InvalidForkByte(_))),
                "pre-Electra fork {fork:?} must be rejected"
            );
        }
    }

    #[test]
    fn fork_name_roundtrip_supported() {
        for fork in [ForkName::Electra, ForkName::Fulu, ForkName::Gloas] {
            let b = wire::fork_name_to_u8(fork).unwrap();
            let back = wire::fork_name_from_u8(b).unwrap();
            assert_eq!(back, fork);
        }
    }

    #[test]
    fn fork_name_from_u8_zero_rejected() {
        assert!(matches!(
            wire::fork_name_from_u8(0),
            Err(WireError::InvalidForkByte(0))
        ));
    }

    #[test]
    fn fork_name_from_u8_unknown_rejected() {
        for b in [4u8, 5, 99, 255] {
            assert!(
                matches!(
                    wire::fork_name_from_u8(b),
                    Err(WireError::InvalidForkByte(_))
                ),
                "byte {b} must be rejected"
            );
        }
    }

    #[test]
    fn native_registration_roundtrip_to_wire() {
        let native = gen_native_signed_vr();
        let wire_reg = wire::native_to_wire_registration(&native);
        let back = wire::wire_to_native_registration(&wire_reg);
        assert_eq!(back.message.fee_recipient, native.message.fee_recipient);
        assert_eq!(back.message.gas_limit, native.message.gas_limit);
        assert_eq!(back.message.timestamp, native.message.timestamp);
        assert_eq!(back.message.pubkey, native.message.pubkey);
    }
}

// Verify `Route::WebSocket` is registered in the RouterConfig defaults
// and resolves to the canonical `/eth/v1/builder/ws` path. Catches a
// regression where the route is dropped from enabled_routes or the path
// constant is accidentally renamed.
#[cfg(test)]
mod route_tests {
    use helix_common::api::PATH_WEBSOCKET;
    use helix_common::config::Route;

    #[test]
    fn websocket_route_maps_to_canonical_path() {
        assert_eq!(Route::WebSocket.path(), PATH_WEBSOCKET);
        assert_eq!(PATH_WEBSOCKET, "/eth/v1/builder/ws");
    }
}

// Registry tests live here too — they exercise the v3.2 1:N shape with
// connection_id support. Implemented after push.rs is rewritten.
#[cfg(test)]
mod registry_tests {
    use super::super::push::WsProposerRegistry;
    use helix_types::BlsPublicKeyBytes;
    use tokio::sync::mpsc;

    fn pk(byte: u8) -> BlsPublicKeyBytes {
        BlsPublicKeyBytes::new([byte; 48])
    }

    #[tokio::test]
    async fn test_register_allocates_increasing_ids() {
        let registry = WsProposerRegistry::default();
        let id1 = registry.allocate_connection_id();
        let id2 = registry.allocate_connection_id();
        assert!(id2 > id1);
    }

    #[tokio::test]
    async fn test_get_all_returns_all_senders_for_pubkey() {
        let registry = WsProposerRegistry::default();
        let p = pk(0xaa);
        for _ in 0..3 {
            let (tx, _rx) = mpsc::unbounded_channel();
            let id = registry.allocate_connection_id();
            registry.register(p, id, tx);
        }
        let senders = registry.get_all(&p);
        assert_eq!(senders.len(), 3);
    }

    #[tokio::test]
    async fn test_unregister_sender_targeted() {
        let registry = WsProposerRegistry::default();
        let p = pk(0xbb);
        let (tx1, _rx1) = mpsc::unbounded_channel();
        let (tx2, _rx2) = mpsc::unbounded_channel();
        let (tx3, _rx3) = mpsc::unbounded_channel();
        let id1 = registry.allocate_connection_id();
        let id2 = registry.allocate_connection_id();
        let id3 = registry.allocate_connection_id();
        registry.register(p, id1, tx1);
        registry.register(p, id2, tx2);
        registry.register(p, id3, tx3);
        // Remove only the middle entry.
        registry.unregister_sender(&p, id2);
        // 2 senders remain; id1 and id3 still present.
        assert_eq!(registry.get_all(&p).len(), 2);
        assert!(registry.is_registered(&p));
    }

    #[tokio::test]
    async fn test_overflow_evicts_oldest() {
        let registry = WsProposerRegistry::default();
        let p = pk(0xcc);
        let mut rxs = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..9 {
            let (tx, rx) = mpsc::unbounded_channel::<(u8, Vec<u8>)>();
            let id = registry.allocate_connection_id();
            registry.register(p, id, tx);
            rxs.push(rx);
            ids.push(id);
        }
        // Cap is 8; 9th register must have evicted the oldest.
        assert_eq!(registry.get_all(&p).len(), 8);

        // The evicted sender (id=ids[0]) should observe its channel close.
        let mut oldest_rx = rxs.remove(0);
        // Yield so the dropped sender propagates.
        tokio::task::yield_now().await;
        match oldest_rx.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {}
            other => panic!("evicted receiver expected Disconnected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_empty_after_last_unregister() {
        let registry = WsProposerRegistry::default();
        let p = pk(0xdd);
        let (tx, _rx) = mpsc::unbounded_channel();
        let id = registry.allocate_connection_id();
        registry.register(p, id, tx);
        registry.unregister_sender(&p, id);
        assert!(!registry.is_registered(&p));
        assert_eq!(registry.get_all(&p).len(), 0);
    }
}
