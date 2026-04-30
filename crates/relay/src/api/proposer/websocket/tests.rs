#[cfg(test)]
mod wire_tests {
    use super::super::wire;
    use alloy_primitives::Address;
    use helix_types::{
        BlsPublicKeyBytes, BlsSignatureBytes, ForkName,
        SignedValidatorRegistrationData, ValidatorRegistrationData,
    };

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
    fn fork_name_to_u8_maps_all_forks() {
        assert_eq!(wire::fork_name_to_u8(ForkName::Electra), 1);
        assert_eq!(wire::fork_name_to_u8(ForkName::Fulu), 2);
        assert_eq!(wire::fork_name_to_u8(ForkName::Deneb), 0);
    }

    #[test]
    fn fork_name_roundtrip() {
        for (fork, expected) in &[(ForkName::Electra, 1u8), (ForkName::Fulu, 2u8)] {
            let b = wire::fork_name_to_u8(*fork);
            assert_eq!(b, *expected);
            let back = wire::fork_name_from_u8(b).unwrap();
            assert_eq!(back, *fork);
        }
    }

    #[test]
    fn native_registration_roundtrip_to_wire() {
        let native = gen_native_signed_vr();
        let wire_reg = wire::native_to_wire_registration(&native);
        let back = wire::wire_to_native_registration(&wire_reg).unwrap();
        assert_eq!(back.message.fee_recipient, native.message.fee_recipient);
        assert_eq!(back.message.gas_limit, native.message.gas_limit);
        assert_eq!(back.message.timestamp, native.message.timestamp);
        assert_eq!(back.message.pubkey, native.message.pubkey);
    }

    #[test]
    fn unknown_fork_discriminant_rejected() {
        assert!(wire::fork_name_from_u8(0xff).is_err());
    }
}

#[cfg(test)]
mod registry_tests {
    use super::super::push::WsProposerRegistry;
    use helix_types::BlsPublicKeyBytes;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn registry_register_and_lookup() {
        let registry = WsProposerRegistry::default();
        let pk = BlsPublicKeyBytes::new([0xaa; 48]);
        let (tx, _rx) = mpsc::channel(4);
        registry.register(pk, tx);
        assert!(registry.get(&pk).is_some());
        assert!(registry.is_registered(&pk));
    }

    #[tokio::test]
    async fn registry_unregister_removes() {
        let registry = WsProposerRegistry::default();
        let pk = BlsPublicKeyBytes::new([0xbb; 48]);
        let (tx, _rx) = mpsc::channel(4);
        registry.register(pk, tx);
        registry.unregister(&pk);
        assert!(registry.get(&pk).is_none());
    }

    #[tokio::test]
    async fn registry_second_insert_replaces_first() {
        let registry = WsProposerRegistry::default();
        let pk = BlsPublicKeyBytes::new([0xcc; 48]);
        let (tx1, _) = mpsc::channel(4);
        let (tx2, _) = mpsc::channel(4);
        registry.register(pk, tx1);
        registry.register(pk, tx2);
        assert_eq!(registry.len(), 1);
    }
}
