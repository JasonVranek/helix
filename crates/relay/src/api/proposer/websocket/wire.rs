//! Conversion layer between helix_types and ws-wire wire types.

use helix_types::{BlsPublicKeyBytes, ForkName, SignedValidatorRegistrationData, BlsSignatureBytes};
use ws_wire::messages::*;

pub fn fork_name_to_u8(fork: ForkName) -> u8 {
    match fork {
        ForkName::Base | ForkName::Altair | ForkName::Bellatrix
        | ForkName::Capella | ForkName::Deneb => 0,
        ForkName::Electra => 1,
        ForkName::Fulu => 2,
        ForkName::Gloas => 3,
    }
}

pub fn fork_name_from_u8(v: u8) -> Result<ForkName, String> {
    match v {
        0 | 1 => Ok(ForkName::Electra),
        2 => Ok(ForkName::Fulu),
        3 => Ok(ForkName::Gloas),
        _ => Err(format!("unknown fork discriminant: {}", v)),
    }
}

pub fn native_to_wire_registration(
    reg: &SignedValidatorRegistrationData,
) -> WireSignedValidatorRegistration {
    WireSignedValidatorRegistration {
        message: WireValidatorRegistrationData {
            fee_recipient: *reg.message.fee_recipient.as_ref(),
            gas_limit: reg.message.gas_limit,
            timestamp: reg.message.timestamp,
            pubkey: *reg.message.pubkey,
        },
        signature: {
            let mut sig = [0u8; 96];
            let src: &[u8] = reg.signature.as_ref();
            let len = src.len().min(96);
            sig[..len].copy_from_slice(&src[..len]);
            sig
        },
    }
}

pub fn wire_to_native_registration(
    wire: &WireSignedValidatorRegistration,
) -> Result<SignedValidatorRegistrationData, String> {
    use alloy_primitives::Address;
    let message = helix_types::ValidatorRegistrationData {
        fee_recipient: Address::new(wire.message.fee_recipient),
        gas_limit: wire.message.gas_limit,
        timestamp: wire.message.timestamp,
        pubkey: BlsPublicKeyBytes::new(wire.message.pubkey),
    };
    let sig_bytes = BlsSignatureBytes::from(
        <[u8; 96]>::try_from(&wire.signature[..96]).map_err(|e| format!("sig: {}", e))?
    );
    Ok(SignedValidatorRegistrationData { message, signature: sig_bytes })
}

pub fn native_bid_to_wire(
    _bid: &helix_types::SignedBidSubmission,
    fork_name: ForkName,
    slot: u64,
    parent_hash: [u8; 32],
) -> BidPush {
    BidPush::new_empty(slot, parent_hash, fork_name_to_u8(fork_name))
}

pub fn accepted_ack(pubkey: [u8; 48], validator_index: u64) -> RegistrationAck {
    RegistrationAck::new_accepted(pubkey, validator_index)
}

pub fn rejected_ack(pubkey: [u8; 48], reason: &str) -> RegistrationAck {
    RegistrationAck::new_rejected(pubkey, reason)
}
