//! Fork-byte mapping + ws-wire ↔ helix-types conversions for the Builder
//! API WebSocket extension.
//!
//! ARCH v3.2 §2.3 / §5: Electra=1, Fulu=2, Gloas=3. Pre-Electra forks and
//! wire byte 0 are invalid. Fork encoding is delegated to ws_wire::framing.

use alloy_primitives::Address;
use helix_types::{
    BlsPublicKeyBytes, BlsSignatureBytes, ForkName, SignedValidatorRegistrationData,
};
use ws_wire::WireError;
use ws_wire::messages::SignedValidatorRegistrationV1;
#[cfg(test)]
use ws_wire::messages::SubscriptionRequest as WireVrd;

// ---------------------------------------------------------------------------
// Fork name <-> u8 (thin adapters delegating to ws_wire)
// ---------------------------------------------------------------------------

pub fn fork_name_to_u8(fork: ForkName) -> Result<u8, WireError> {
    use ForkName::*;
    let name = match fork {
        Electra => "electra",
        Fulu => "fulu",
        Gloas => "gloas",
        _other => return Err(WireError::InvalidForkByte(0)),
    };
    ws_wire::framing::fork_name_to_u8(name)
}

pub fn fork_name_from_u8(v: u8) -> Result<ForkName, WireError> {
    match ws_wire::framing::fork_name_from_u8(v)? {
        "electra" => Ok(ForkName::Electra),
        "fulu" => Ok(ForkName::Fulu),
        "gloas" => Ok(ForkName::Gloas),
        _ => unreachable!("ws_wire only returns known fork names"),
    }
}

// ---------------------------------------------------------------------------
// Registration conversion
// ---------------------------------------------------------------------------

/// Convert a native Helix registration to the ws-wire specs-layout struct.
/// Kept available for tests and future outgoing-registration needs;
/// Helix today only receives registrations, never sends them over WS.
#[cfg(test)]
pub fn native_to_wire_registration(
    reg: &SignedValidatorRegistrationData,
) -> SignedValidatorRegistrationV1 {
    SignedValidatorRegistrationV1 {
        message: WireVrd {
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

/// Convert a ws-wire registration back to the native Helix type for
/// BLS-verification and registry storage.
pub fn wire_to_native_registration(
    wire: &SignedValidatorRegistrationV1,
) -> SignedValidatorRegistrationData {
    let message = helix_types::ValidatorRegistrationData {
        fee_recipient: Address::new(wire.message.fee_recipient),
        gas_limit: wire.message.gas_limit,
        timestamp: wire.message.timestamp,
        pubkey: BlsPublicKeyBytes::new(wire.message.pubkey),
    };
    let signature = BlsSignatureBytes::from(wire.signature);
    SignedValidatorRegistrationData { message, signature }
}
