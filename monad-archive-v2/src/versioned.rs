use std::borrow::Cow;

use thiserror::Error;

use alloy_rlp::{Decodable, Encodable};
use mongodb::bson::{self, Bson};

use crate::errors::BoxError;

/// 1-byte prefix + RLP payload. No inner allocation, upgrade path via Past enum.
pub trait Versioned: Sized + Encodable + Decodable {
    /// Current schema version number.
    const VERSION: u8;

    /// Enum (or other type) representing all prior schemas.
    type Past: PastDecode;

    /// RLP encode with a 1-byte version prefix.
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256); // heuristic to reduce reallocs
        out.push(Self::VERSION); // raw version tag
        self.encode(&mut out); // append RLP of current schema
        out
    }

    /// Convenience: wrap as BSON binary for storage.
    fn to_bson(&self) -> Bson {
        Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: self.to_bytes(),
        })
    }

    /// RLP decode with a 1-byte version prefix.
    fn from_bytes(bytes: &[u8]) -> Result<Self, VersionedError> {
        if bytes.is_empty() {
            return Err(VersionedError::EmptyInput);
        }

        let version = bytes[0];
        let mut tail = &bytes[1..];

        if version == Self::VERSION {
            let value = Self::decode(&mut tail).map_err(VersionedError::RlpDecode)?;
            if !tail.is_empty() {
                return Err(VersionedError::TrailingBytes(tail.len()));
            }
            Ok(value)
        } else if version < Self::VERSION {
            let past = Self::Past::decode_by_version(version, &mut tail)?;
            let value = Self::upgrade_from(past)?;
            if !tail.is_empty() {
                return Err(VersionedError::TrailingBytes(tail.len()));
            }
            Ok(value)
        } else {
            Err(VersionedError::FutureUnknownVersion(version))
        }
    }

    /// Upgrade a prior schema value into the current schema.
    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError>;
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum VersionedError {
    #[error("RLP decode error: {0}")]
    RlpDecode(#[from] alloy_rlp::Error),

    #[error("empty input")]
    EmptyInput,

    #[error("future or unknown version: {0}")]
    FutureUnknownVersion(u8),

    #[error("no decoder for past version: {0}")]
    PastUnknownVersion(u8),

    #[error("failed to decode past version {version}: {source}")]
    PastDecodeFailed {
        version: u8,
        #[source]
        source: alloy_rlp::Error,
    },

    #[error("trailing bytes after payload: {0}")]
    TrailingBytes(usize),

    #[error("other: {msg}")]
    Other {
        msg: Cow<'static, str>,
        #[source]
        source: BoxError,
    },
}

impl VersionedError {
    pub fn other<E: Into<BoxError>>(msg: impl Into<Cow<'static, str>>) -> impl FnOnce(E) -> Self {
        |e| Self::Other {
            msg: msg.into(),
            source: e.into(),
        }
    }
}

/// Implemented for the enum that collects all older schemas.
/// The enum will get this via the `versioned_enum!` macro below.
pub trait PastDecode: Sized {
    fn decode_by_version(version: u8, tail: &mut &[u8]) -> Result<Self, VersionedError>;
}

pub struct NoPastVersions;

impl PastDecode for NoPastVersions {
    fn decode_by_version(version: u8, _tail: &mut &[u8]) -> Result<Self, VersionedError> {
        Err(VersionedError::PastUnknownVersion(version))
    }
}

/// Small macro that turns a simple version map into:
/// - An enum of past types
/// - An impl PastDecode for that enum using Decodable of each variant's type
#[macro_export]
macro_rules! versioned_enum {
    ( $(#[$meta:meta])* enum $Name:ident { $( $ver:literal => $Variant:ident ( $Ty:ty ) ),+ $(,)? } ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        enum $Name {
            $( $Variant($Ty) ),+
        }

        impl $crate::versioned::PastDecode for $Name {
            fn decode_by_version(version: u8, tail: &mut &[u8]) -> Result<Self, $crate::versioned::VersionedError> {
                match version {
                    $( $ver => {
                        match < $Ty as ::alloy_rlp::Decodable >::decode(tail) {
                            Ok(v) => Ok(Self::$Variant(v)),
                            Err(e) => Err($crate::versioned::VersionedError::PastDecodeFailed { version, source: e }),
                        }
                    } ),+,
                    _ => Err($crate::versioned::VersionedError::PastUnknownVersion(version)),
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioned_enum;
    use alloy_rlp::{RlpDecodable, RlpEncodable};

    #[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
    struct DummyV0 {
        id: u16,
        data: Vec<u8>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
    struct DummyV1 {
        id: u32,
        payload: Vec<u8>,
        checksum: u8,
    }

    // Define the "registry" of past versions with a single line
    versioned_enum! {
        /// All supported historical Dummy schemas
        enum PastDummy {
            0 => V0(DummyV0)
        }
    }

    impl super::Versioned for DummyV1 {
        const VERSION: u8 = 1;
        type Past = PastDummy;

        fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
            match past {
                PastDummy::V0(v0) => {
                    // Move out fields to avoid clones
                    let DummyV0 { id, data } = v0;
                    let checksum = checksum8(&data);
                    Ok(DummyV1 {
                        id: id as u32,
                        payload: data,
                        checksum,
                    })
                }
            }
        }
    }

    fn checksum8(data: &[u8]) -> u8 {
        data.iter().fold(0u8, |a, &b| a.wrapping_add(b))
    }

    #[test]
    fn roundtrip_current_v1() {
        let v1 = DummyV1 {
            id: 42,
            payload: vec![1, 2, 3],
            checksum: checksum8(&[1, 2, 3]),
        };
        let bytes = v1.to_bytes();
        let back = DummyV1::from_bytes(&bytes).unwrap();
        assert_eq!(v1, back);
    }

    #[test]
    fn upgrade_from_v0_to_v1() {
        let v0 = DummyV0 {
            id: 7,
            data: vec![9, 9, 9],
        };
        // Manually encode a v0 blob: prefix 0, then RLP of DummyV0
        let mut blob = Vec::new();
        blob.push(0);
        v0.encode(&mut blob);

        let up = DummyV1::from_bytes(&blob).unwrap();
        assert_eq!(up.id, 7);
        assert_eq!(up.payload, vec![9, 9, 9]);
        assert_eq!(up.checksum, checksum8(&[9, 9, 9]));
    }

    #[test]
    fn future_version_rejected() {
        let v1 = DummyV1 {
            id: 1,
            payload: vec![],
            checksum: 0,
        };
        let mut blob = v1.to_bytes();
        blob[0] = 9; // pretend future
        let err = DummyV1::from_bytes(&blob).unwrap_err();
        assert!(matches!(err, VersionedError::FutureUnknownVersion(9)));
    }

    #[test]
    fn empty_or_corrupt_inputs() {
        // empty
        let err = DummyV1::from_bytes(&[]).unwrap_err();
        assert!(matches!(err, VersionedError::EmptyInput));

        // corrupt (truncate tail RLP)
        let v1 = DummyV1 {
            id: 5,
            payload: vec![1],
            checksum: 1,
        };
        let mut blob = v1.to_bytes();
        blob.pop(); // truncate
        let err = DummyV1::from_bytes(&blob).unwrap_err();
        assert!(matches!(err, VersionedError::RlpDecode(_)));
    }

    #[test]
    fn trailing_bytes_are_rejected_current() {
        let v1 = DummyV1 {
            id: 10,
            payload: vec![1, 2],
            checksum: checksum8(&[1, 2]),
        };
        let mut blob = v1.to_bytes();
        blob.push(0); // extra junk
        let err = DummyV1::from_bytes(&blob).unwrap_err();
        assert!(matches!(err, VersionedError::TrailingBytes(1)));
    }

    #[test]
    fn trailing_bytes_are_rejected_past() {
        // Encode as version 0 (past), then add junk
        let v0 = DummyV0 {
            id: 3,
            data: vec![4, 5],
        };
        let mut blob = Vec::new();
        blob.push(0);
        v0.encode(&mut blob);
        blob.push(0); // junk byte

        let err = DummyV1::from_bytes(&blob).unwrap_err();
        assert!(matches!(err, VersionedError::TrailingBytes(1)));
    }
}
