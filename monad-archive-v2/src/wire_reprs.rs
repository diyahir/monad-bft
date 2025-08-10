use alloy_rlp::{RlpDecodable, RlpEncodable};

use crate::{
    model::{Tx, TxReceipt, TxTrace},
    versioned::{NoPastVersions, Versioned, VersionedError},
};

const FIRST_VERSION: u8 = 1;

#[derive(Debug, RlpEncodable, RlpDecodable)]
pub struct TxReceiptList(pub Vec<TxReceipt>);

impl Versioned for TxReceiptList {
    const VERSION: u8 = FIRST_VERSION;
    type Past = NoPastVersions;

    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
        match past {
            NoPastVersions => Err(VersionedError::PastUnknownVersion(0)),
        }
    }
}

#[derive(Debug, RlpEncodable, RlpDecodable)]
pub struct TxList(pub Vec<Tx>);

impl Versioned for TxList {
    const VERSION: u8 = FIRST_VERSION;
    type Past = NoPastVersions;

    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
        match past {
            NoPastVersions => Err(VersionedError::PastUnknownVersion(0)),
        }
    }
}

#[derive(Debug, RlpEncodable, RlpDecodable)]
pub struct TxTraceList(pub Vec<TxTrace>);

impl Versioned for TxTraceList {
    const VERSION: u8 = FIRST_VERSION;
    type Past = NoPastVersions;

    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
        match past {
            NoPastVersions => Err(VersionedError::PastUnknownVersion(0)),
        }
    }
}

impl Versioned for Tx {
    const VERSION: u8 = 1;
    type Past = NoPastVersions;

    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
        match past {
            NoPastVersions => Err(VersionedError::PastUnknownVersion(0)),
        }
    }
}

impl Versioned for TxReceipt {
    const VERSION: u8 = FIRST_VERSION;
    type Past = NoPastVersions;

    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
        match past {
            NoPastVersions => Err(VersionedError::PastUnknownVersion(0)),
        }
    }
}

impl Versioned for TxTrace {
    const VERSION: u8 = FIRST_VERSION;
    type Past = NoPastVersions;

    fn upgrade_from(past: Self::Past) -> Result<Self, VersionedError> {
        match past {
            NoPastVersions => Err(VersionedError::PastUnknownVersion(0)),
        }
    }
}
