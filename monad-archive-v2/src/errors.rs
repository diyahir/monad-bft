use std::{backtrace::Backtrace, borrow::Cow};

use thiserror::Error;

use crate::versioned::VersionedError;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type WriterResult = Result<(), WriterError>;
pub type ReaderResult<T> = Result<T, ReaderError>;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("authentication error")]
    Auth {
        #[source]
        source: BoxError,
    },
    #[error("rate limited")]
    RateLimited {
        #[source]
        source: BoxError,
    },
    #[error("timeout")]
    Timeout {
        #[source]
        source: BoxError,
    },
    #[error("retryable")]
    Retryable {
        #[source]
        source: BoxError,
    },
    #[error(transparent)]
    Other(BoxError),
}

impl NetworkError {
    pub fn auth<E: Into<BoxError>>(source: E) -> Self {
        Self::Auth {
            source: source.into(),
        }
    }

    pub fn rate_limited<E: Into<BoxError>>(source: E) -> Self {
        Self::RateLimited {
            source: source.into(),
        }
    }

    pub fn timeout<E: Into<BoxError>>(source: E) -> Self {
        Self::Timeout {
            source: source.into(),
        }
    }

    pub fn other<E: Into<BoxError>>(source: E) -> Self {
        Self::Other(source.into())
    }
}

#[derive(Error, Debug)]
pub enum KVError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("range bounds: Key={key} bytes=({start}..={end}) len={len}")]
    RangeBounds {
        key: String,
        start: u32,
        end: u32,
        len: usize,
    },
    #[error(transparent)]
    Network(#[from] NetworkError),
}

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("not found: {0}")]
    NotFound(Cow<'static, str>),

    #[error("invalid arguments: {0}")]
    InvalidArgs(BoxError),

    #[error("invalid data: {0}")]
    InvalidData(BoxError),

    #[error(transparent)]
    Network(#[from] NetworkError),

    #[error("RLP decode error: {type_name}")]
    RlpDecode {
        type_name: &'static str,
        #[source]
        source: alloy_rlp::Error,
    },

    #[error(transparent)]
    Versioned(#[from] VersionedError),

    #[error(transparent)]
    Other(eyre::Report),
}

impl ReaderError {
    pub fn rlp_decode(type_name: &'static str) -> impl FnOnce(alloy_rlp::Error) -> Self {
        move |source| Self::RlpDecode { type_name, source }
    }

    pub fn not_found(key: impl Into<Cow<'static, str>>) -> Self {
        Self::NotFound(key.into())
    }
}

impl From<KVError> for ReaderError {
    fn from(err: KVError) -> Self {
        match err {
            KVError::NotFound(key) => ReaderError::NotFound(key.into()),
            err @ KVError::RangeBounds { .. } => ReaderError::InvalidArgs(Box::new(err)),
            KVError::Network(err) => ReaderError::Network(err),
        }
    }
}

#[derive(Error, Debug)]
pub enum WriterError {
    #[error("encode failed: {type_name}")]
    Encode {
        type_name: &'static str,
        #[source]
        source: BoxError,
    },

    #[error("invalid arguments")]
    InvalidArgs {
        #[source]
        source: BoxError,
    },

    #[error("inconsistent tx/rx/trace lengths")]
    InconsistentTxRxTraceLengths {
        tx_len: u32,
        rx_len: u32,
        trace_len: u32,
    },

    #[error("precondition failed: {reason}")]
    InvariantViolated { reason: Cow<'static, str> },

    #[error("commit result unknown")]
    CommitAmbiguous {
        #[source]
        source: BoxError,
    },

    #[error(transparent)]
    Network(#[from] NetworkError),
}

impl WriterError {
    pub fn encode<E: Into<BoxError>>(type_name: &'static str) -> impl FnOnce(E) -> Self {
        move |source| Self::Encode {
            type_name,
            source: source.into(),
        }
    }
}

#[allow(non_snake_case)]
#[inline]
pub fn ROK<T>(value: T) -> ReaderResult<T> {
    Ok(value)
}

#[allow(non_snake_case)]
#[inline]
pub fn WOK() -> WriterResult {
    Ok(())
}
