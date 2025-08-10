//! Mongo -> semantic error mapping with ergonomic `From` and retry helpers.
//! Keep this in your Mongo backend module.

use mongodb::bson::{Bson, Document};
use mongodb::error::{
    Error as MongoError, ErrorKind, WriteFailure, RETRYABLE_WRITE_ERROR,
    TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT,
};
use std::io;

use crate::errors::{BoxError, NetworkError, ReaderError, WriterError};

/// Extension helpers to classify Mongo errors without leaking them outside the backend.
pub trait MongoErrorExt {
    /// Quick, backend-local retry for READS (server selection, pool cleared, DNS, IO).
    fn should_quick_retry_read(&self) -> bool;
    /// Quick, backend-local retry for WRITES (adds driver retry labels).
    fn should_quick_retry_write(&self) -> bool;

    /// True if commit result is ambiguous (label-based).
    fn is_commit_ambiguous(&self) -> bool;

    /// Duplicate-key (cover legacy codes too).
    fn is_duplicate_key(&self) -> bool;

    /// Write conflict codes.
    fn is_write_conflict(&self) -> bool;

    /// Timeout classification (IO timeout, server selection timeout, command timeout).
    fn is_timeout(&self) -> bool;

    /// Extract the top-level numeric code when present.
    fn mongo_code(&self) -> Option<i32>;
}

impl MongoErrorExt for MongoError {
    #[inline]
    fn should_quick_retry_read(&self) -> bool {
        matches!(
            &*self.kind,
            ErrorKind::DnsResolve { .. }
                | ErrorKind::Io(_)
                | ErrorKind::ServerSelection { .. }
                | ErrorKind::ConnectionPoolCleared { .. }
        )
    }

    #[inline]
    fn should_quick_retry_write(&self) -> bool {
        if self.contains_label(RETRYABLE_WRITE_ERROR)
            || self.contains_label(TRANSIENT_TRANSACTION_ERROR)
        {
            return true;
        }
        self.should_quick_retry_read()
    }

    #[inline]
    fn is_commit_ambiguous(&self) -> bool {
        self.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
    }

    #[inline]
    fn is_duplicate_key(&self) -> bool {
        matches!(self.mongo_code(), Some(11000 | 11001 | 12582))
    }

    #[inline]
    fn is_write_conflict(&self) -> bool {
        matches!(self.mongo_code(), Some(112 | 244))
    }

    #[inline]
    fn is_timeout(&self) -> bool {
        match &*self.kind {
            // IO timed out (socket)
            ErrorKind::Io(e) if e.kind() == io::ErrorKind::TimedOut => true,
            // Server selection exceeded time
            ErrorKind::ServerSelection { .. } => true,
            // Command timeouts (50 = ExceededTimeLimit/MaxTimeMSExpired, 89 = NetworkTimeout)
            ErrorKind::Command(e) => matches!(e.code, 50 | 89),
            // Write concern timeout - treat as timeout; call-site can refine to CommitAmbiguous at commit stage
            ErrorKind::Write(WriteFailure::WriteConcernError(e)) => {
                if let Some(details) = &e.details {
                    details.get_bool("wtimeout").unwrap_or(false)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    #[inline]
    fn mongo_code(&self) -> Option<i32> {
        match &*self.kind {
            ErrorKind::Command(e) => Some(e.code),
            ErrorKind::Write(WriteFailure::WriteError(e)) => Some(e.code),
            ErrorKind::BulkWrite(e) => e.write_errors.values().next().map(|we| we.code),
            ErrorKind::Write(WriteFailure::WriteConcernError(e)) => Some(e.code),
            _ => None,
        }
    }
}

#[inline]
fn boxed(err: MongoError) -> BoxError {
    Box::new(err)
}

fn key_from_details(details: Option<&Document>) -> Option<String> {
    // Prefer structured fields
    if let Some(doc) = details {
        if let Some(b) = doc.get("keyPattern").or_else(|| doc.get("keyValue")) {
            return Some(match b {
                Bson::Document(d) => d
                    .iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(","),
                other => other.to_string(),
            });
        }
        if let Some(Bson::String(name)) = doc.get("index") {
            return Some(name.clone());
        }
        if let Some(Bson::String(msg)) = doc.get("errmsg") {
            // Heuristic: "... index: <name> ..." - pull token after "index:"
            if let Some(pos) = msg.find("index:") {
                let tail = &msg[(pos + 6)..].trim_start();
                let candidate = tail
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_end_matches(',');
                if !candidate.is_empty() {
                    return Some(candidate.to_string());
                }
            }
        }
    }
    None
}

fn extract_conflict_key(err: &MongoError) -> Option<String> {
    match &*err.kind {
        ErrorKind::Write(WriteFailure::WriteError(e)) => key_from_details(e.details.as_ref()),
        ErrorKind::BulkWrite(e) => e
            .write_errors
            .values()
            .next()
            .and_then(|we| key_from_details(we.details.as_ref())),
        _ => None,
    }
}

/// From<MongoError> so `?` maps to semantic ReaderError automatically.
/// Use for general read operations after your short backend-local retries.
impl From<MongoError> for ReaderError {
    fn from(err: MongoError) -> Self {
        if err.is_timeout() {
            return ReaderError::Network(NetworkError::timeout(err));
        }
        if err.should_quick_retry_read() {
            return ReaderError::Network(NetworkError::Retryable { source: boxed(err) });
        }
        ReaderError::Other(eyre::eyre!("mongo error: {err}"))
    }
}

/// From<MongoError> so `?` maps to semantic WriterError automatically (non-commit context).
/// Use during write operations except the commit step; see `map_mongo_commit_error` for commit.
impl From<MongoError> for WriterError {
    fn from(err: MongoError) -> Self {
        if err.is_commit_ambiguous() {
            return WriterError::CommitAmbiguous { source: boxed(err) };
        }
        if err.is_duplicate_key() || err.is_write_conflict() {
            return WriterError::InvalidArgs { source: boxed(err) };
        }
        if err.is_timeout() {
            return WriterError::Network(NetworkError::timeout(err));
        }
        if err.should_quick_retry_write() {
            return WriterError::Network(NetworkError::Retryable { source: boxed(err) });
        }
        WriterError::Network(NetworkError::Other(err.into()))
    }
}

// Commit-stage specific mapping. Call this at the `commit_transaction` boundary
// after exhausting your bounded internal retries. Treat write concern timeouts
// as ambiguous (commit may have applied).
// pub(super) fn map_mongo_commit_error(err: MongoError) -> WriterError {
//     if err.is_commit_ambiguous() {
//         return WriterError::CommitAmbiguous {
//             source: Some(boxed(err)),
//         };
//     }
//     // Write concern timeout during commit - ambiguous.
//     if let ErrorKind::Write(WriteFailure::WriteConcernError(_)) = &*err.kind {
//         return WriterError::CommitAmbiguous {
//             source: Some(boxed(err)),
//         };
//     }
//     // Fallback to the general writer mapping.
//     WriterError::from(err)
// }
