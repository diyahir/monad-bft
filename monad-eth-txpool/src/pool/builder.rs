// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    marker::PhantomData,
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::{keccak256, B256};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_crypto::signing_domain;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};


/// Signing domain for block builder transaction bundles
pub struct BlockBuilderDomain;
impl signing_domain::SigningDomain for BlockBuilderDomain {
    const PREFIX: &'static [u8] = b"MONAD_BLOCK_BUILDER_v1";
}

/// A cryptographically signed bundle of transactions from a block builder
#[derive(Debug, Clone)]
pub struct SignedBuilderTxBundle<ST: CertificateSignatureRecoverable> {
    /// The transactions in this bundle
    pub transactions: Vec<Recovered<TxEnvelope>>,
    /// Signature over the bundle hash
    pub signature: ST,
    /// The signer's public key
    pub signer: CertificateSignaturePubKey<ST>,
    /// Timestamp when bundle was created (for replay protection)
    pub timestamp: u64,
}

impl<ST: CertificateSignatureRecoverable> SignedBuilderTxBundle<ST> {
    /// Compute the hash that should be signed for this bundle
    pub fn compute_bundle_hash(&self) -> B256 {
        let mut data = Vec::new();
        
        // Hash transaction data
        for tx in &self.transactions {
            data.extend_from_slice(tx.tx_hash().as_slice());
        }
        
        // Include timestamp for replay protection
        data.extend_from_slice(&self.timestamp.to_be_bytes());
        
        keccak256(data)
    }
    
    /// Verify the signature on this bundle
    pub fn verify_signature(&self) -> bool {
        let bundle_hash = self.compute_bundle_hash();
        self.signature
            .verify::<BlockBuilderDomain>(&bundle_hash.0, &self.signer)
            .is_ok()
    }
}

/// Wire format for submitting builder transaction bundles via API
#[derive(Debug, Serialize, Deserialize)]
pub struct BuilderTxBundleRequest {
    /// Array of transaction hex strings
    pub transactions: Vec<String>,
    /// Signature hex string  
    pub signature: String,
    /// Signer public key hex string
    pub signer: String,
    /// Unix timestamp
    pub timestamp: u64,
}

/// Storage for block builder transactions that should be prioritized
#[derive(Debug, Clone)]
pub struct BlockBuilderTxPool<ST: CertificateSignatureRecoverable> {
    /// Raw transactions ready for inclusion (validation happens at proposal time)
    transactions: VecDeque<Recovered<TxEnvelope>>,
    /// Maximum number of builder transactions to store
    max_size: usize,
    /// Authorized builder public keys
    authorized_builders: HashSet<CertificateSignaturePubKey<ST>>,
    /// Maximum bundle age for replay protection
    max_bundle_age_secs: u64,
    /// Recently seen bundle hashes (simple replay protection)
    recent_bundles: HashMap<B256, u64>, // hash -> timestamp
    /// For generic parameter tracking
    _phantom: PhantomData<ST>,
}

impl<ST: CertificateSignatureRecoverable> BlockBuilderTxPool<ST> {
    /// Create a new block builder transaction pool
    pub fn new(
        max_size: usize,
        authorized_builders: Vec<CertificateSignaturePubKey<ST>>,
        max_bundle_age_secs: u64,
    ) -> Self {
        Self {
            transactions: VecDeque::with_capacity(max_size),
            max_size,
            authorized_builders: authorized_builders.into_iter().collect(),
            max_bundle_age_secs,
            recent_bundles: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Update the authorized builders list
    pub fn update_authorized_builders(
        &mut self,
        authorized_builders: Vec<CertificateSignaturePubKey<ST>>,
    ) {
        self.authorized_builders = authorized_builders.into_iter().collect();
    }

    /// Add a signed bundle of transactions
    pub fn add_signed_bundle(
        &mut self,
        bundle: SignedBuilderTxBundle<ST>,
        current_time: u64,
    ) -> Result<usize, BuilderError> {
        // 1. Check if builder is authorized
        if !self.authorized_builders.contains(&bundle.signer) {
            debug!(
                signer = ?bundle.signer,
                "Unauthorized block builder attempted to submit transactions"
            );
            return Err(BuilderError::UnauthorizedBuilder);
        }

        // 2. Check timestamp (not too old, not too far in future)
        let age = current_time.saturating_sub(bundle.timestamp);
        if age > self.max_bundle_age_secs {
            debug!(
                bundle_age = age,
                max_age = self.max_bundle_age_secs,
                "Block builder bundle too old"
            );
            return Err(BuilderError::BundleTooOld);
        }
        if bundle.timestamp > current_time + 60 {
            // 1 minute future tolerance
            debug!(
                bundle_timestamp = bundle.timestamp,
                current_time = current_time,
                "Block builder bundle timestamp from future"
            );
            return Err(BuilderError::BundleFromFuture);
        }

        // 3. Verify cryptographic signature
        if !bundle.verify_signature() {
            warn!(
                signer = ?bundle.signer,
                "Invalid cryptographic signature on block builder bundle"
            );
            return Err(BuilderError::InvalidSignature);
        }

        // 4. Check for replay (bundle hash already seen recently)
        let bundle_hash = bundle.compute_bundle_hash();
        if self.recent_bundles.contains_key(&bundle_hash) {
            debug!(
                bundle_hash = ?bundle_hash,
                "Replay attempt detected for block builder bundle"
            );
            return Err(BuilderError::ReplayAttempt);
        }

        // 5. Add to recent bundles cache
        self.recent_bundles.insert(bundle_hash, current_time);

        // 6. Store raw transactions (validation happens later in existing pipeline)
        let mut added = 0;
        for tx in bundle.transactions {
            if self.transactions.len() >= self.max_size {
                // Remove oldest transaction to make room
                self.transactions.pop_front();
            }

            // Store raw transaction - validation happens when creating proposal
            self.transactions.push_back(tx);
            added += 1;
        }

        debug!(
            signer = ?bundle.signer,
            added_transactions = added,
            pool_size = self.transactions.len(),
            "Successfully added block builder transaction bundle"
        );

        Ok(added)
    }

    /// Get transactions for block proposal, up to the specified limit
    pub fn get_transactions(&mut self, limit: usize) -> Vec<Recovered<TxEnvelope>> {
        let to_take = limit.min(self.transactions.len());
        self.transactions.drain(..to_take).collect()
    }

    /// Get the number of transactions currently stored
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Clear all stored transactions
    pub fn clear(&mut self) {
        self.transactions.clear();
    }

    /// Get an iterator over the stored transactions (for debugging/metrics)
    pub fn iter(&self) -> impl Iterator<Item = &Recovered<TxEnvelope>> {
        self.transactions.iter()
    }

    /// Clean up old entries from the replay protection cache
    pub fn cleanup_old_bundles(&mut self, current_time: u64) {
        // Remove bundle hashes older than 2x the max bundle age
        let cutoff = current_time.saturating_sub(self.max_bundle_age_secs * 2);
        
        // Remove old entries from the hashmap
        self.recent_bundles.retain(|_hash, timestamp| *timestamp >= cutoff);
    }

}

/// Errors that can occur when processing block builder transactions
#[derive(Debug, PartialEq, Eq)]
pub enum BuilderError {
    UnauthorizedBuilder,
    BundleTooOld,
    BundleFromFuture,
    InvalidSignature,
    ReplayAttempt,
    NotEnabled,
    TransactionValidationFailed(String),
    BundleTooLarge(usize),
}

impl fmt::Display for BuilderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuilderError::UnauthorizedBuilder => write!(f, "Block builder not authorized"),
            BuilderError::BundleTooOld => write!(f, "Bundle too old"),
            BuilderError::BundleFromFuture => write!(f, "Bundle timestamp from future"),
            BuilderError::InvalidSignature => write!(f, "Invalid cryptographic signature"),
            BuilderError::ReplayAttempt => write!(f, "Replay attempt detected"),
            BuilderError::NotEnabled => write!(f, "Block builder functionality not enabled"),
            BuilderError::TransactionValidationFailed(reason) => {
                write!(f, "Transaction validation failed: {}", reason)
            }
            BuilderError::BundleTooLarge(count) => {
                write!(f, "Bundle too large: {} transactions", count)
            }
        }
    }
}

impl std::error::Error for BuilderError {}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        NopKeyPair, NopPubKey, NopSignature,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_SECRET: B256 = B256::new(hex!(
        "1111111111111111111111111111111111111111111111111111111111111111"
    ));

    fn make_test_keypair(secret: B256) -> NopKeyPair {
        let mut secret_bytes = secret.0;
        NopKeyPair::from_bytes(&mut secret_bytes).unwrap()
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn test_bundle_signature_verification() {
        let keypair = make_test_keypair(TEST_SECRET);
        let timestamp = current_timestamp();
        
        // Create empty bundle for testing
        let transactions = Vec::new();
        let bundle_hash = {
            let mut data = Vec::new();
            data.extend_from_slice(&timestamp.to_be_bytes());
            keccak256(data)
        };
        
        let bundle = SignedBuilderTxBundle {
            transactions,
            signature: NopSignature::sign::<BlockBuilderDomain>(&bundle_hash.0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        assert!(bundle.verify_signature());
    }

    #[test]
    fn test_authorization() {
        let authorized_keypair = make_test_keypair(TEST_SECRET);
        let unauthorized_keypair = make_test_keypair(B256::new(hex!(
            "2222222222222222222222222222222222222222222222222222222222222222"
        )));
        
        let mut pool = BlockBuilderTxPool::new(
            100,
            vec![authorized_keypair.pubkey()],
            300,
        );
        
        let timestamp = current_timestamp();
        
        // Test authorized submission (will fail at transaction validation step, which is expected)
        let authorized_bundle = SignedBuilderTxBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<BlockBuilderDomain>(&keccak256(&timestamp.to_be_bytes()).0, &authorized_keypair),
            signer: authorized_keypair.pubkey(),
            timestamp,
        };
        
        // Should succeed authorization but fail at transaction validation (expected for now)
        let result = pool.add_signed_bundle(authorized_bundle, timestamp);
        assert!(result.is_ok());
        
        // Test unauthorized submission
        let unauthorized_bundle = SignedBuilderTxBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<BlockBuilderDomain>(&keccak256(&timestamp.to_be_bytes()).0, &unauthorized_keypair),
            signer: unauthorized_keypair.pubkey(),
            timestamp,
        };
        
        assert_eq!(
            pool.add_signed_bundle(unauthorized_bundle, timestamp).unwrap_err(),
            BuilderError::UnauthorizedBuilder
        );
    }

    #[test]
    fn test_replay_protection() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = BlockBuilderTxPool::new(100, vec![keypair.pubkey()], 300);
        
        let timestamp = current_timestamp();
        let bundle = SignedBuilderTxBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<BlockBuilderDomain>(&keccak256(&timestamp.to_be_bytes()).0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        // First submission should succeed
        assert!(pool.add_signed_bundle(bundle.clone(), timestamp).is_ok());
        
        // Second submission of same bundle should fail
        assert_eq!(
            pool.add_signed_bundle(bundle, timestamp).unwrap_err(),
            BuilderError::ReplayAttempt
        );
    }

    #[test]
    fn test_timestamp_validation() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = BlockBuilderTxPool::new(100, vec![keypair.pubkey()], 300);
        
        let current_time = current_timestamp();
        
        // Test bundle too old
        let old_timestamp = current_time - 400;
        let old_bundle = SignedBuilderTxBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<BlockBuilderDomain>(&keccak256(&old_timestamp.to_be_bytes()).0, &keypair),
            signer: keypair.pubkey(),
            timestamp: old_timestamp,
        };
        
        assert_eq!(
            pool.add_signed_bundle(old_bundle, current_time).unwrap_err(),
            BuilderError::BundleTooOld
        );
        
        // Test bundle from future
        let future_timestamp = current_time + 100;
        let future_bundle = SignedBuilderTxBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<BlockBuilderDomain>(&keccak256(&future_timestamp.to_be_bytes()).0, &keypair),
            signer: keypair.pubkey(),
            timestamp: future_timestamp,
        };
        
        assert_eq!(
            pool.add_signed_bundle(future_bundle, current_time).unwrap_err(),
            BuilderError::BundleFromFuture
        );
    }
}
