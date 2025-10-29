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

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::{hex, keccak256, Address, B256};
use alloy_rlp::Encodable;
use monad_chain_config::{execution_revision::ExecutionChainParams, revision::ChainParams};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_crypto::signing_domain;
use monad_eth_block_policy::{nonce_usage::{NonceUsage, NonceUsageMap}, validation::static_validate_transaction};
use monad_eth_txpool_types::TransactionError;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};


/// Signing domain for external block builder transaction bundles
pub struct ExternalBlockBuilderDomain;
impl signing_domain::SigningDomain for ExternalBlockBuilderDomain {
    const PREFIX: &'static [u8] = b"MONAD_BLOCK_BUILDER_v1";
}

/// A cryptographically signed bundle of transactions from an external block builder
#[derive(Debug, Clone)]
pub struct SignedExternalBuilderBundle<ST: CertificateSignatureRecoverable> {
    /// The transactions in this bundle
    pub transactions: Vec<Recovered<TxEnvelope>>,
    /// Signature over the bundle hash
    pub signature: ST,
    /// The signer's public key
    pub signer: CertificateSignaturePubKey<ST>,
    /// Timestamp when bundle was created (for replay protection)
    pub timestamp: u64,
}

impl<ST: CertificateSignatureRecoverable> SignedExternalBuilderBundle<ST> {
    /// Compute the hash that should be signed for this bundle
    pub fn compute_bundle_hash(&self) -> B256 {
        let mut data = Vec::new();
        
        // Hash transaction data
        for (i, tx) in self.transactions.iter().enumerate() {
            let tx_hash = tx.tx_hash();
            debug!("  Bundle hash TX[{}]: 0x{}", i, hex::encode(tx_hash.as_slice()));
            data.extend_from_slice(tx_hash.as_slice());
        }
        
        // Include timestamp for replay protection
        let timestamp_bytes = self.timestamp.to_be_bytes();
        debug!("  Bundle hash timestamp bytes: 0x{}", hex::encode(&timestamp_bytes));
        data.extend_from_slice(&timestamp_bytes);
        
        debug!("  Bundle hash input data: 0x{}", hex::encode(&data));
        let hash = keccak256(data);
        debug!("  Computed bundle hash: 0x{}", hex::encode(hash.as_slice()));
        hash
    }
    
    /// Verify the signature on this bundle
    pub fn verify_signature(&self) -> bool {
        let bundle_hash = self.compute_bundle_hash();
        self.signature
            .verify::<ExternalBlockBuilderDomain>(&bundle_hash.0, &self.signer)
            .is_ok()
    }
}

/// Wire format for submitting external builder transaction bundles via API
#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalBuilderBundleRequest {
    /// Array of transaction hex strings
    pub transactions: Vec<String>,
    /// Signature hex string  
    pub signature: String,
    /// Signer public key hex string
    pub signer: String,
    /// Unix timestamp
    pub timestamp: u64,
}

/// Metadata about transactions in the external builder pool
#[derive(Debug, Clone, Default)]
pub struct ExternalBuilderPoolMetadata {
    /// Total gas limit of all transactions
    pub total_gas: u64,
    /// Total size in bytes of all transactions
    pub total_size: u64,
    /// Map of (sender, nonce) to track duplicates
    pub nonce_map: HashMap<(Address, u64), usize>,
}

/// Storage for external block builder transactions that should be prioritized
#[derive(Debug, Clone)]
pub struct ExternalBuilderTxPool<ST: CertificateSignatureRecoverable> {
    /// Raw transactions ready for inclusion
    transactions: VecDeque<Recovered<TxEnvelope>>,
    /// Metadata about the transactions in the pool
    metadata: ExternalBuilderPoolMetadata,
    /// Authorized builder public keys
    authorized_builders: HashSet<CertificateSignaturePubKey<ST>>,
    /// Maximum bundle age for replay protection
    max_bundle_age_secs: u64,
    /// Recently seen bundle hashes (simple replay protection)
    recent_bundles: HashMap<B256, u64>, // hash -> timestamp
    /// For generic parameter tracking
    _phantom: PhantomData<ST>,
}

impl<ST: CertificateSignatureRecoverable> ExternalBuilderTxPool<ST> {
    /// Create a new external block builder transaction pool
    pub fn new(
        authorized_builders: Vec<CertificateSignaturePubKey<ST>>,
        max_bundle_age_secs: u64,
    ) -> Self {
        Self {
            transactions: VecDeque::new(),
            metadata: ExternalBuilderPoolMetadata::default(),
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

    /// Add a signed bundle of transactions with comprehensive validation
    pub fn add_signed_bundle(
        &mut self,
        bundle: SignedExternalBuilderBundle<ST>,
        current_time: u64,
        chain_id: u64,
        chain_params: &ChainParams,
        execution_params: &ExecutionChainParams,
    ) -> Result<usize, ExternalBuilderError> {
        debug!("=== External Builder Bundle Validation ===");
        debug!("  Timestamp: {}", bundle.timestamp);
        debug!("  Current time: {}", current_time);
        debug!("  Num transactions: {}", bundle.transactions.len());
        debug!("  Signer: {:?}", bundle.signer);
        
        // 1. Check if builder is authorized
        debug!("Step 1: Checking authorization...");
        debug!("  Authorized builders: {:?}", self.authorized_builders);
        if !self.authorized_builders.contains(&bundle.signer) {
            warn!(
                signer = ?bundle.signer,
                "Unauthorized block builder attempted to submit transactions"
            );
            return Err(ExternalBuilderError::UnauthorizedBuilder);
        }
        debug!("  ✓ Builder is authorized");

        // 2. Check timestamp (not too old, not too far in future)
        debug!("Step 2: Checking timestamp validity...");
        let age = current_time.saturating_sub(bundle.timestamp);
        debug!("  Bundle age: {} seconds", age);
        debug!("  Max age: {} seconds", self.max_bundle_age_secs);
        if age > self.max_bundle_age_secs {
            warn!(
                bundle_age = age,
                max_age = self.max_bundle_age_secs,
                "Block builder bundle too old"
            );
            return Err(ExternalBuilderError::BundleTooOld);
        }
        if bundle.timestamp > current_time + 60 {
            // 1 minute future tolerance
            warn!(
                bundle_timestamp = bundle.timestamp,
                current_time = current_time,
                "Block builder bundle timestamp from future"
            );
            return Err(ExternalBuilderError::BundleFromFuture);
        }
        debug!("  ✓ Timestamp is valid");

        // 3. Verify cryptographic signature
        debug!("Step 3: Verifying cryptographic signature...");
        let bundle_hash = bundle.compute_bundle_hash();
        debug!("  Bundle hash: {:?}", bundle_hash);
        if !bundle.verify_signature() {
            warn!(
                signer = ?bundle.signer,
                bundle_hash = ?bundle_hash,
                "Invalid cryptographic signature on block builder bundle"
            );
            return Err(ExternalBuilderError::InvalidSignature);
        }
        debug!("  ✓ Signature is valid");

        // 4. Check for replay (bundle hash already seen recently)
        debug!("Step 4: Checking for replay...");
        if self.recent_bundles.contains_key(&bundle_hash) {
            warn!(
                bundle_hash = ?bundle_hash,
                "Replay attempt detected for block builder bundle"
            );
            return Err(ExternalBuilderError::ReplayAttempt);
        }
        debug!("  ✓ Not a replay");

        // 5. Validate transaction contents
        debug!("Step 5: Validating transaction contents...");
        let mut nonce_tracker = NonceUsageMap::default();
        let mut valid_transactions = Vec::new();
        let mut total_gas: u64 = 0;
        let mut total_size: u64 = 0;
        
        for (i, tx) in bundle.transactions.into_iter().enumerate() {
            // 6a. Check for duplicate nonces within bundle
            if let Some(old_nonce) = nonce_tracker.add_known(tx.signer(), tx.nonce()) {
                match old_nonce {
                    NonceUsage::Known(prev_nonce) => {
                        warn!(
                            "Invalid nonce sequence in bundle from {:?}: \
                             tx[{}] has nonce {} but previous was {}",
                            bundle.signer, i, tx.nonce(), prev_nonce
                        );
                        return Err(ExternalBuilderError::InvalidNonceSequence {
                            sender: tx.signer(),
                            nonce: tx.nonce(),
                            prev_nonce,
                            index: i,
                        });
                    }
                    NonceUsage::Possible(_) => {
                        // Shouldn't happen with our simple tracking, but be safe
                        warn!(
                            "Unexpected NonceUsage::Possible in bundle validation at tx[{}]",
                            i
                        );
                        return Err(ExternalBuilderError::InvalidNonceSequence {
                            sender: tx.signer(),
                            nonce: tx.nonce(),
                            prev_nonce: 0, // Placeholder
                            index: i,
                        });
                    }
                }
            }
            
            // 6b. Full static validation (catches most invalid txs)
            if let Err(error) = static_validate_transaction(
                tx.as_ref(),
                chain_id,
                chain_params,
                execution_params,
            ) {
                warn!(
                    "Transaction {} in bundle from {:?} failed static validation: {:?}",
                    i, bundle.signer, error
                );
                return Err(ExternalBuilderError::StaticValidationFailed { index: i, error });
            }
            
            // 6c. Check bundle doesn't exceed block gas limit
            let tx_gas = tx.gas_limit();
            if let Some(new_total_gas) = total_gas.checked_add(tx_gas) {
                if new_total_gas > chain_params.proposal_gas_limit {
                    warn!(
                        "Bundle from {:?} exceeds block gas limit: {} + {} > {}",
                        bundle.signer, total_gas, tx_gas, chain_params.proposal_gas_limit
                    );
                    return Err(ExternalBuilderError::BundleExceedsGasLimit {
                        bundle_gas: new_total_gas,
                        block_limit: chain_params.proposal_gas_limit,
                    });
                }
                total_gas = new_total_gas;
            } else {
                // Overflow
                return Err(ExternalBuilderError::BundleExceedsGasLimit {
                    bundle_gas: u64::MAX,
                    block_limit: chain_params.proposal_gas_limit,
                });
            }
            
            // 6d. Check bundle doesn't exceed block size limit
            let tx_size = tx.tx().length() as u64;
            if let Some(new_total_size) = total_size.checked_add(tx_size) {
                if new_total_size > chain_params.proposal_byte_limit {
                    warn!(
                        "Bundle from {:?} exceeds block size limit: {} + {} > {}",
                        bundle.signer, total_size, tx_size, chain_params.proposal_byte_limit
                    );
                    return Err(ExternalBuilderError::BundleExceedsSizeLimit {
                        bundle_size: new_total_size,
                        block_limit: chain_params.proposal_byte_limit,
                    });
                }
                total_size = new_total_size;
            } else {
                // Overflow
                return Err(ExternalBuilderError::BundleExceedsSizeLimit {
                    bundle_size: u64::MAX,
                    block_limit: chain_params.proposal_byte_limit,
                });
            }
            
            valid_transactions.push(tx);
        }
        
        debug!(
            "  ✓ All {} transactions validated (total gas: {}, total size: {})",
            valid_transactions.len(),
            total_gas,
            total_size
        );

        // 6. Store validated transactions and update metadata
        self.recent_bundles.insert(bundle_hash, current_time);
        
        let added = valid_transactions.len();
        for tx in valid_transactions.into_iter() {
            // Update metadata as we add transactions
            let key = (tx.signer(), tx.nonce());
            self.metadata.nonce_map.insert(key, self.metadata.nonce_map.len());
            self.metadata.total_gas += tx.gas_limit();
            self.metadata.total_size += tx.tx().length() as u64;
            
            self.transactions.push_back(tx);
        }

        debug!("  ✓ Added {} transactions to builder pool", added);
        debug!("  New pool size: {}", self.transactions.len());
        debug!("  Pool metadata: gas={}, size={}, nonces={}", 
               self.metadata.total_gas, self.metadata.total_size, self.metadata.nonce_map.len());
        debug!("=== Builder Bundle Validation Complete ===");
        
        debug!(
            signer = ?bundle.signer,
            added_transactions = added,
            pool_size = self.transactions.len(),
            "Successfully added block builder transaction bundle"
        );

        Ok(added)
    }

    /// Get transactions for block proposal, up to the specified limit
    /// Returns (transactions, metadata for those transactions)
    pub fn get_transactions(&mut self, limit: usize) -> (Vec<Recovered<TxEnvelope>>, ExternalBuilderPoolMetadata) {
        let to_take = limit.min(self.transactions.len());
        let taken: Vec<_> = self.transactions.drain(..to_take).collect();
        
        // Calculate metadata for the taken transactions
        let mut taken_metadata = ExternalBuilderPoolMetadata::default();
        for tx in &taken {
            let key = (tx.signer(), tx.nonce());
            taken_metadata.nonce_map.insert(key, taken_metadata.nonce_map.len());
            taken_metadata.total_gas += tx.gas_limit();
            taken_metadata.total_size += tx.tx().length() as u64;
        }
        
        // Recalculate metadata for remaining transactions
        self.metadata = ExternalBuilderPoolMetadata::default();
        for tx in &self.transactions {
            let key = (tx.signer(), tx.nonce());
            self.metadata.nonce_map.insert(key, self.metadata.nonce_map.len());
            self.metadata.total_gas += tx.gas_limit();
            self.metadata.total_size += tx.tx().length() as u64;
        }
        
        (taken, taken_metadata)
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
        self.metadata = ExternalBuilderPoolMetadata::default();
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
pub enum ExternalBuilderError {
    UnauthorizedBuilder,
    BundleTooOld,
    BundleFromFuture,
    InvalidSignature,
    ReplayAttempt,
    NotEnabled,
    TransactionValidationFailed(String),
    BundleTooLarge(usize),
    InvalidNonceSequence {
        sender: Address,
        nonce: u64,
        prev_nonce: u64,
        index: usize,
    },
    StaticValidationFailed {
        index: usize,
        error: TransactionError,
    },
    BundleExceedsGasLimit {
        bundle_gas: u64,
        block_limit: u64,
    },
    BundleExceedsSizeLimit {
        bundle_size: u64,
        block_limit: u64,
    },
}

impl fmt::Display for ExternalBuilderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExternalBuilderError::UnauthorizedBuilder => write!(f, "Block builder not authorized"),
            ExternalBuilderError::BundleTooOld => write!(f, "Bundle too old"),
            ExternalBuilderError::BundleFromFuture => write!(f, "Bundle timestamp from future"),
            ExternalBuilderError::InvalidSignature => write!(f, "Invalid cryptographic signature"),
            ExternalBuilderError::ReplayAttempt => write!(f, "Replay attempt detected"),
            ExternalBuilderError::NotEnabled => write!(f, "Block builder functionality not enabled"),
            ExternalBuilderError::TransactionValidationFailed(reason) => {
                write!(f, "Transaction validation failed: {}", reason)
            }
            ExternalBuilderError::BundleTooLarge(count) => {
                write!(f, "Bundle too large: {} transactions", count)
            }
            ExternalBuilderError::InvalidNonceSequence {
                sender,
                nonce,
                prev_nonce,
                index,
            } => write!(
                f,
                "Invalid nonce sequence at transaction {}: sender={:?}, nonce={}, previous={}",
                index, sender, nonce, prev_nonce
            ),
            ExternalBuilderError::StaticValidationFailed { index, error } => {
                write!(f, "Transaction {} failed static validation: {:?}", index, error)
            }
            ExternalBuilderError::BundleExceedsGasLimit {
                bundle_gas,
                block_limit,
            } => write!(
                f,
                "Bundle exceeds block gas limit: {} > {}",
                bundle_gas, block_limit
            ),
            ExternalBuilderError::BundleExceedsSizeLimit {
                bundle_size,
                block_limit,
            } => write!(
                f,
                "Bundle exceeds block size limit: {} > {}",
                bundle_size, block_limit
            ),
        }
    }
}

impl std::error::Error for ExternalBuilderError {}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_primitives::{hex, TxKind};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::{ChainConfig, MockChainConfig, revision::{ChainRevision, MockChainRevision}};
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        NopKeyPair, NopSignature,
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
    
    fn make_test_transaction(nonce: u64, chain_id: u64, signer: &PrivateKeySigner) -> Recovered<TxEnvelope> {
        let address = Address::from(hex!("0000000000000000000000000000000000000001"));
        
        let tx = TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            to: TxKind::Call(address),
            gas_price: 1000,
            gas_limit: 21000,
            value: alloy_primitives::U256::from(1000),
            input: Default::default(),
        };
        
        let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let signed_tx = tx.into_signed(signature);
        
        let sender = signer.address();
        Recovered::new_unchecked(signed_tx.into(), sender)
    }
    
    fn get_test_chain_params() -> (&'static ChainParams, &'static ExecutionChainParams) {
        (
            MockChainRevision::DEFAULT.chain_params(),
            monad_chain_config::execution_revision::MonadExecutionRevision::LATEST
                .execution_chain_params(),
        )
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
        
        let bundle = SignedExternalBuilderBundle {
            transactions,
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&bundle_hash.0, &keypair),
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
        
        let mut pool = ExternalBuilderTxPool::new(
            vec![authorized_keypair.pubkey()],
            300,
        );
        
        let timestamp = current_timestamp();
        let (chain_params, execution_params) = get_test_chain_params();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        
        // Test authorized submission (empty bundle, should succeed)
        let authorized_bundle = SignedExternalBuilderBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&timestamp.to_be_bytes()).0, &authorized_keypair),
            signer: authorized_keypair.pubkey(),
            timestamp,
        };
        
        let result = pool.add_signed_bundle(
            authorized_bundle,
            timestamp,
            chain_id,
            chain_params,
            execution_params,
        );
        assert!(result.is_ok());
        
        // Test unauthorized submission
        let unauthorized_bundle = SignedExternalBuilderBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&timestamp.to_be_bytes()).0, &unauthorized_keypair),
            signer: unauthorized_keypair.pubkey(),
            timestamp,
        };
        
        assert_eq!(
            pool.add_signed_bundle(
                unauthorized_bundle,
                timestamp,
                chain_id,
                chain_params,
                execution_params,
            )
            .unwrap_err(),
            ExternalBuilderError::UnauthorizedBuilder
        );
    }

    #[test]
    fn test_replay_protection() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = ExternalBuilderTxPool::new(vec![keypair.pubkey()], 300);
        
        let timestamp = current_timestamp();
        let (chain_params, execution_params) = get_test_chain_params();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        
        let bundle = SignedExternalBuilderBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&timestamp.to_be_bytes()).0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        // First submission should succeed
        assert!(pool
            .add_signed_bundle(bundle.clone(), timestamp, chain_id, chain_params, execution_params)
            .is_ok());
        
        // Second submission of same bundle should fail
        assert_eq!(
            pool.add_signed_bundle(bundle, timestamp, chain_id, chain_params, execution_params)
                .unwrap_err(),
            ExternalBuilderError::ReplayAttempt
        );
    }

    #[test]
    fn test_timestamp_validation() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = ExternalBuilderTxPool::new(vec![keypair.pubkey()], 300);
        
        let current_time = current_timestamp();
        let (chain_params, execution_params) = get_test_chain_params();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        
        // Test bundle too old
        let old_timestamp = current_time - 400;
        let old_bundle = SignedExternalBuilderBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&old_timestamp.to_be_bytes()).0, &keypair),
            signer: keypair.pubkey(),
            timestamp: old_timestamp,
        };
        
        assert_eq!(
            pool.add_signed_bundle(old_bundle, current_time, chain_id, chain_params, execution_params)
                .unwrap_err(),
            ExternalBuilderError::BundleTooOld
        );
        
        // Test bundle from future
        let future_timestamp = current_time + 100;
        let future_bundle = SignedExternalBuilderBundle {
            transactions: Vec::new(),
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&future_timestamp.to_be_bytes()).0, &keypair),
            signer: keypair.pubkey(),
            timestamp: future_timestamp,
        };
        
        assert_eq!(
            pool.add_signed_bundle(future_bundle, current_time, chain_id, chain_params, execution_params)
                .unwrap_err(),
            ExternalBuilderError::BundleFromFuture
        );
    }
    
    #[test]
    fn test_duplicate_nonces_in_bundle() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = ExternalBuilderTxPool::new(vec![keypair.pubkey()], 300);
        
        let timestamp = current_timestamp();
        let (chain_params, execution_params) = get_test_chain_params();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        
        // Create a signer for transactions
        let tx_signer_secret = B256::new(hex!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        ));
        let tx_signer = PrivateKeySigner::from_bytes(&tx_signer_secret).unwrap();
        
        // Create two transactions with the same nonce from the same sender
        let tx1 = make_test_transaction(0, chain_id, &tx_signer);
        let tx2 = make_test_transaction(0, chain_id, &tx_signer); // Duplicate nonce!
        
        let bundle = SignedExternalBuilderBundle {
            transactions: vec![tx1, tx2],
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&[]).0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        // Compute correct bundle hash for signature
        let correct_bundle_hash = bundle.compute_bundle_hash();
        let bundle_with_correct_sig = SignedExternalBuilderBundle {
            transactions: bundle.transactions,
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&correct_bundle_hash.0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        let result = pool.add_signed_bundle(
            bundle_with_correct_sig,
            timestamp,
            chain_id,
            chain_params,
            execution_params,
        );
        
        // Should fail due to duplicate nonce
        assert!(matches!(result, Err(ExternalBuilderError::InvalidNonceSequence { .. })));
    }
    
    #[test]
    fn test_invalid_chain_id_in_transaction() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = ExternalBuilderTxPool::new(vec![keypair.pubkey()], 300);
        
        let timestamp = current_timestamp();
        let (chain_params, execution_params) = get_test_chain_params();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        
        // Create transaction with wrong chain ID
        let tx_signer_secret = B256::new(hex!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        ));
        let tx_signer = PrivateKeySigner::from_bytes(&tx_signer_secret).unwrap();
        
        let wrong_chain_id = chain_id + 1;
        let tx_with_wrong_chain = make_test_transaction(0, wrong_chain_id, &tx_signer);
        
        let bundle = SignedExternalBuilderBundle {
            transactions: vec![tx_with_wrong_chain],
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&[]).0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        let correct_bundle_hash = bundle.compute_bundle_hash();
        let bundle_with_correct_sig = SignedExternalBuilderBundle {
            transactions: bundle.transactions,
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&correct_bundle_hash.0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        let result = pool.add_signed_bundle(
            bundle_with_correct_sig,
            timestamp,
            chain_id,
            chain_params,
            execution_params,
        );
        
        // Should fail due to invalid chain ID
        assert!(matches!(
            result,
            Err(ExternalBuilderError::StaticValidationFailed {
                error: TransactionError::InvalidChainId,
                ..
            })
        ));
    }
    
    #[test]
    fn test_valid_bundle_with_multiple_senders() {
        let keypair = make_test_keypair(TEST_SECRET);
        let mut pool = ExternalBuilderTxPool::new(vec![keypair.pubkey()], 300);
        
        let timestamp = current_timestamp();
        let (chain_params, execution_params) = get_test_chain_params();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        
        // Create transactions from different senders with same nonce (this is OK!)
        let signer1_secret = B256::new(hex!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        ));
        let signer1 = PrivateKeySigner::from_bytes(&signer1_secret).unwrap();
        
        let signer2_secret = B256::new(hex!(
            "2222222222222222222222222222222222222222222222222222222222222222"
        ));
        let signer2 = PrivateKeySigner::from_bytes(&signer2_secret).unwrap();
        
        let tx1 = make_test_transaction(0, chain_id, &signer1);
        let tx2 = make_test_transaction(0, chain_id, &signer2); // Same nonce, different sender - OK!
        
        let bundle = SignedExternalBuilderBundle {
            transactions: vec![tx1, tx2],
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&[]).0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        let correct_bundle_hash = bundle.compute_bundle_hash();
        let bundle_with_correct_sig = SignedExternalBuilderBundle {
            transactions: bundle.transactions,
            signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&correct_bundle_hash.0, &keypair),
            signer: keypair.pubkey(),
            timestamp,
        };
        
        let result = pool.add_signed_bundle(
            bundle_with_correct_sig,
            timestamp,
            chain_id,
            chain_params,
            execution_params,
        );
        
        // Should succeed
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
    }
}
