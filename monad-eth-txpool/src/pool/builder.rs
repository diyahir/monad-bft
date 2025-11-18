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
    collections::{HashMap, VecDeque},
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
use monad_types::Balance;
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

/// A bundle that's currently active and can be used for block building
#[derive(Debug, Clone)]
struct ActiveBundle<ST: CertificateSignatureRecoverable> {
    /// Hash of this bundle (for replay detection)
    bundle_hash: B256,
    /// Builder who submitted this
    builder: CertificateSignaturePubKey<ST>,
    /// Transactions in this bundle
    transactions: VecDeque<Recovered<TxEnvelope>>,
    /// Metadata for this bundle
    metadata: ExternalBuilderPoolMetadata,
    /// When this bundle was received (for future metrics/debugging)
    #[allow(dead_code)]
    received_at: u64,
}

/// Storage for external block builder transactions that should be prioritized
/// Only one bundle can be active at a time. New bundles replace old ones.
#[derive(Debug, Clone)]
pub struct ExternalBuilderTxPool<ST: CertificateSignatureRecoverable> {
    /// Current active bundle (can be replaced until used in a block)
    current_bundle: Option<ActiveBundle<ST>>,
    /// Authorized builder public key 
    authorized_builder: Option<CertificateSignaturePubKey<ST>>,
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
        authorized_builder: Option<CertificateSignaturePubKey<ST>>,
        max_bundle_age_secs: u64,
    ) -> Self {
        Self {
            current_bundle: None,
            authorized_builder,
            max_bundle_age_secs,
            recent_bundles: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Update the authorized builder
    pub fn update_authorized_builder(
        &mut self,
        authorized_builder: Option<CertificateSignaturePubKey<ST>>,
    ) {
        self.authorized_builder = authorized_builder;
        
        // Clear current bundle if it's from a builder that's no longer authorized
        if let Some(ref bundle) = self.current_bundle {
            if self.authorized_builder.as_ref() != Some(&bundle.builder) {
                debug!("Clearing bundle from de-authorized builder: {:?}", bundle.builder);
                self.current_bundle = None;
            }
        }
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
        debug!("  Authorized builder: {:?}", self.authorized_builder);
        if self.authorized_builder.as_ref() != Some(&bundle.signer) {
            warn!(
                signer = ?bundle.signer,
                authorized = ?self.authorized_builder,
                "Unauthorized block builder attempted to submit transactions"
            );
            return Err(ExternalBuilderError::UnauthorizedBuilder);
        }
        debug!("  ✓ Builder is authorized");

        // 2. Reject empty bundles (no legitimate use case, potential spam/replay vector)
        debug!("Step 2: Checking bundle is not empty...");
        if bundle.transactions.is_empty() {
            warn!(
                signer = ?bundle.signer,
                "Block builder submitted empty bundle"
            );
            return Err(ExternalBuilderError::BundleEmpty);
        }
        debug!("  ✓ Bundle contains {} transactions", bundle.transactions.len());

        // 3. Check timestamp (not too old, not too far in future)
        debug!("Step 3: Checking timestamp validity...");
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

        // 4. Verify cryptographic signature
        debug!("Step 4: Verifying cryptographic signature...");
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

        // 5. Check for replay (bundle hash already seen recently)
        debug!("Step 5: Checking for replay...");
        if self.recent_bundles.contains_key(&bundle_hash) {
            warn!(
                bundle_hash = ?bundle_hash,
                "Replay attempt detected for block builder bundle"
            );
            return Err(ExternalBuilderError::ReplayAttempt);
        }
        debug!("  ✓ Not a replay");

        // 6. Validate transaction contents
        debug!("Step 6: Validating transaction contents...");
        let mut nonce_tracker = NonceUsageMap::default();
        let mut valid_transactions = Vec::new();
        let mut total_gas: u64 = 0;
        let mut total_size: u64 = 0;
        
        for (i, tx) in bundle.transactions.into_iter().enumerate() {
            // 6a. Check for duplicate/invalid nonces within bundle
            // Allow sequential nonces (prev_nonce + 1) from the same sender
            if let Some(old_nonce) = nonce_tracker.add_known(tx.signer(), tx.nonce()) {
                match old_nonce {
                    NonceUsage::Known(prev_nonce) => {
                        // Allow sequential nonces (prev_nonce + 1)
                        // Reject duplicate nonces, backwards nonces, or non-sequential gaps
                        if tx.nonce() != prev_nonce + 1 {
                            warn!(
                                "Invalid nonce sequence in bundle from {:?}: \
                                 tx[{}] has nonce {} but previous was {} (expected {} for sequential nonces)",
                                bundle.signer, i, tx.nonce(), prev_nonce, prev_nonce + 1
                            );
                            return Err(ExternalBuilderError::InvalidNonceSequence {
                                sender: tx.signer(),
                                nonce: tx.nonce(),
                                prev_nonce,
                                index: i,
                            });
                        }
                        // If nonce == prev_nonce + 1, this is valid sequential nonce from same sender
                        debug!(
                            "  tx[{}] from {:?}: sequential nonce {} (previous: {})",
                            i, tx.signer(), tx.nonce(), prev_nonce
                        );
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
            } else {
                // First transaction from this sender in the bundle
                debug!(
                    "  tx[{}] from {:?}: first nonce {} for this sender",
                    i, tx.signer(), tx.nonce()
                );
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

        // 6. Store validated transactions - REPLACE any existing bundle
        self.recent_bundles.insert(bundle_hash, current_time);
        
        // Check if we're replacing an existing bundle
        if let Some(old_bundle) = &self.current_bundle {
            debug!(
                "Replacing existing bundle from builder {:?} (hash: {:?}) with new bundle from builder {:?} (hash: {:?})",
                old_bundle.builder, old_bundle.bundle_hash, bundle.signer, bundle_hash
            );
        }
        
        // Create metadata for this bundle
        let mut metadata = ExternalBuilderPoolMetadata::default();
        let mut transactions = VecDeque::new();
        
        for tx in valid_transactions.into_iter() {
            let key = (tx.signer(), tx.nonce());
            metadata.nonce_map.insert(key, metadata.nonce_map.len());
            metadata.total_gas += tx.gas_limit();
            metadata.total_size += tx.tx().length() as u64;
            transactions.push_back(tx);
        }
        
        let added = transactions.len();
        
        // Replace current bundle with this new one
        self.current_bundle = Some(ActiveBundle {
            bundle_hash,
            builder: bundle.signer.clone(),
            transactions,
            metadata,
            received_at: current_time,
        });

        debug!("  ✓ Set new active bundle with {} transactions", added);
        debug!("=== Builder Bundle Validation Complete ===");
        
        debug!(
            signer = ?bundle.signer,
            transactions = added,
            "Successfully set active block builder bundle"
        );

        Ok(added)
    }

    /// Get transactions for block proposal, up to the specified limit
    /// This consumes the current bundle. Subsequent calls return empty until a new bundle arrives.
    /// Returns (transactions, metadata for those transactions)
    pub fn get_transactions(&mut self, limit: usize) -> (Vec<Recovered<TxEnvelope>>, ExternalBuilderPoolMetadata) {
        if let Some(mut bundle) = self.current_bundle.take() {
            let to_take = limit.min(bundle.transactions.len());
            let taken: Vec<_> = bundle.transactions.drain(..to_take).collect();
            
            // Calculate metadata for the taken transactions
            let mut taken_metadata = ExternalBuilderPoolMetadata::default();
            for tx in &taken {
                let key = (tx.signer(), tx.nonce());
                taken_metadata.nonce_map.insert(key, taken_metadata.nonce_map.len());
                taken_metadata.total_gas += tx.gas_limit();
                taken_metadata.total_size += tx.tx().length() as u64;
            }
            
            // If there are remaining transactions, put the bundle back (partial consumption)
            if !bundle.transactions.is_empty() {
                // Recalculate metadata for remaining transactions
                bundle.metadata = ExternalBuilderPoolMetadata::default();
                for tx in &bundle.transactions {
                    let key = (tx.signer(), tx.nonce());
                    bundle.metadata.nonce_map.insert(key, bundle.metadata.nonce_map.len());
                    bundle.metadata.total_gas += tx.gas_limit();
                    bundle.metadata.total_size += tx.tx().length() as u64;
                }
                self.current_bundle = Some(bundle);
            }
            
            (taken, taken_metadata)
        } else {
            // No current bundle
            (Vec::new(), ExternalBuilderPoolMetadata::default())
        }
    }

    /// Get the number of transactions currently stored
    pub fn len(&self) -> usize {
        self.current_bundle.as_ref().map_or(0, |b| b.transactions.len())
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.current_bundle.is_none()
    }

    /// Clear the current bundle
    pub fn clear(&mut self) {
        self.current_bundle = None;
    }

    /// Get an iterator over the stored transactions (for debugging/metrics)
    pub fn iter(&self) -> impl Iterator<Item = &Recovered<TxEnvelope>> {
        self.current_bundle
            .as_ref()
            .map(|b| b.transactions.iter())
            .into_iter()
            .flatten()
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
    PoolNotReady,
    BundleEmpty,
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
    InsufficientBalance {
        sender: Address,
        required: Balance,
        available: Balance,
        index: usize,
    },
    StateBackendError,
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
            ExternalBuilderError::PoolNotReady => write!(f, "Transaction pool not ready"),
            ExternalBuilderError::BundleEmpty => write!(f, "Bundle contains no transactions"),
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
            ExternalBuilderError::InsufficientBalance {
                sender,
                required,
                available,
                index,
            } => write!(
                f,
                "Insufficient balance at transaction {}: sender={:?}, required={}, available={}",
                index, sender, required, available
            ),
            ExternalBuilderError::StateBackendError => write!(f, "State backend error during validation"),
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
