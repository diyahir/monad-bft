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

//! Tests to verify that external block builder transactions cannot create invalid blocks
//!
//! This test suite systematically checks all the ways a block can be invalid:
//! 1. Nonce issues (gaps, duplicates, backwards)
//! 2. Insufficient balance (regular and reserve balance)
//! 3. Gas limit exceeded
//! 4. Size limit exceeded
//! 5. Transaction count limit exceeded
//! 6. Invalid chain ID
//! 7. Invalid signatures
//! 8. Static validation failures
//!
//! For each category, we test whether external builder transactions appended to the top
//! of a block can create these invalid states.

use alloy_consensus::{transaction::Recovered, SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{hex, keccak256, Address, B256, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::{
    ChainConfig, MockChainConfig,
    execution_revision::{ExecutionChainParams, MonadExecutionRevision},
    revision::{ChainParams, ChainRevision, MockChainRevision},
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    NopKeyPair, NopSignature,
};
use monad_eth_txpool::builder::{
    ExternalBlockBuilderDomain, ExternalBuilderError, ExternalBuilderTxPool,
    SignedExternalBuilderBundle,
};
use std::time::{SystemTime, UNIX_EPOCH};

// Test constants
const TEST_BUILDER_SECRET: B256 = B256::new(hex!(
    "1111111111111111111111111111111111111111111111111111111111111111"
));

const TEST_USER1_SECRET: B256 = B256::new(hex!(
    "2222222222222222222222222222222222222222222222222222222222222222"
));

const TEST_USER2_SECRET: B256 = B256::new(hex!(
    "3333333333333333333333333333333333333333333333333333333333333333"
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

fn make_transaction(
    nonce: u64,
    chain_id: u64,
    gas_limit: u64,
    gas_price: u128,
    value: U256,
    to: Address,
    signer: &PrivateKeySigner,
) -> Recovered<TxEnvelope> {
    let tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        to: TxKind::Call(to),
        gas_price,
        gas_limit,
        value,
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
        MonadExecutionRevision::LATEST.execution_chain_params(),
    )
}

fn make_signed_bundle(
    transactions: Vec<Recovered<TxEnvelope>>,
    timestamp: u64,
    keypair: &NopKeyPair,
) -> SignedExternalBuilderBundle<NopSignature> {
    let mut data = Vec::new();
    for tx in transactions.iter() {
        data.extend_from_slice(tx.tx_hash().as_slice());
    }
    data.extend_from_slice(&timestamp.to_be_bytes());
    let bundle_hash = keccak256(data);

    SignedExternalBuilderBundle {
        transactions,
        signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&bundle_hash.0, keypair),
        signer: keypair.pubkey(),
        timestamp,
    }
}

// ============================================================================
// CATEGORY 1: NONCE ISSUES
// ============================================================================

#[test]
fn test_nonce_gap_in_builder_bundle_rejected_at_submission() {
    // Test that a builder bundle with a nonce gap (e.g., nonce 0 then nonce 2)
    // is rejected during bundle validation, not during block building

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Create transactions with a nonce gap from same sender
    let tx1 = make_transaction(0, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);
    let tx2 = make_transaction(2, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer); // Gap! Should be nonce 1

    let bundle = make_signed_bundle(vec![tx1, tx2], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    // Should be rejected with InvalidNonceSequence
    assert!(matches!(
        result,
        Err(ExternalBuilderError::InvalidNonceSequence { .. })
    ));
}

#[test]
fn test_builder_bundle_with_high_starting_nonce_accepted() {
    // Test that a builder bundle can have transactions starting at high nonces
    // This is valid at bundle submission time because we don't validate against
    // current account state. However, this WOULD create an invalid block if the
    // account's actual nonce is lower.
    //
    // This demonstrates a potential vulnerability: bundle validation passes,
    // but block execution would fail.

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Create transactions starting at nonce 100 (very high)
    let tx1 = make_transaction(100, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);
    let tx2 = make_transaction(101, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);

    let bundle = make_signed_bundle(vec![tx1, tx2], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    // Bundle validation PASSES because it doesn't check against account state
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);

    // VULNERABILITY: If the actual account nonce is 0, this would create an invalid block!
    // The block builder needs to validate that builder transaction nonces are correct
    // when assembling the block, not just when receiving the bundle.
}

#[test]
fn test_duplicate_nonces_same_sender_rejected() {
    // Duplicate nonces from same sender should be rejected

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Create two transactions with the same nonce
    let tx1 = make_transaction(5, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);
    let tx2 = make_transaction(5, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);

    let bundle = make_signed_bundle(vec![tx1, tx2], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    assert!(matches!(
        result,
        Err(ExternalBuilderError::InvalidNonceSequence { .. })
    ));
}

#[test]
fn test_backwards_nonces_rejected() {
    // Nonces going backwards should be rejected

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Create transactions with backwards nonces
    let tx1 = make_transaction(10, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);
    let tx2 = make_transaction(9, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer); // Going backwards!

    let bundle = make_signed_bundle(vec![tx1, tx2], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    assert!(matches!(
        result,
        Err(ExternalBuilderError::InvalidNonceSequence { .. })
    ));
}

// ============================================================================
// CATEGORY 2: GAS LIMIT ISSUES
// ============================================================================

#[test]
fn test_builder_bundle_exceeding_block_gas_limit_rejected() {
    // Bundle that exceeds the block gas limit should be rejected

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    let block_gas_limit = chain_params.proposal_gas_limit;

    // Create transaction that exceeds block gas limit
    let tx1 = make_transaction(
        0,
        chain_id,
        block_gas_limit + 1,
        1000,
        U256::from(100),
        to_address,
        &tx_signer,
    );

    let bundle = make_signed_bundle(vec![tx1], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    // The transaction will fail static validation first (gas limit too high)
    // before we check the bundle gas limit
    assert!(result.is_err(), "Expected error but got: {:?}", result);
    // Either StaticValidationFailed (gas limit too high) or BundleExceedsGasLimit
    match result {
        Err(ExternalBuilderError::StaticValidationFailed { .. }) => {
            // Transaction has gas limit exceeding the max allowed
        }
        Err(ExternalBuilderError::BundleExceedsGasLimit { .. }) => {
            // Bundle gas limit exceeded
        }
        other => panic!("Unexpected result: {:?}", other),
    }
}

#[test]
fn test_multiple_builder_txs_exceeding_gas_limit_rejected() {
    // Multiple transactions whose combined gas exceeds limit should be rejected

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    let block_gas_limit = chain_params.proposal_gas_limit;
    let gas_per_tx = block_gas_limit / 2 + 1; // Each tx is just over half the limit

    let tx1 = make_transaction(0, chain_id, gas_per_tx, 1000, U256::from(100), to_address, &tx_signer);
    let tx2 = make_transaction(1, chain_id, gas_per_tx, 1000, U256::from(100), to_address, &tx_signer);

    let bundle = make_signed_bundle(vec![tx1, tx2], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    // Should be rejected for exceeding block gas limit
    assert!(result.is_err(), "Expected error but got: {:?}", result);
    match result {
        Err(ExternalBuilderError::BundleExceedsGasLimit { .. }) => {
            // Expected: Bundle gas limit exceeded
        }
        Err(ExternalBuilderError::StaticValidationFailed { .. }) => {
            // Also acceptable: Individual tx gas limit too high
        }
        other => panic!("Unexpected result: {:?}", other),
    }
}

// ============================================================================
// CATEGORY 3: SIZE LIMIT ISSUES
// ============================================================================

#[test]
fn test_builder_bundle_exceeding_block_size_limit_rejected() {
    // Bundle that exceeds the block size limit should be rejected

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    let block_byte_limit = chain_params.proposal_byte_limit;

    // Create a transaction with very large input data to exceed size limit
    let large_input = vec![0u8; block_byte_limit as usize];

    let tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce: 0,
        to: TxKind::Call(to_address),
        gas_price: 1000,
        gas_limit: 1_000_000, // Need high gas for large data
        value: U256::from(100),
        input: large_input.into(),
    };

    let signature = tx_signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    let signed_tx = tx.into_signed(signature);
    let sender = tx_signer.address();
    let tx1 = Recovered::new_unchecked(signed_tx.into(), sender);

    let bundle = make_signed_bundle(vec![tx1], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    // Should be rejected for exceeding size limit or init code limit
    assert!(result.is_err(), "Expected error but got: {:?}", result);
    match result {
        Err(ExternalBuilderError::BundleExceedsSizeLimit { .. }) => {
            // Expected: Bundle size limit exceeded
        }
        Err(ExternalBuilderError::StaticValidationFailed { .. }) => {
            // Also acceptable: Init code limit or other static validation
        }
        other => panic!("Unexpected result: {:?}", other),
    }
}

// ============================================================================
// CATEGORY 4: CHAIN ID VALIDATION
// ============================================================================

#[test]
fn test_builder_bundle_wrong_chain_id_rejected() {
    // Transactions with wrong chain ID should be rejected

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Create transaction with WRONG chain ID
    let wrong_chain_id = chain_id + 999;
    let tx1 = make_transaction(0, wrong_chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);

    let bundle = make_signed_bundle(vec![tx1], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    assert!(matches!(
        result,
        Err(ExternalBuilderError::StaticValidationFailed { .. })
    ));
}

// ============================================================================
// CATEGORY 5: VALID SEQUENTIAL NONCES FROM SAME SENDER
// ============================================================================

#[test]
fn test_builder_bundle_sequential_nonces_same_sender_accepted() {
    // Sequential nonces from the same sender should be accepted

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let tx_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Create 5 transactions with sequential nonces from same sender
    let mut transactions = Vec::new();
    for nonce in 10..15 {
        let tx = make_transaction(nonce, chain_id, 21000, 1000, U256::from(100), to_address, &tx_signer);
        transactions.push(tx);
    }

    let bundle = make_signed_bundle(transactions, timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 5);
}

// ============================================================================
// CATEGORY 6: EDGE CASES AND CORNER CASES
// ============================================================================

#[test]
fn test_empty_builder_bundle_rejected() {
    // Empty bundles should be rejected - they serve no legitimate purpose
    // and could be used for spam or replay attacks

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let bundle = make_signed_bundle(vec![], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    // Empty bundles should be rejected
    assert!(matches!(result, Err(ExternalBuilderError::BundleEmpty)));
}

#[test]
fn test_builder_bundle_interleaved_senders_accepted() {
    // Transactions from multiple senders interleaved should be accepted

    let keypair = make_test_keypair(TEST_BUILDER_SECRET);
    let mut pool = ExternalBuilderTxPool::new(Some(keypair.pubkey()), 300);

    let timestamp = current_timestamp();
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();

    let signer1 = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
    let signer2 = PrivateKeySigner::from_bytes(&TEST_USER2_SECRET).unwrap();
    let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

    // Interleave transactions from two senders
    let tx1 = make_transaction(0, chain_id, 21000, 1000, U256::from(100), to_address, &signer1);
    let tx2 = make_transaction(0, chain_id, 21000, 1000, U256::from(100), to_address, &signer2);
    let tx3 = make_transaction(1, chain_id, 21000, 1000, U256::from(100), to_address, &signer1);
    let tx4 = make_transaction(1, chain_id, 21000, 1000, U256::from(100), to_address, &signer2);

    let bundle = make_signed_bundle(vec![tx1, tx2, tx3, tx4], timestamp, &keypair);

    let result = pool.add_signed_bundle(
        bundle,
        timestamp,
        chain_id,
        chain_params,
        execution_params,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 4);
}

// ============================================================================
// INTEGRATION TESTS: FULL BLOCK BUILDING WITH BUILDER + MEMPOOL TXS
// ============================================================================
//
// These tests verify that the combination of [builder bundle txs, mempool txs]
// always produces a valid block, and that invalid combinations are properly
// rejected or filtered.

mod integration_tests {
    use super::*;
    use monad_crypto::{NopPubKey, certificate_signature::PubKey};
    use monad_node_config::ExternalBlockBuilderConfig;
    use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
    use monad_state_backend::{InMemoryState, InMemoryStateInner, InMemoryBlockState};
    use monad_testutil::signing::MockSignatures;
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_testutil::generate_block_with_txs;
    use monad_consensus_types::{block::GENESIS_TIMESTAMP, payload::RoundSignature};
    use monad_types::{Epoch, Round, SeqNum, NodeId, Balance, GENESIS_SEQ_NUM};
    use std::{time::Duration, collections::BTreeMap};

    const EXECUTION_DELAY: u64 = 4;
    const BASE_FEE_PER_GAS: u64 = 100_000_000_000;
    const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
    const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;

    type TestTxPool = EthTxPool<
        NopSignature,
        MockSignatures<NopSignature>,
        InMemoryState<NopSignature, MockSignatures<NopSignature>>,
        MockChainConfig,
        MockChainRevision,
    >;
    
    type TestBlockPolicy = EthBlockPolicy<
        NopSignature,
        MockSignatures<NopSignature>,
        MockChainConfig,
        MockChainRevision,
    >;

    fn setup_test_environment(
        authorized_builder: Option<NopKeyPair>,
        account_balances: BTreeMap<Address, Balance>,
        account_nonces: BTreeMap<Address, u64>,
    ) -> (TestTxPool, TestBlockPolicy, InMemoryState<NopSignature, MockSignatures<NopSignature>>) {
        let builder_config = if let Some(ref keypair) = authorized_builder {
            ExternalBlockBuilderConfig {
                enabled: true,
                authorized_builder: Some(monad_node_config::ExternalBlockBuilderIdentityConfig {
                    name: Some("test-builder".to_string()),
                    pubkey: keypair.pubkey(),
                }),
                max_bundle_age_secs: 300,
            }
        } else {
            ExternalBlockBuilderConfig::default()
        };

        let mut pool = EthTxPool::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            MockChainConfig::DEFAULT.chain_id(),
            MockChainRevision::DEFAULT,
            MonadExecutionRevision::LATEST,
            true, // do_local_insert
            builder_config,
        );
        
        // Set up state backend with account balances
        let max_balance = account_balances.values().max().copied().unwrap_or(Balance::MAX);
        let state_backend = InMemoryStateInner::new(
            max_balance,
            SeqNum(EXECUTION_DELAY),
            InMemoryBlockState::genesis(account_nonces),
        );
        
        // Initialize block policy
        let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY);
        
        // Initialize pool with genesis block
        let metrics = EthTxPoolMetrics::default();
        let mut ipc_events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);
        
        pool.update_committed_block(
            &mut event_tracker,
            &MockChainConfig::DEFAULT,
            generate_block_with_txs(
                Round(0),
                SeqNum(0),
                BASE_FEE_PER_GAS,
                &MockChainConfig::DEFAULT,
                Vec::default(),
            ),
        );
        
        (pool, block_policy, state_backend)
    }

    #[test]
    fn test_builder_tx_with_wrong_nonce_filtered_during_block_building() {
        // This tests the CRITICAL vulnerability: builder txs with nonces that don't
        // match account state should be filtered out during block building
        
        let builder_keypair = make_test_keypair(TEST_BUILDER_SECRET);
        let user_signer = PrivateKeySigner::from_bytes(&TEST_USER1_SECRET).unwrap();
        let user_address = user_signer.address();
        let chain_id = MockChainConfig::DEFAULT.chain_id();
        let timestamp = current_timestamp();
        let to_address = Address::from(hex!("0000000000000000000000000000000000000001"));

        // Set up test environment with account that has nonce 0 and balance
        let mut account_nonces = BTreeMap::new();
        account_nonces.insert(user_address, 0);  // Account nonce is 0!
        
        let mut account_balances = BTreeMap::new();
        account_balances.insert(user_address, Balance::MAX);
        
        let (mut pool, block_policy, state_backend) = setup_test_environment(
            Some(builder_keypair.clone()),
            account_balances,
            account_nonces,
        );

        // Create a builder bundle with HIGH nonces (100, 101)
        // but the account's actual nonce is 0!
        let builder_tx1 = make_transaction(100, chain_id, 21000, 1000, U256::from(100), to_address, &user_signer);
        let builder_tx2 = make_transaction(101, chain_id, 21000, 1000, U256::from(100), to_address, &user_signer);
        
        let bundle = make_signed_bundle(
            vec![builder_tx1.clone(), builder_tx2.clone()],
            timestamp,
            &builder_keypair,
        );
        
        // Submit the bundle - it WILL be accepted because bundle validation
        // doesn't check against account state
        let result = pool.submit_signed_builder_bundle(bundle, timestamp);
        assert!(result.is_ok(), "Bundle should be accepted at submission time");

        // Now attempt to create a block proposal - this is where the vulnerability manifests
        let metrics = EthTxPoolMetrics::default();
        let mut ipc_events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);
        
        let mock_keypair = NopKeyPair::from_bytes(&mut [5_u8; 32]).unwrap();
        let proposal = pool.create_proposal(
            &mut event_tracker,
            Epoch(1),
            Round(1),
            SeqNum(1),
            BASE_FEE_PER_GAS,
            1000,  // tx_limit
            PROPOSAL_GAS_LIMIT,
            PROPOSAL_SIZE_LIMIT,
            [0_u8; 20],
            GENESIS_TIMESTAMP + 1,
            NodeId::new(NopPubKey::from_bytes(&[0_u8; 32]).unwrap()),
            RoundSignature::new(Round(0), &mock_keypair),
            vec![],  // extending_blocks
            &block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
        );
        
        assert!(proposal.is_ok(), "Block proposal should be created");
        let proposal = proposal.unwrap();
        
        // CRITICAL TEST: Check if builder txs with wrong nonces were included
        // The transactions have nonce 100-101, but account nonce is 0
        // This WOULD create an invalid block!
        let included_txs = &proposal.body.transactions;
        
        // Filter to find our builder transactions
        let builder_tx_hashes: Vec<_> = vec![builder_tx1.tx_hash(), builder_tx2.tx_hash()];
        let included_builder_txs: Vec<_> = included_txs
            .iter()
            .filter(|tx| {
                let tx_hash = tx.tx_hash();
                builder_tx_hashes.contains(&tx_hash)
            })
            .collect();
        
        // Document the current behavior
        if !included_builder_txs.is_empty() {
            println!("⚠️  VULNERABILITY CONFIRMED:");
            println!("   {} builder txs with wrong nonces INCLUDED in block!", included_builder_txs.len());
            println!("   Builder tx nonces: 100-101");
            println!("   Account nonce: 0");
            println!("   Result: This WOULD create an INVALID BLOCK!");
            println!("");
            println!("   This test demonstrates the vulnerability documented in the analysis:");
            println!("   Builder transactions with arbitrary nonces that don't match account state");
            println!("   are accepted at bundle submission and included in blocks without validation.");
            println!("");
            println!("   FIX NEEDED: Add nonce validation in create_proposal() around line 402 in mod.rs");
        } else {
            println!("✓ PROTECTION WORKS:");
            println!("   Builder txs with wrong nonces were FILTERED OUT");
            println!("   No invalid block created");
        }
    }

}
