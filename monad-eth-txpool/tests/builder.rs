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

//! Tests for the external block builder transaction pool functionality

use alloy_consensus::{transaction::Recovered, SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{hex, keccak256, Address, B256, TxKind};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::{
    ChainConfig, MockChainConfig, 
    execution_revision::{ExecutionChainParams, MonadExecutionRevision}, 
    revision::{ChainParams, ChainRevision, MockChainRevision}
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    NopKeyPair, NopSignature,
};
use monad_eth_txpool::builder::{
    ExternalBlockBuilderDomain, ExternalBuilderError, ExternalBuilderTxPool, SignedExternalBuilderBundle,
};
use monad_eth_txpool_types::TransactionError;
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
        MonadExecutionRevision::LATEST.execution_chain_params(),
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

#[test]
fn test_bundle_replacement() {
    let keypair = make_test_keypair(TEST_SECRET);
    let mut pool = ExternalBuilderTxPool::new(vec![keypair.pubkey()], 300);
    
    let (chain_params, execution_params) = get_test_chain_params();
    let chain_id = MockChainConfig::DEFAULT.chain_id();
    
    // Create first bundle with one transaction
    let tx_signer1 = PrivateKeySigner::from_bytes(&B256::new(hex!(
        "1111111111111111111111111111111111111111111111111111111111111111"
    ))).unwrap();
    let tx1 = make_test_transaction(0, chain_id, &tx_signer1);
    
    let timestamp1 = current_timestamp();
    let bundle1 = SignedExternalBuilderBundle {
        transactions: vec![tx1.clone()],
        signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&[]).0, &keypair),
        signer: keypair.pubkey(),
        timestamp: timestamp1,
    };
    
    let bundle1_hash = bundle1.compute_bundle_hash();
    let bundle1_with_sig = SignedExternalBuilderBundle {
        transactions: bundle1.transactions,
        signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&bundle1_hash.0, &keypair),
        signer: keypair.pubkey(),
        timestamp: timestamp1,
    };
    
    // Submit first bundle
    let result1 = pool.add_signed_bundle(
        bundle1_with_sig,
        timestamp1,
        chain_id,
        chain_params,
        execution_params,
    );
    assert!(result1.is_ok());
    assert_eq!(pool.len(), 1);
    
    // Create second bundle with different transactions from different senders
    let tx_signer2 = PrivateKeySigner::from_bytes(&B256::new(hex!(
        "2222222222222222222222222222222222222222222222222222222222222222"
    ))).unwrap();
    let tx_signer3 = PrivateKeySigner::from_bytes(&B256::new(hex!(
        "3333333333333333333333333333333333333333333333333333333333333333"
    ))).unwrap();
    let tx2 = make_test_transaction(0, chain_id, &tx_signer2);
    let tx3 = make_test_transaction(0, chain_id, &tx_signer3);
    
    let timestamp2 = timestamp1 + 1;
    let bundle2 = SignedExternalBuilderBundle {
        transactions: vec![tx2.clone(), tx3.clone()],
        signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&keccak256(&[]).0, &keypair),
        signer: keypair.pubkey(),
        timestamp: timestamp2,
    };
    
    let bundle2_hash = bundle2.compute_bundle_hash();
    let bundle2_with_sig = SignedExternalBuilderBundle {
        transactions: bundle2.transactions,
        signature: NopSignature::sign::<ExternalBlockBuilderDomain>(&bundle2_hash.0, &keypair),
        signer: keypair.pubkey(),
        timestamp: timestamp2,
    };
    
    // Submit second bundle - should replace first
    let result2 = pool.add_signed_bundle(
        bundle2_with_sig,
        timestamp2,
        chain_id,
        chain_params,
        execution_params,
    );
    assert!(result2.is_ok());
    
    // Pool should now have 2 transactions (from second bundle)
    assert_eq!(pool.len(), 2);
    
    // Get transactions - should only get those from second bundle
    let (txs, _metadata) = pool.get_transactions(10);
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[0].tx_hash(), tx2.tx_hash());
    assert_eq!(txs[1].tx_hash(), tx3.tx_hash());
    
    // Pool should now be empty after consumption
    assert_eq!(pool.len(), 0);
    assert!(pool.is_empty());
}

