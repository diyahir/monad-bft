mod builder_crypto;

use alloy_eips::eip2718::Encodable2718;
use alloy_network::{EthereumWallet, TransactionBuilder};
use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{TransactionReceipt, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use builder_crypto::{BuilderKeypair, compute_bundle_hash};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;
use tracing::{info, warn};

// Foundry/Anvil test accounts with pre-funded balances
const ACCOUNT_1_PRIVATE_KEY: &str =
    "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const ACCOUNT_1_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

const ACCOUNT_2_PRIVATE_KEY: &str =
    "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
const ACCOUNT_2_ADDRESS: &str = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";

const RPC_URL: &str = "http://localhost:8080";
const CHAIN_ID: u64 = 20143;

// Test builder private key (for development only - generate a new one for production)
// This is a randomly generated key just for testing
const BUILDER_PRIVATE_KEY: &str = "a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7";

#[derive(Debug, Serialize, Deserialize)]
struct BuilderBundleParams {
    transactions: Vec<String>,
    signature: String,
    signer: String,
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct BuilderBundleResponse {
    status: String,
}

#[derive(Debug)]
struct TestResult {
    test_number: usize,
    normal_tx_block: u64,
    builder_tx_block: u64,
    normal_tx_index: u64,
    builder_tx_index: u64,
    builder_first: bool,
    same_block: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("========================================");
    info!("Magma External Block Builder Tests");
    info!("========================================\n");
    
    // Check RPC connectivity
    info!("Checking RPC connectivity at {}...", RPC_URL);
    match check_rpc_connectivity().await {
        Ok(chain_id) => {
            info!("✓ RPC is responding");
            info!("✓ Chain ID: {} (0x{:x})", chain_id, chain_id);
            if chain_id != CHAIN_ID {
                warn!("⚠ Warning: Expected chain ID {}, got {}", CHAIN_ID, chain_id);
            }
        }
        Err(e) => {
            tracing::error!("❌ Cannot connect to RPC at {}", RPC_URL);
            tracing::error!("Error: {:?}", e);
            tracing::error!("\nPlease ensure the single-node Docker environment is running:");
            tracing::error!("  cd docker/single-node");
            tracing::error!("  nets/run.sh");
            return Err(e);
        }
    }

    info!("\nStarting Monad Builder Bundle Transaction Ordering Tests");
    info!("RPC URL: {}", RPC_URL);
    info!("Expected Chain ID: {}", CHAIN_ID);

    // Load builder keypair
    let builder_keypair = BuilderKeypair::from_hex(BUILDER_PRIVATE_KEY)?;
    
    info!("\n========== Builder Bundle Connectivity Test ==========");
    info!("Testing if builder bundles are being accepted by the node...\n");
    
    // First, run a connectivity test for builder bundles
    match test_builder_bundle_connectivity(&builder_keypair).await {
        Ok(()) => {
            info!("✓ Builder bundle connectivity test PASSED!");
            info!("  The node is accepting and mining builder bundles.\n");
        }
        Err(e) => {
            tracing::error!("✗ Builder bundle connectivity test FAILED!");
            tracing::error!("  Error: {:?}\n", e);
            tracing::error!("Please check:");
            tracing::error!("  1. The builder public key is correctly added to node.toml");
            tracing::error!("  2. Run: cargo run --bin print-builder-pubkey");
            tracing::error!("  3. Add the public key to the active node.toml");
            tracing::error!("  4. Restart the single-node environment");
            return Err(e);
        }
    }

    // Run bundle ordering test
    info!("\n========== Bundle Ordering Test ==========");
    info!("Testing that transactions in a bundle maintain their specified order...\n");
    
    match test_bundle_ordering(&builder_keypair).await {
        Ok(()) => {
            info!("✓ Bundle ordering test PASSED!");
            info!("  All transactions appeared in the correct order.\n");
        }
        Err(e) => {
            tracing::error!("✗ Bundle ordering test FAILED!");
            tracing::error!("  Error: {:?}\n", e);
        }
    }

    // Run multiple test iterations
    let num_tests = 10;
    let mut results = Vec::new();

    for i in 1..=num_tests {
        tracing::debug!("\n========== Test Iteration {} ==========", i);
        match run_single_test(i, &builder_keypair).await {
            Ok(result) => {
                results.push(result);
            }
            Err(e) => {
                warn!("Test {} failed: {:?}", i, e);
            }
        }
        // Small delay between tests
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Print summary
    print_summary(&results);

    Ok(())
}

async fn check_rpc_connectivity() -> Result<u64> {
    let client = reqwest::Client::new();
    
    let request = json!({
        "jsonrpc": "2.0",
        "method": "eth_chainId",
        "params": [],
        "id": 1
    });

    let response = client
        .post(RPC_URL)
        .json(&request)
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
        .context("Failed to connect to RPC endpoint")?;

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("Failed to parse RPC response")?;

    if let Some(error) = response_json.get("error") {
        anyhow::bail!("RPC error: {}", error);
    }

    let chain_id_hex = response_json
        .get("result")
        .and_then(|v| v.as_str())
        .context("Missing result in RPC response")?;

    let chain_id = u64::from_str_radix(chain_id_hex.trim_start_matches("0x"), 16)
        .context("Failed to parse chain ID")?;

    Ok(chain_id)
}

async fn test_bundle_ordering(builder_keypair: &BuilderKeypair) -> Result<()> {
    tracing::debug!("Creating 6 transactions interleaved between two accounts...");
    
    // Setup signers for both accounts
    let signer1 = PrivateKeySigner::from_str(ACCOUNT_1_PRIVATE_KEY)?;
    let signer2 = PrivateKeySigner::from_str(ACCOUNT_2_PRIVATE_KEY)?;
    
    let wallet1 = EthereumWallet::from(signer1.clone());
    let wallet2 = EthereumWallet::from(signer2.clone());
    
    // Create providers
    let provider1 = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet1.clone())
        .on_http(RPC_URL.parse()?);
    
    let provider2 = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet2.clone())
        .on_http(RPC_URL.parse()?);
    
    let addr1 = Address::from_str(ACCOUNT_1_ADDRESS)?;
    let addr2 = Address::from_str(ACCOUNT_2_ADDRESS)?;
    
    // Get current nonces
    let nonce1 = provider1.get_transaction_count(addr1).await?;
    let nonce2 = provider2.get_transaction_count(addr2).await?;
    
    tracing::debug!("Account 1 starting nonce: {}", nonce1);
    tracing::debug!("Account 2 starting nonce: {}", nonce2);
    
    // Create 6 transactions, interleaved between accounts
    // Pattern: acc1[0], acc2[0], acc1[1], acc2[1], acc1[2], acc2[2]
    let mut bundle_txs = Vec::new();
    let mut expected_hashes = Vec::new();
    
    for i in 0..3 {
        // Transaction from account 1
        let tx1 = TransactionRequest::default()
            .with_to(addr2)
            .with_value(U256::from(1000000000000000u64)) // 0.001 ETH
            .with_gas_limit(21000)
            .with_max_fee_per_gas(100_000_000_000)
            .with_max_priority_fee_per_gas(100_000_000_000)
            .with_chain_id(CHAIN_ID)
            .with_nonce(nonce1 + i);
        
        let tx1_envelope = tx1.build(&wallet1).await?;
        let tx1_encoded = tx1_envelope.encoded_2718();
        let tx1_hash = keccak256(&tx1_encoded);
        bundle_txs.push(hex::encode(&tx1_encoded));
        expected_hashes.push(format!("0x{}", hex::encode(tx1_hash)));
        
        tracing::debug!("  Created acc1 tx {} with nonce {}: {}", i, nonce1 + i, expected_hashes.last().unwrap());
        
        // Transaction from account 2
        let tx2 = TransactionRequest::default()
            .with_to(addr1)
            .with_value(U256::from(1000000000000000u64)) // 0.001 ETH
            .with_gas_limit(21000)
            .with_max_fee_per_gas(100_000_000_000)
            .with_max_priority_fee_per_gas(100_000_000_000)
            .with_chain_id(CHAIN_ID)
            .with_nonce(nonce2 + i);
        
        let tx2_envelope = tx2.build(&wallet2).await?;
        let tx2_encoded = tx2_envelope.encoded_2718();
        let tx2_hash = keccak256(&tx2_encoded);
        bundle_txs.push(hex::encode(&tx2_encoded));
        expected_hashes.push(format!("0x{}", hex::encode(tx2_hash)));
        
        tracing::debug!("  Created acc2 tx {} with nonce {}: {}", i, nonce2 + i, expected_hashes.last().unwrap());
    }
    
    tracing::debug!("Submitting bundle with 6 transactions...");
    tracing::debug!("Expected order: acc1[0], acc2[0], acc1[1], acc2[1], acc1[2], acc2[2]");
    
    // Submit the bundle
    submit_builder_bundle(bundle_txs, builder_keypair).await?;
    tracing::debug!("Bundle submitted successfully");
    
    // Wait for all transactions to be mined
    tracing::debug!("Waiting for all transactions to be mined...");
    let mut receipts = Vec::new();
    
    for (i, tx_hash) in expected_hashes.iter().enumerate() {
        match wait_for_transaction_receipt(tx_hash, 30).await {
            Ok(receipt) => {
                tracing::debug!("  TX[{}] mined in block {}, index {}", 
                      i, 
                      receipt.block_number.unwrap_or(0),
                      receipt.transaction_index.unwrap_or(0));
                receipts.push(receipt);
            }
            Err(e) => {
                tracing::error!("  TX[{}] failed to mine: {:?}", i, e);
                anyhow::bail!("Transaction {} failed to mine", i);
            }
        }
    }
    
    // Verify all transactions are in the same block
    let first_block = receipts[0].block_number.context("Missing block number")?;
    let all_same_block = receipts.iter().all(|r| r.block_number == Some(first_block));
    
    if !all_same_block {
        anyhow::bail!("Not all transactions appeared in the same block!");
    }
    
    info!("All 6 transactions appeared in block {}", first_block);
    
    // Verify ordering
    tracing::debug!("Verifying transaction order...");
    let indices: Vec<(usize, u64)> = receipts
        .iter()
        .enumerate()
        .map(|(i, r)| (i, r.transaction_index.unwrap_or(u64::MAX)))
        .collect();
    
    // Check if indices are sequential
    let first_index = indices[0].1;
    let mut order_correct = true;
    
    for (i, (_tx_num, index)) in indices.iter().enumerate() {
        let expected_index = first_index + i as u64;
        tracing::debug!("  TX[{}]: index {} (expected {})", i, index, expected_index);
        
        if *index != expected_index {
            order_correct = false;
            tracing::error!("  ✗ TX[{}] has wrong index! Expected {}, got {}", i, expected_index, index);
        }
    }
    
    if !order_correct {
        anyhow::bail!("Transactions did not appear in the correct order!");
    }
    
    info!("All transactions maintained correct sequential order");
    Ok(())
}

async fn test_builder_bundle_connectivity(builder_keypair: &BuilderKeypair) -> Result<()> {
    tracing::debug!("Creating a simple test transaction for builder bundle...");
    
    // Setup signer for test account
    let signer = PrivateKeySigner::from_str(ACCOUNT_2_PRIVATE_KEY)?;
    let wallet = EthereumWallet::from(signer.clone());
    
    // Create provider
    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet.clone())
        .on_http(RPC_URL.parse()?);
    
    let addr1 = Address::from_str(ACCOUNT_1_ADDRESS)?;
    let addr2 = Address::from_str(ACCOUNT_2_ADDRESS)?;
    
    let nonce = provider.get_transaction_count(addr2).await?;
    tracing::debug!("Test account nonce: {}", nonce);
    
    // Create a simple transaction
    let tx = TransactionRequest::default()
        .with_to(addr1)
        .with_value(U256::from(1000000000000000u64)) // 0.001 ETH
        .with_gas_limit(21000)
        .with_max_fee_per_gas(200_000_000_000) // 200 gwei
        .with_max_priority_fee_per_gas(200_000_000_000)
        .with_chain_id(CHAIN_ID)
        .with_nonce(nonce);
    
    // Sign the transaction
    let tx_envelope = tx.build(&wallet).await?;
    let tx_encoded = tx_envelope.encoded_2718();
    let tx_hex = hex::encode(&tx_encoded);
    let tx_hash = keccak256(&tx_encoded);
    let expected_hash = format!("0x{}", hex::encode(tx_hash));
    
    tracing::debug!("Test transaction hash: {}", expected_hash);
    tracing::debug!("Submitting via builder bundle...");
    
    // Submit via builder bundle
    let returned_hash = submit_builder_bundle(vec![tx_hex], builder_keypair).await?;
    tracing::debug!("Builder bundle accepted, returned hash: {}", returned_hash);
    
    // Wait for transaction to be mined
    tracing::debug!("Waiting for transaction to be mined (3 second timeout)...");
    match wait_for_transaction_receipt(&expected_hash, 3).await {
        Ok(receipt) => {
            tracing::debug!("Transaction mined in block {}", receipt.block_number.unwrap_or(0));
            Ok(())
        }
        Err(e) => {
            tracing::error!("✗ Transaction was not mined within timeout");
            tracing::error!("  This likely means the builder bundle was rejected by the node.");
            Err(e)
        }
    }
}

async fn run_single_test(test_num: usize, builder_keypair: &BuilderKeypair) -> Result<TestResult> {
    // Setup signers
    let signer1 = PrivateKeySigner::from_str(ACCOUNT_1_PRIVATE_KEY)?;
    let signer2 = PrivateKeySigner::from_str(ACCOUNT_2_PRIVATE_KEY)?;

    let wallet1 = EthereumWallet::from(signer1.clone());
    let wallet2 = EthereumWallet::from(signer2.clone());

    // Create providers
    let provider1 = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet1)
        .on_http(RPC_URL.parse()?);

    let provider2 = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet2.clone())
        .on_http(RPC_URL.parse()?);

    // Get current nonces
    let addr1 = Address::from_str(ACCOUNT_1_ADDRESS)?;
    let addr2 = Address::from_str(ACCOUNT_2_ADDRESS)?;

    let nonce1 = provider1.get_transaction_count(addr1).await?;
    let nonce2 = provider2.get_transaction_count(addr2).await?;

    tracing::debug!("Account 1 nonce: {}", nonce1);
    tracing::debug!("Account 2 nonce: {}", nonce2);

    // Create normal transaction (tx1) - simple transfer
    let tx1 = TransactionRequest::default()
        .with_to(addr2)
        .with_value(U256::from(1000000000000000u64)) // 0.001 ETH
        .with_gas_limit(21000)
        .with_max_fee_per_gas(100_000_000_000) // 100 gwei
        .with_max_priority_fee_per_gas(100_000_000_000)
        .with_chain_id(CHAIN_ID)
        .with_nonce(nonce1);

    // Create builder bundle transaction (tx2) - also simple transfer
    let tx2 = TransactionRequest::default()
        .with_to(addr1)
        .with_value(U256::from(1000000000000000u64)) // 0.001 ETH
        .with_gas_limit(21000)
        .with_max_fee_per_gas(200_000_000_000) // 200 gwei - higher to ensure it would be picked
        .with_max_priority_fee_per_gas(200_000_000_000)
        .with_chain_id(CHAIN_ID)
        .with_nonce(nonce2);

    // Sign tx2 for the builder bundle
    let tx2_envelope = tx2.build(&wallet2).await?;
    let tx2_encoded = tx2_envelope.encoded_2718();
    let tx2_hex = hex::encode(&tx2_encoded); // No 0x prefix for builder bundle

    tracing::debug!("Signed tx2 for builder bundle");

    // Submit both transactions concurrently to minimize delay
    // The normal tx is initiated first, but both requests are sent without waiting
    let normal_future = provider1.send_transaction(tx1);
    let builder_future = submit_builder_bundle(vec![tx2_hex.clone()], builder_keypair);
    
    let (pending_tx1_result, builder_tx_hash_result) = tokio::join!(normal_future, builder_future);
    let pending_tx1 = pending_tx1_result?;
    let builder_tx_hash = builder_tx_hash_result?;
    
    let normal_tx_hash = format!("{:?}", pending_tx1.tx_hash());
    tracing::debug!("Normal transaction submitted: {}", normal_tx_hash);
    tracing::debug!("Builder bundle submitted: {}", builder_tx_hash);

    // Wait for both transactions to be mined
    tracing::debug!("Waiting for transactions to be mined...");

    let normal_receipt = pending_tx1
        .get_receipt()
        .await
        .context("Failed to get normal transaction receipt")?;

    // Get builder transaction receipt by hash
    let builder_receipt = wait_for_transaction_receipt(&builder_tx_hash, 30).await?;

    let normal_block = normal_receipt.block_number.unwrap_or(0);
    let builder_block = builder_receipt.block_number.unwrap_or(0);
    let normal_index = normal_receipt.transaction_index.unwrap_or(0);
    let builder_index = builder_receipt.transaction_index.unwrap_or(0);

    tracing::debug!("Normal tx: block {}, index {}", normal_block, normal_index);
    tracing::debug!(
        "Builder tx: block {}, index {}",
        builder_block, builder_index
    );

    let same_block = normal_block == builder_block;
    let builder_first = if same_block {
        builder_index < normal_index
    } else {
        builder_block < normal_block
    };

    if same_block {
        if builder_first {
            info!("✓ Test {}: Builder tx first (block {}, indices: builder={}, normal={})", 
                  test_num, normal_block, builder_index, normal_index);
        } else {
            info!("✗ Test {}: Normal tx first (block {}, indices: builder={}, normal={})", 
                  test_num, normal_block, builder_index, normal_index);
        }
    } else {
        info!("Test {}: Different blocks (builder={}, normal={})", test_num, builder_block, normal_block);
    }

    Ok(TestResult {
        test_number: test_num,
        normal_tx_block: normal_block,
        builder_tx_block: builder_block,
        normal_tx_index: normal_index,
        builder_tx_index: builder_index,
        builder_first,
        same_block,
    })
}

async fn submit_builder_bundle(transactions: Vec<String>, builder_keypair: &BuilderKeypair) -> Result<String> {
    let client = reqwest::Client::new();

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    // Compute transaction hashes for signing
    let mut tx_hashes = Vec::new();
    for (i, tx_hex) in transactions.iter().enumerate() {
        let tx_bytes = hex::decode(tx_hex.trim_start_matches("0x"))?;
        let tx_hash = keccak256(&tx_bytes);
        tracing::debug!("  TX[{}] hash: 0x{}", i, hex::encode(&tx_hash));
        tx_hashes.push(tx_hash.0);
    }

    // Compute bundle hash and sign it
    let tx_hash_refs: Vec<&[u8; 32]> = tx_hashes.iter().collect();
    let bundle_hash = compute_bundle_hash(&tx_hash_refs, timestamp);
    let signature_bytes = builder_keypair.sign_bundle(&bundle_hash)?;
    let signer_pubkey = builder_keypair.public_key_hex();
    
    // Self-verify the signature before sending
    let sig_valid = builder_keypair.verify_bundle_signature(&bundle_hash, &signature_bytes)?;
    tracing::debug!("=== Bundle Signature Details ===");
    tracing::debug!("  Timestamp: {}", timestamp);
    tracing::debug!("  Bundle hash: 0x{}", hex::encode(&bundle_hash));
    tracing::debug!("  Signature: 0x{}", hex::encode(&signature_bytes));
    tracing::debug!("  Signer pubkey: {}", signer_pubkey);
    tracing::debug!("  Self-verification: {}", if sig_valid { "✓ VALID" } else { "✗ INVALID" });
    tracing::debug!("  Num transactions: {}", transactions.len());
    
    if !sig_valid {
        anyhow::bail!("Self-verification of bundle signature failed!");
    }

    let params = BuilderBundleParams {
        transactions: transactions.clone(),
        signature: hex::encode(&signature_bytes),
        signer: signer_pubkey,
        timestamp,
    };

    let request = json!({
        "jsonrpc": "2.0",
        "method": "monad_submitBuilderBundle",
        "params": params,
        "id": 1
    });

    let response = client
        .post(RPC_URL)
        .json(&request)
        .send()
        .await
        .context("Failed to send builder bundle request")?;

    let response_text = response.text().await?;
    tracing::debug!("RPC Response: {}", response_text);
    let response_json: serde_json::Value = serde_json::from_str(&response_text)?;

    if let Some(error) = response_json.get("error") {
        anyhow::bail!("Builder bundle submission failed: {}", error);
    }
    
    tracing::debug!("Bundle accepted by RPC: {:?}", response_json.get("result"));

    // Extract transaction hash from the first transaction
    // The transaction hash is deterministic based on the signed transaction
    let tx_bytes = hex::decode(transactions[0].trim_start_matches("0x"))?;
    let tx_hash = keccak256(&tx_bytes);
    Ok(format!("0x{}", hex::encode(tx_hash)))
}

async fn wait_for_transaction_receipt(
    tx_hash: &str,
    timeout_secs: u64,
) -> Result<TransactionReceipt> {
    let provider = ProviderBuilder::new().on_http(RPC_URL.parse()?);

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for transaction receipt: {}", tx_hash);
        }

        let tx_hash_parsed = B256::from_str(tx_hash.trim_start_matches("0x"))?;

        if let Some(receipt) = provider.get_transaction_receipt(tx_hash_parsed).await? {
            return Ok(receipt);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}

fn print_summary(results: &[TestResult]) {
    info!("\n\n========================================");
    info!("TEST SUMMARY");
    info!("========================================\n");

    let total = results.len();
    let same_block_count = results.iter().filter(|r| r.same_block).count();
    let builder_first_count = results.iter().filter(|r| r.builder_first).count();
    let builder_first_same_block = results
        .iter()
        .filter(|r| r.same_block && r.builder_first)
        .count();

    info!("Total tests run: {}", total);
    info!("Tests with both txs in same block: {}", same_block_count);
    info!("Tests where builder tx came first: {}", builder_first_count);
    info!(
        "Tests where builder tx came first IN SAME BLOCK: {}",
        builder_first_same_block
    );

    info!("\nDetailed Results:");
    info!("{:<6} {:<12} {:<12} {:<10} {:<10} {:<12}", "Test", "Normal Blk", "Builder Blk", "Normal Idx", "Builder Idx", "Builder 1st?");
    info!("{}", "-".repeat(70));

    for result in results {
        info!(
            "{:<6} {:<12} {:<12} {:<10} {:<10} {:<12}",
            result.test_number,
            result.normal_tx_block,
            result.builder_tx_block,
            result.normal_tx_index,
            result.builder_tx_index,
            if result.builder_first { "YES ✓" } else { "NO ✗" }
        );
    }

    info!("\n========================================");

    if same_block_count > 0 {
        let success_rate =
            (builder_first_same_block as f64 / same_block_count as f64) * 100.0;
        info!(
            "\nBuilder bundle success rate (same block): {:.1}%",
            success_rate
        );

        if success_rate == 100.0 {
            info!("✓ ALL builder bundle transactions appeared first in their blocks!");
        } else if success_rate >= 80.0 {
            warn!(
                "⚠ Most builder bundle transactions appeared first, but not all."
            );
        } else {
            warn!("✗ Builder bundle transactions did NOT consistently appear first!");
        }
    } else {
        warn!("⚠ No tests had both transactions in the same block - may need to run more tests or adjust timing.");
    }
}

