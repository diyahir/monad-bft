# Magma External Block Builder Tests

This test suite verifies that transactions submitted via `monad_submitBuilderBundle` are correctly prioritized and appear at the top of blocks, ahead of regular transactions.

## Overview

The test suite performs the following for each test iteration:

1. **Creates two transactions:**
   - `tx1` (normal): Submitted via standard `eth_sendRawTransaction`
   - `tx2` (builder bundle): Submitted via `monad_submitBuilderBundle`

2. **Submission order:**
   - `tx2` is submitted FIRST via the builder bundle endpoint
   - `tx1` is submitted SECOND via the normal RPC endpoint

3. **Verification:**
   - Waits for both transactions to be mined
   - Checks which block each transaction appears in
   - If in the same block, verifies `tx2` (builder bundle) has a lower transaction index than `tx1`
   - Reports success rate and detailed results

## Prerequisites

- Running Monad single-node Docker environment (see main README)
- Rust toolchain (matches the version specified in `rust-toolchain.toml` at the repo root)
- RPC server accessible at `http://localhost:8080`

## Test Accounts

The tests use Foundry/Anvil pre-funded accounts:

- **Account 1:** `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`
  - Used for normal transactions
  
- **Account 2:** `0x70997970C51812dc3A010C7d01b50e0d17dc79C8`
  - Used for builder bundle transactions

Both accounts have large initial balances on the devnet.

## Running the Tests

### Setup (First Time Only)

1. **Get the builder public key:**
   ```bash
   cd magma-external-blockbuilder-tests
   cargo run --bin print-builder-pubkey
   ```
   
   This will print the public key and the exact TOML configuration you need.

2. **Add the builder to the node config:**
   
   **Option A: Template (recommended)** - Edit `docker/devnet/monad/config/node.toml` and paste the TOML block from step 1.
   
   **Option B: Active instance** - Find the log directory (`docker/single-node/logs/YYYYMMDD_HHMMSS-XXXXX/`) and edit `node/config/node.toml`.

3. **Start/restart the single-node environment:**
   ```bash
   cd ../docker/single-node
   
   # If editing template (Option A):
   nets/run.sh
   
   # If editing active instance (Option B):
   docker compose down
   nets/run.sh --cached-build logs/YYYYMMDD_HHMMSS-XXXXX
   ```

### Running Tests

From the `magma-external-blockbuilder-tests` directory:

```bash
cargo run
```

The test will automatically:
- Check RPC connectivity and verify the chain ID
- Display the builder public key
- Run 5 test iterations (configurable)
- Display detailed results and summary statistics

### With Logging

To see detailed debug logs:

```bash
RUST_LOG=debug cargo run
```

### Running Multiple Iterations

The test runs 10 iterations by default. Edit `src/main.rs` line ~90 to change:

```rust
let num_tests = 10; // Change this number
```

## Expected Behavior

✅ **Success Criteria:**

- When both transactions land in the same block, the builder bundle transaction (`tx2`) should have a lower transaction index than the normal transaction (`tx1`)
- Success rate should be 100% for same-block tests

⚠️ **Note on Timing:**

Due to block production timing, not all test iterations will have both transactions in the same block. This is expected behavior. The test accounts for this by:

- Running multiple iterations (default: 10)
- Tracking which tests had both transactions in the same block
- Calculating success rate only for same-block scenarios

## Test Output

The test provides detailed output including:

```
========================================
TEST SUMMARY
========================================

Total tests run: 10
Tests with both txs in same block: 7
Tests where builder tx came first: 10
Tests where builder tx came first IN SAME BLOCK: 7

Detailed Results:
Test   Normal Blk   Builder Blk  Normal Idx Builder Idx Builder 1st?
----------------------------------------------------------------------
1      100          100          1          0           YES ✓
2      101          101          1          0           YES ✓
...
```

## Architecture

The test suite:

1. Uses the `alloy` Rust library for Ethereum interactions
2. Creates and signs transactions using the pre-funded Foundry accounts
3. Submits transactions via JSON-RPC
4. Polls for transaction receipts to verify inclusion
5. Analyzes block numbers and transaction indices to verify ordering

## Troubleshooting

### RPC Not Responding

The test automatically checks RPC connectivity at startup. If it fails to connect, you'll see:

```
❌ Cannot connect to RPC at http://localhost:8080
Error: ...
Please ensure the single-node Docker environment is running:
  cd docker/single-node
  nets/run.sh
```

Verify the single-node environment is running:

```bash
cd /home/monad/github/monad-bft/docker/single-node
docker ps
```

Expected output should show `monad_rpc-1` container running.

### Builder Bundle Submission Errors

If you see "Builder bundle submission failed" errors, check:

1. The RPC server logs for detailed error messages
2. That the signature validation is not rejecting bundles (current implementation uses dummy signatures for testing)

### All Transactions in Different Blocks

If no tests have both transactions in the same block:

1. Try increasing the number of test iterations (edit `num_tests` in `src/main.rs`)
2. The test includes a 100ms delay between builder bundle and normal transaction submission - you can adjust this timing

## Configuration

Edit `src/main.rs` to customize:

- `RPC_URL`: RPC endpoint (default: `http://localhost:8080`)
- `CHAIN_ID`: Chain ID (default: `20143`)
- `num_tests`: Number of test iterations (default: `10`)
- Transaction parameters (gas price, value, etc.)

## License

GPL-3.0 (matches the Monad BFT repository license)

