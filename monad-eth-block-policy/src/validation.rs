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

use alloy_consensus::{Transaction, TxEnvelope};
use monad_eth_txpool_types::TransactionError;

/// Stateless helper function to check validity of an Ethereum transaction
pub fn static_validate_transaction(
    tx: &TxEnvelope,
    chain_id: u64,
    proposal_gas_limit: u64,
    max_code_size: usize,
) -> Result<(), TransactionError> {
    // post Ethereum Homestead fork validation
    // includes EIP-155 validation
    EthHomesteadForkValidation::validate(tx, chain_id)?;

    // post Ethereum London fork validation
    // includes EIP-1559 validation
    EthLondonForkValidation::validate(tx)?;

    // post Ethereum Shanghai fork validation
    // includes EIP-3860 validation
    EthShanghaiForkValidation::validate(tx, max_code_size)?;

    // Ethereum Yellow paper intrinsic gas validation
    YellowPaperValidation::validate(tx)?;

    // post Ethereum Prague fork validation
    // includes EIP-7623 validation
    EthPragueForkValidation::validate(tx)?;

    if tx.gas_limit() > proposal_gas_limit {
        return Err(TransactionError::GasLimitTooHigh);
    }

    if tx.is_eip4844() || tx.is_eip7702() {
        return Err(TransactionError::UnsupportedTransactionType);
    }

    Ok(())
}

struct YellowPaperValidation {}
impl YellowPaperValidation {
    fn validate(tx: &TxEnvelope) -> Result<(), TransactionError> {
        Self::intrinsic_gas_validation(tx)
    }
    fn intrinsic_gas_validation(tx: &TxEnvelope) -> Result<(), TransactionError> {
        // YP eq. 62 - intrinsic gas validation
        let intrinsic_gas = compute_intrinsic_gas(tx);
        if tx.gas_limit() < intrinsic_gas {
            return Err(TransactionError::GasLimitTooLow);
        }
        Ok(())
    }
}

struct EthHomesteadForkValidation {}
impl EthHomesteadForkValidation {
    fn validate(tx: &TxEnvelope, chain_id: u64) -> Result<(), TransactionError> {
        Self::eip_2(tx)?;
        Self::eip_155(tx, chain_id)
    }
    fn eip_2(tx: &TxEnvelope) -> Result<(), TransactionError> {
        // verify that s is in the lower half of the curve
        if tx.signature().normalize_s().is_some() {
            return Err(TransactionError::UnsupportedTransactionType);
        }
        Ok(())
    }
    fn eip_155(tx: &TxEnvelope, chain_id: u64) -> Result<(), TransactionError> {
        // We still allow legacy transactions without chain_id specified to pass through
        if let Some(tx_chain_id) = tx.chain_id() {
            if tx_chain_id != chain_id {
                return Err(TransactionError::InvalidChainId);
            }
        }
        Ok(())
    }
}

struct EthLondonForkValidation {}
impl EthLondonForkValidation {
    fn validate(tx: &TxEnvelope) -> Result<(), TransactionError> {
        Self::eip_1559(tx)
    }
    fn eip_1559(tx: &TxEnvelope) -> Result<(), TransactionError> {
        if let Some(max_priority_fee) = tx.max_priority_fee_per_gas() {
            if max_priority_fee > tx.max_fee_per_gas() {
                return Err(TransactionError::MaxPriorityFeeTooHigh);
            }
        }
        Ok(())
    }
}

struct EthShanghaiForkValidation {}
impl EthShanghaiForkValidation {
    fn validate(tx: &TxEnvelope, max_code_size: usize) -> Result<(), TransactionError> {
        Self::eip_3860(tx, max_code_size)
    }
    fn eip_3860(tx: &TxEnvelope, max_code_size: usize) -> Result<(), TransactionError> {
        // max init_code is (2 * max_code_size)
        let max_init_code_size: usize = 2 * max_code_size;
        if tx.kind().is_create() && tx.input().len() > max_init_code_size {
            return Err(TransactionError::InitCodeLimitExceeded);
        }
        Ok(())
    }
}

struct EthPragueForkValidation {}
impl EthPragueForkValidation {
    fn validate(tx: &TxEnvelope) -> Result<(), TransactionError> {
        Self::eip_7623(tx)
    }
    fn eip_7623(tx: &TxEnvelope) -> Result<(), TransactionError> {
        let floor_data_gas = compute_floor_data_gas(tx);
        if tx.gas_limit() < floor_data_gas {
            return Err(TransactionError::GasLimitTooLow);
        }
        Ok(())
    }
}

fn compute_intrinsic_gas(tx: &TxEnvelope) -> u64 {
    // base stipend
    let mut intrinsic_gas = 21000;

    // YP, Eqn. 60, first summation
    // 4 gas for each zero byte and 16 gas for each non zero byte
    let zero_data_len = tx.input().iter().filter(|v| **v == 0).count() as u64;
    let non_zero_data_len = tx.input().len() as u64 - zero_data_len;
    intrinsic_gas += zero_data_len * 4;
    // EIP-2028: Transaction data gas cost reduction (was originally 64 for non zero byte)
    intrinsic_gas += non_zero_data_len * 16;

    if tx.kind().is_create() {
        // adds 32000 to intrinsic gas if transaction is contract creation
        intrinsic_gas += 32000;
        // EIP-3860: Limit and meter initcode
        // Init code stipend for bytecode analysis
        intrinsic_gas += ((tx.input().len() as u64 + 31) / 32) * 2;
    }

    // EIP-2930
    let access_list = tx
        .access_list()
        .map(|list| list.0.as_slice())
        .unwrap_or(&[]);
    let accessed_slots: usize = access_list.iter().map(|item| item.storage_keys.len()).sum();
    // each address in access list costs 2400 gas
    intrinsic_gas += access_list.len() as u64 * 2400;
    // each storage key in access list costs 1900 gas
    intrinsic_gas += accessed_slots as u64 * 1900;

    intrinsic_gas
}

fn compute_floor_data_gas(tx: &TxEnvelope) -> u64 {
    // EIP-7623
    let zero_data_len = tx.input().iter().filter(|v| **v == 0).count() as u64;
    let non_zero_data_len = tx.input().len() as u64 - zero_data_len;
    21_000 + (zero_data_len * 10 + non_zero_data_len * 40)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_primitives::{Address, Bytes, FixedBytes, PrimitiveSignature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    use super::*;

    fn sign_tx(signature_hash: &FixedBytes<32>) -> PrimitiveSignature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    #[test]
    fn test_static_validate_transaction() {
        let address = Address(FixedBytes([0x11; 20]));
        const CHAIN_ID: u64 = 1337;
        const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;

        // pre EIP-155 transaction with no chain id is allowed
        let tx_no_chain_id = TxLegacy {
            chain_id: None,
            nonce: 0,
            to: TxKind::Call(address),
            gas_price: 1000,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let signature = sign_tx(&tx_no_chain_id.signature_hash());
        let txn = tx_no_chain_id.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Ok(())));

        // transaction with incorrect chain id
        let tx_invalid_chain_id = TxEip1559 {
            chain_id: CHAIN_ID - 1,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let signature = sign_tx(&tx_invalid_chain_id.signature_hash());
        let txn = tx_invalid_chain_id.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::InvalidChainId)));

        // contract deployment transaction with input data larger than 2 * 0x6000 (initcode limit)
        let input = vec![0; 2 * 0x6000 + 1];
        let tx_over_initcode_limit = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Create,
            max_fee_per_gas: 10000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            input: input.into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_over_initcode_limit.signature_hash());
        let txn = tx_over_initcode_limit.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(
            result,
            Err(TransactionError::InitCodeLimitExceeded)
        ));

        // transaction with larger max priority fee than max fee per gas
        let tx_priority_fee_too_high = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10000,
            gas_limit: 1_000_000,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_priority_fee_too_high.signature_hash());
        let txn = tx_priority_fee_too_high.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(
            result,
            Err(TransactionError::MaxPriorityFeeTooHigh)
        ));

        // transaction with gas limit lower than intrinsic gas
        let tx_gas_limit_too_low = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 20_000,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_low.signature_hash());
        let txn = tx_gas_limit_too_low.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::GasLimitTooLow)));

        // transaction with gas limit lower than floor data gas
        // floor data gas is 21000 + (zero byte * 10) + (non-zero byte * 40)
        let tx_gas_limit_too_low_data = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 30_000,
            input: vec![0xaa; 226].into(), // 21000 + (226 * 40) > 30000
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_low_data.signature_hash());
        let txn = tx_gas_limit_too_low_data.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::GasLimitTooLow)));

        // transaction with gas limit higher than block gas limit
        let tx_gas_limit_too_high = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: PROPOSAL_GAS_LIMIT + 1,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_high.signature_hash());
        let txn = tx_gas_limit_too_high.into_signed(signature);

        let result = static_validate_transaction(&txn.into(), CHAIN_ID, PROPOSAL_GAS_LIMIT, 0x6000);
        assert!(matches!(result, Err(TransactionError::GasLimitTooHigh)));
    }

    #[test]
    fn test_compute_floor_data_gas() {
        const CHAIN_ID: u64 = 1337;
        let tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(Address(FixedBytes([0x11; 20]))),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            // input data with 3 zero byte and 4 non-zero byte
            input: Bytes::from_str("0x12003456000078").unwrap(),
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let tx = tx.into_signed(signature);

        let result = compute_floor_data_gas(&tx.into());
        assert_eq!(result, 21000 + (3 * 10) + (4 * 40));
    }

    #[test]
    fn test_compute_intrinsic_gas() {
        const CHAIN_ID: u64 = 1337;
        let tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Create,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            input: Bytes::from_str("0x6040608081523462000414").unwrap(),
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let tx = tx.into_signed(signature);

        let result = compute_intrinsic_gas(&tx.into());
        assert_eq!(result, 53166);
    }
}
