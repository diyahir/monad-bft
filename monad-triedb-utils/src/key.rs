use alloy_primitives::keccak256;
use alloy_rlp::Encodable;
use monad_types::Round;
use tracing::warn;

// Table nibbles
const STATE_NIBBLE: u8 = 0;
const CODE_NIBBLE: u8 = 1;
const RECEIPT_NIBBLE: u8 = 2;
const TRANSACTION_NIBBLE: u8 = 3;
const BLOCK_HEADER_NIBBLE: u8 = 4;
const TRANSACTION_HASH_NIBBLE: u8 = 7;
const BLOCK_HASH_NIBBLE: u8 = 8;
const CALL_FRAME_NIBBLE: u8 = 9;
const BFT_BLOCK_NIBBLE: u8 = 10;

// table_key = concat(proposal nibble, little_endian(round - 8 bytes), table_nibble)
const PROPOSAL_NIBBLE: u8 = 0;
// table_key = concat(finalized nibble, table_nibble)
const FINALIZED_NIBBLE: u8 = 1;

#[derive(Debug, Clone, Copy)]
pub enum Version {
    Proposal(Round),
    Finalized,
}

pub enum KeyInput<'a> {
    Address(&'a [u8; 20]),
    Storage(&'a [u8; 20], &'a [u8; 32]),
    CodeHash(&'a [u8; 32]),
    ReceiptIndex(Option<u64>),
    TxIndex(Option<u64>),
    BlockHeader,
    TxHash(&'a [u8; 32]),
    BlockHash(&'a [u8; 32]),
    CallFrame(Option<u64>),
    BftBlock,
}

pub fn create_triedb_key(version: Version, key: KeyInput) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    match version {
        Version::Proposal(round) => {
            key_nibbles.push(PROPOSAL_NIBBLE);
            for byte in round.0.to_be_bytes() {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        Version::Finalized => {
            key_nibbles.push(FINALIZED_NIBBLE);
        }
    };

    match key {
        KeyInput::Address(addr) => {
            key_nibbles.push(STATE_NIBBLE);
            let hashed_addr = keccak256(addr);
            for byte in hashed_addr {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::Storage(addr, at) => {
            key_nibbles.push(STATE_NIBBLE);
            let hashed_addr = keccak256(addr);
            for byte in hashed_addr {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }

            let hashed_at = keccak256(at);
            for byte in hashed_at {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::CodeHash(code_hash) => {
            key_nibbles.push(CODE_NIBBLE);
            for byte in code_hash {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::ReceiptIndex(receipt_index) => {
            key_nibbles.push(RECEIPT_NIBBLE);

            if let Some(index) = receipt_index {
                let mut rlp_buf = vec![];
                index.encode(&mut rlp_buf);

                for byte in rlp_buf {
                    key_nibbles.push(byte >> 4);
                    key_nibbles.push(byte & 0xF);
                }
            }
        }
        KeyInput::TxIndex(tx_index) => {
            key_nibbles.push(TRANSACTION_NIBBLE);

            if let Some(index) = tx_index {
                let mut rlp_buf = vec![];
                index.encode(&mut rlp_buf);

                for byte in rlp_buf {
                    key_nibbles.push(byte >> 4);
                    key_nibbles.push(byte & 0xF);
                }
            }
        }
        KeyInput::BlockHeader => key_nibbles.push(BLOCK_HEADER_NIBBLE),
        KeyInput::TxHash(tx_hash) => {
            key_nibbles.push(TRANSACTION_HASH_NIBBLE);

            for byte in tx_hash {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::BlockHash(block_hash) => {
            key_nibbles.push(BLOCK_HASH_NIBBLE);

            for byte in block_hash {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::CallFrame(tx_index) => {
            key_nibbles.push(CALL_FRAME_NIBBLE);

            if let Some(index) = tx_index {
                let mut rlp_buf = vec![];
                index.encode(&mut rlp_buf);

                for byte in rlp_buf {
                    key_nibbles.push(byte >> 4);
                    key_nibbles.push(byte & 0xF);
                }
            }
        }
        KeyInput::BftBlock => key_nibbles.push(BFT_BLOCK_NIBBLE),
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}
