use std::collections::HashSet;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::hasher::{Hash, Hasher, HasherType};
use monad_types::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    ledger::*,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::*,
};

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[serde(deny_unknown_fields)]
pub struct QuorumCertificate<SCT> {
    pub info: QcInfo,

    #[serde(serialize_with = "serialize_signature_collection::<_, SCT>")]
    #[serde(deserialize_with = "deserialize_signature_collection::<_, SCT>")]
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub signatures: SCT,

    signature_hash: Hash,
}

impl<T: std::fmt::Debug> std::fmt::Debug for QuorumCertificate<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QC")
            .field("info", &self.info)
            .field("sigs", &self.signatures)
            .field("signature_hash", &self.signature_hash)
            .finish_non_exhaustive()
    }
}

fn serialize_signature_collection<S, SCT>(
    signature_collection: &SCT,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    SCT: SignatureCollection,
    S: Serializer,
{
    let hex_str = "0x".to_string() + &hex::encode(signature_collection.serialize());
    serializer.serialize_str(&hex_str)
}

fn deserialize_signature_collection<'de, D, SCT>(deserializer: D) -> Result<SCT, D::Error>
where
    SCT: SignatureCollection,
    D: Deserializer<'de>,
{
    let buf = <std::string::String as Deserialize>::deserialize(deserializer)?;

    let Some(hex_str) = buf.strip_prefix("0x") else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

    SCT::deserialize(bytes.as_ref()).map_err(<D::Error as serde::de::Error>::custom)
}

#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize, RlpDecodable, RlpEncodable)]
pub struct QcInfo {
    pub vote: Vote,
}

impl QcInfo {
    pub fn get_round(&self) -> Round {
        self.vote.vote_info.round
    }

    pub fn get_epoch(&self) -> Epoch {
        self.vote.vote_info.epoch
    }
}

impl std::fmt::Debug for QcInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QcInfo").field("v", &self.vote).finish()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Rank(pub QcInfo);

impl PartialEq for Rank {
    fn eq(&self, other: &Self) -> bool {
        self.0.get_round() == other.0.get_round()
    }
}

impl Eq for Rank {}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Rank {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.get_round().0.cmp(&other.0.get_round().0)
    }
}

impl<SCT: SignatureCollection> QuorumCertificate<SCT> {
    pub fn new(info: QcInfo, signatures: SCT) -> Self {
        let hash = signatures.get_hash();
        QuorumCertificate {
            info,
            signatures,
            signature_hash: hash,
        }
    }

    // This will be the initial high qc for all nodes
    pub fn genesis_qc() -> Self {
        let vote_info = VoteInfo {
            id: GENESIS_BLOCK_ID,
            epoch: Epoch(1),
            round: Round(0),
            parent_id: GENESIS_BLOCK_ID,
            parent_round: Round(0),
            seq_num: GENESIS_SEQ_NUM,
            timestamp: 0,
            version: MonadVersion::version(),
        };

        let sigs = SCT::new(Vec::new(), &ValidatorMapping::new(std::iter::empty()), &[])
            .expect("genesis qc sigs");
        let sig_hash = sigs.get_hash();

        QuorumCertificate {
            info: QcInfo {
                vote: Vote {
                    vote_info,
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            signatures: sigs,
            signature_hash: sig_hash,
        }
    }

    pub fn get_hash(&self) -> Hash {
        self.signature_hash
    }

    pub fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> HashSet<NodeId<SCT::NodeIdPubKey>> {
        // TODO-3, consider caching this qc_msg hash in qc for performance in future
        let qc_msg = HasherType::hash_object(&self.info.vote);
        self.signatures
            .get_participants(validator_mapping, qc_msg.as_ref())
    }

    pub fn get_round(&self) -> Round {
        self.info.get_round()
    }

    pub fn get_epoch(&self) -> Epoch {
        self.info.get_epoch()
    }

    pub fn get_block_id(&self) -> BlockId {
        self.info.vote.vote_info.id
    }

    pub fn get_seq_num(&self) -> SeqNum {
        self.info.vote.vote_info.seq_num
    }

    pub fn get_timestamp(&self) -> u64 {
        self.info.vote.vote_info.timestamp
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampAdjustmentDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Copy)]
pub struct TimestampAdjustment {
    pub delta: u64,
    pub direction: TimestampAdjustmentDirection,
}
