// test utils for mock swarm unit test
#[cfg(test)]
pub mod test_tool {
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use monad_blocksync::messages::message::{
        BlockSyncHeadersResponse, BlockSyncRequestMessage, BlockSyncResponseMessage,
    };
    use monad_consensus::messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    };
    use monad_consensus_types::{
        block::{
            BlockRange, ConsensusBlockHeader, MockExecutionBody, MockExecutionProposedHeader,
            MockExecutionProtocol,
        },
        ledger::CommitResult,
        payload::{ConsensusBlockBody, FullTransactionList, RoundSignature},
        quorum_certificate::{QcInfo, QuorumCertificate},
        state_root_hash::StateRootHash,
        timeout::{Timeout, TimeoutInfo},
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::Hash,
        NopKeyPair, NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use monad_state::VerifiedMonadMessage;
    use monad_testutil::signing::create_keys;
    use monad_transformer::{LinkMessage, ID};
    use monad_types::{BlockId, DontCare, Epoch, NodeId, Round, SeqNum};

    type ST = NopSignature;
    type KeyPairType = <ST as CertificateSignature>::KeyPairType;
    type PubKeyType = CertificateSignaturePubKey<ST>;
    type SC = MultiSig<NopSignature>;
    type QC = QuorumCertificate<SC>;
    type EP = MockExecutionProtocol;

    /// FIXME-3 these should take in from/to/from_tick as params, not have defaults
    pub fn get_mock_message() -> LinkMessage<PubKeyType, String> {
        let keys = create_keys::<ST>(2);
        LinkMessage {
            from: ID::new(NodeId::new(keys[0].pubkey())),
            to: ID::new(NodeId::new(keys[1].pubkey())),
            message: "Dummy Message".to_string(),
            from_tick: Duration::from_millis(10),
        }
    }

    pub fn fake_node_id() -> NodeId<PubKeyType> {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPairType::from_bytes(&mut privkey).unwrap();
        NodeId::new(keypair.pubkey())
    }

    pub fn fake_qc() -> QuorumCertificate<SC> {
        QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        ..DontCare::dont_care()
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MultiSig { sigs: vec![] },
        )
    }

    pub fn fake_block(round: Round) -> (ConsensusBlockHeader<ST, SC, EP>, ConsensusBlockBody<EP>) {
        let body = ConsensusBlockBody {
            execution_body: MockExecutionBody {
                data: Bytes::default(),
            },
        };

        (
            ConsensusBlockHeader::new(
                fake_node_id(),
                Epoch(1),
                round,
                Vec::new(), // delayed_results
                MockExecutionProposedHeader {},
                body.get_id(),
                fake_qc(),
                SeqNum(0),
                0,
                RoundSignature::new(round, &NopKeyPair::from_bytes(&mut [127; 32]).unwrap()),
            ),
            body,
        )
    }

    pub fn fake_proposal_message(
        kp: &KeyPairType,
        round: Round,
    ) -> VerifiedMonadMessage<ST, SC, EP> {
        let (block_header, block_body) = fake_block(round);
        let internal_msg = ProposalMessage {
            block_header,
            block_body,
            last_round_tc: None,
        };
        ConsensusMessage {
            version: 1,
            message: ProtocolMessage::Proposal(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_vote_message(kp: &KeyPairType, round: Round) -> VerifiedMonadMessage<ST, SC, EP> {
        let vote_info = VoteInfo {
            round,
            ..DontCare::dont_care()
        };
        let internal_msg = VoteMessage {
            vote: Vote {
                vote_info,
                ledger_commit_info: CommitResult::NoCommit,
            },
            sig: NopSignature::sign(&[0x00_u8, 32], kp),
        };
        ConsensusMessage {
            version: 1,
            message: ProtocolMessage::Vote(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_timeout_message(kp: &KeyPairType) -> VerifiedMonadMessage<ST, SC, EP> {
        let timeout_info = TimeoutInfo {
            epoch: Epoch(1),
            round: Round(0),
            high_qc: fake_qc(),
        };
        let internal_msg = TimeoutMessage {
            timeout: Timeout {
                tminfo: timeout_info,
                last_round_tc: None,
            },
            sig: NopSignature::sign(&[0x00_u8, 32], kp),
        };
        ConsensusMessage {
            version: 1,
            message: ProtocolMessage::Timeout(internal_msg),
        }
        .sign(kp)
        .into()
    }

    pub fn fake_request_block_sync() -> VerifiedMonadMessage<ST, SC, EP> {
        let internal_msg = BlockSyncRequestMessage::Headers(BlockRange {
            last_block_id: BlockId(Hash([0x01_u8; 32])),
            max_blocks: SeqNum(1),
        });
        VerifiedMonadMessage::BlockSyncRequest(internal_msg)
    }

    pub fn fake_block_sync() -> VerifiedMonadMessage<ST, SC, EP> {
        let internal_msg = BlockSyncResponseMessage::HeadersResponse(
            BlockSyncHeadersResponse::NotAvailable(BlockRange {
                last_block_id: BlockId(Hash([0x01_u8; 32])),
                max_blocks: SeqNum(1),
            }),
        );
        VerifiedMonadMessage::BlockSyncResponse(internal_msg)
    }
}
