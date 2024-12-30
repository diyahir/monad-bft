use monad_consensus::messages::message::{ProposalMessage, TimeoutMessage, VoteMessage};
use monad_consensus_types::{
    block::Block,
    ledger::CommitResult,
    payload::{ExecutionProtocol, FullTransactionList, Payload},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    hasher::{Hashable, Hasher, HasherType},
    NopKeyPair, NopSignature,
};
use monad_multi_sig::MultiSig;
use monad_testutil::signing::*;
use monad_types::*;
use test_case::test_case;
use zerocopy::AsBytes;

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<NopSignature>;

// TODO: remove test after TimeoutDigest type
#[ignore]
#[test]
fn timeout_digest() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        ..DontCare::dont_care()
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.epoch);
    hasher.update(ti.round);
    hasher.update(ti.high_qc.get_round());
    let h1 = hasher.hash();

    let h2 = ti.timeout_digest();

    assert_eq!(h1, h2);
}

// TODO: update test after hash update, hash(rlp(obj))
#[ignore]
#[test]
fn timeout_info_hash() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        ..DontCare::dont_care()
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.epoch);
    hasher.update(ti.round.0.as_bytes());
    hasher.update(ti.high_qc.get_block_id().0.as_bytes());
    hasher.update(ti.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&ti);

    assert_eq!(h1, h2);
}

// TODO: update test after hash update, hash(rlp(obj))
#[ignore]
#[test]
fn timeout_hash() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        ..DontCare::dont_care()
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let mut hasher = HasherType::new();
    hasher.update(tmo.tminfo.epoch.0.as_bytes());
    hasher.update(tmo.tminfo.round.0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.get_block_id().0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo);

    assert_eq!(h1, h2);
}

// TODO: update test after hash update, hash(rlp(obj))
#[ignore]
#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        ..DontCare::dont_care()
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let cert_key = get_certificate_key::<MockSignatures<SignatureType>>(7);

    let tmo_msg = TimeoutMessage::new(tmo, &cert_key);

    let mut hasher = HasherType::new();
    hasher.update(tmo_msg.timeout.tminfo.epoch.0.as_bytes());
    hasher.update(tmo_msg.timeout.tminfo.round.0.as_bytes());
    hasher.update(
        tmo_msg
            .timeout
            .tminfo
            .high_qc
            .info
            .vote
            .vote_info
            .id
            .0
            .as_bytes(),
    );
    hasher.update(tmo_msg.timeout.tminfo.high_qc.get_hash());
    unsafe {
        let sig_bytes = std::mem::transmute::<
            <SignatureCollectionType as SignatureCollection>::SignatureType,
            [u8; std::mem::size_of::<
                <SignatureCollectionType as SignatureCollection>::SignatureType,
            >()],
        >(tmo_msg.sig);
        hasher.update(sig_bytes);
    }

    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo_msg);

    assert_eq!(h1, h2);
}

// FIXME: block_id is hash of rlp encoded block, update this test
#[ignore]
#[test]
fn proposal_msg_hash() {
    use monad_testutil::signing::block_hash;

    let txns = FullTransactionList::new(vec![1, 2, 3, 4].into());

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = <NopKeyPair as CertificateKeyPair>::from_bytes(&mut privkey).unwrap();
    let author = NodeId::new(keypair.pubkey());
    let epoch = Epoch(1);
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        QcInfo {
            vote: Vote {
                vote_info: VoteInfo {
                    ..DontCare::dont_care()
                },
                ledger_commit_info: CommitResult::NoCommit,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let payload = Payload { txns };
    let block = Block::<MockSignatures<SignatureType>>::new(
        author,
        0,
        epoch,
        round,
        &ExecutionProtocol::dont_care(),
        payload.get_id(),
        &qc,
    );

    let proposal: ProposalMessage<MockSignatures<SignatureType>> = ProposalMessage {
        block: block.clone(),
        payload,
        last_round_tc: None,
    };

    let h1 = HasherType::hash_object(&proposal);
    let h2 = block_hash(&block);

    assert_eq!(h1, h2);
}

#[test]
fn max_high_qc() {
    let high_qc_rounds = [
        HighQcRound { qc_round: Round(1) },
        HighQcRound { qc_round: Round(3) },
        HighQcRound { qc_round: Round(1) },
    ]
    .iter()
    .map(|x| {
        let msg = HasherType::hash_object(x);
        let keypair = get_key::<SignatureType>(0);
        HighQcRoundSigColTuple {
            high_qc_round: *x,
            sigs: SignatureType::sign(msg.as_ref(), &keypair),
        }
    })
    .collect();

    let tc = TimeoutCertificate {
        epoch: Epoch(1),
        round: Round(2),
        high_qc_rounds,
    };

    assert_eq!(tc.max_round(), Round(3));
}

// TODO: update test after hash update, hash(rlp(obj))
#[ignore]
#[test_case(CommitResult::NoCommit ; "None commit_state")]
#[test_case(CommitResult::Commit ; "Some commit_state")]
fn vote_msg_hash(cs: CommitResult) {
    let vi = VoteInfo {
        ..DontCare::dont_care()
    };

    let v = Vote {
        vote_info: vi,
        ledger_commit_info: cs,
    };

    let certkey = get_certificate_key::<SignatureCollectionType>(7);
    let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkey);

    let mut hasher = HasherType::new();
    v.hash(&mut hasher);
    unsafe {
        let sig_bytes = std::mem::transmute::<
            <SignatureCollectionType as SignatureCollection>::SignatureType,
            [u8; std::mem::size_of::<
                <SignatureCollectionType as SignatureCollection>::SignatureType,
            >()],
        >(vm.sig);
        hasher.update(sig_bytes);
    }

    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&vm);

    assert_eq!(h1, h2);
}
