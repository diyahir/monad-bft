use monad_bls::BlsSignatureCollection;
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::VoteMessage,
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::MockExecutionProtocol,
    ledger::CommitResult,
    payload::FullTransactionList,
    state_root_hash::StateRootHash,
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hasher, HasherType},
    NopSignature,
};
use monad_executor_glue::{
    convert::interface::{deserialize_event, serialize_event},
    ConsensusEvent, MonadEvent,
};
use monad_multi_sig::MultiSig;
use monad_testutil::{
    proposal::ProposalGen,
    signing::{get_certificate_key, get_key},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, Epoch, MonadVersion, NodeId, Round, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetType},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type ExecutionProtocolType = MockExecutionProtocol;

#[test]
fn test_consensus_timeout_event() {
    let event = MonadEvent::ConsensusEvent(
        ConsensusEvent::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::Timeout,
    );

    let buf = serialize_event(&event);
    let rx_event = deserialize_event(&buf);

    assert_eq!(event, rx_event.unwrap());
}

#[test]
fn test_consensus_message_event_vote_multisig() {
    let keypair = get_key::<SignatureType>(0);
    let certkeypair = get_certificate_key::<SignatureCollectionType>(7);
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        epoch: Epoch(1),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
        timestamp: 0,
        version: MonadVersion::version(),
    };
    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: CommitResult::Commit,
    };

    let votemsg: ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
        ProtocolMessage::Vote(VoteMessage::new(vote, &certkeypair));
    let conmsg = ConsensusMessage {
        version: 1,
        message: votemsg,
    };
    let conmsg_hash = HasherType::hash_object(&conmsg);
    let sig = SignatureType::sign(conmsg_hash.as_ref(), &keypair);
    let unmsg = Unverified::new(Unvalidated::new(conmsg), sig);

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: NodeId::new(keypair.pubkey()),
        unverified_message: unmsg,
    });

    let buf = serialize_event(&event);
    let rx_event =
        deserialize_event::<SignatureType, SignatureCollectionType, ExecutionProtocolType>(&buf);

    assert_eq!(event, rx_event.unwrap());
}

#[test]
fn test_consensus_message_event_proposal_bls() {
    let validator_set_factory = ValidatorSetFactory::default();
    let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<
        SignatureType,
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>,
        _,
    >(10, validator_set_factory);
    let mut val_epoch_map = ValidatorsEpochMapping::new(validator_set_factory);
    val_epoch_map.insert(
        Epoch(1),
        Vec::from_iter(valset.get_members().clone()),
        ValidatorMapping::new(valmap),
    );
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let election = SimpleRoundRobin::default();
    let mut propgen: ProposalGen<
        SignatureType,
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>,
    > = ProposalGen::new();

    let proposal = propgen.next_proposal(
        &keys,
        cert_keys.as_slice(),
        &epoch_manager,
        &val_epoch_map,
        &election,
        FullTransactionList::empty(),
        Vec::new(),
    );

    let consensus_proposal_msg = ProtocolMessage::Proposal((*proposal).clone());
    let conmsg = ConsensusMessage {
        version: 1,
        message: consensus_proposal_msg,
    };
    let conmsg_hash = HasherType::hash_object(&conmsg);
    let sig = SignatureType::sign(conmsg_hash.as_ref(), &keys[0]);

    let uvm = Unverified::new(Unvalidated::new(conmsg), sig);

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: NodeId::new(proposal.author().pubkey()),
        unverified_message: uvm,
    });

    let buf = serialize_event(&event);
    let rx_event = deserialize_event::<
        SignatureType,
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>,
        ExecutionProtocolType,
    >(&buf);

    assert_eq!(event, rx_event.unwrap());
}
