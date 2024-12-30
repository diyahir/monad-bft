use bytes::Bytes;
use monad_bls::BlsSignatureCollection;
use monad_compress::{brotli::BrotliCompression, CompressionAlgo};
use monad_consensus::messages::consensus_message::{ConsensusMessage, ProtocolMessage};
use monad_consensus_types::{
    payload::FullTransactionList, state_root_hash::StateRootHash, voting::ValidatorMapping,
};
use monad_secp::SecpSignature;
use monad_state::VerifiedMonadMessage;
use monad_testutil::{proposal::ProposalGen, validators::create_keys_w_validators};
use monad_types::{Epoch, Round, SeqNum, Serializable};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetType},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use peak_alloc::PeakAlloc;
use rand_chacha::rand_core::{RngCore, SeedableRng};

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

fn main() {
    // created a serialized proposal message
    // transaction hashes follow a random distribution
    let mut transactions = [0x00_u8; 400 * 5000];
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
    rng.fill_bytes(&mut transactions);

    let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<
        SecpSignature,
        BlsSignatureCollection<_>,
        _,
    >(10, ValidatorSetFactory::default());

    let validator_stakes = Vec::from_iter(valset.get_members().clone());

    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(Epoch(1), validator_stakes, ValidatorMapping::new(valmap));
    let election = SimpleRoundRobin::default();
    let mut propgen: ProposalGen<_, _> =
        ProposalGen::<SecpSignature, BlsSignatureCollection<_>>::new();

    let proposal = propgen
        .next_proposal(
            &keys,
            cert_keys.as_slice(),
            &epoch_manager,
            &val_epoch_map,
            &election,
            FullTransactionList::new(transactions.to_vec().into()),
            StateRootHash::default(),
        )
        .destructure()
        .2;

    let epoch = epoch_manager
        .get_epoch(proposal.block.round)
        .expect("epoch exists");
    let proposer_leader = election.get_leader(
        proposal.block.round,
        val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
    );
    let leader_key = keys
        .iter()
        .find(|k| k.pubkey() == proposer_leader.pubkey())
        .expect("key in valset");

    let proposal: VerifiedMonadMessage<_, _> = ConsensusMessage {
        version: 1,
        message: ProtocolMessage::Proposal(proposal),
    }
    .sign::<SecpSignature>(leader_key)
    .into();

    let proposal_bytes: Bytes = proposal.serialize();

    println!(
        "current mem usage before compression {:?} MB",
        PEAK_ALLOC.current_usage_as_mb()
    );
    println!(
        "peak mem usage before compression {:?} MB",
        PEAK_ALLOC.peak_usage_as_mb()
    );
    // compress proposal
    let algo = BrotliCompression::new(11, 22, Vec::new());
    let mut compressed = Vec::new();
    algo.compress(&proposal_bytes, &mut compressed)
        .expect("compression success");

    // report the peak memory usage
    println!(
        "current mem usage after compression {:?} MB",
        PEAK_ALLOC.current_usage_as_mb()
    );
    println!(
        "peak mem usage after compression {:?} MB",
        PEAK_ALLOC.peak_usage_as_mb()
    );

    println!("uncompressed size {:?}", proposal_bytes.len());
    println!("compressed size {:?}", compressed.len());
}
