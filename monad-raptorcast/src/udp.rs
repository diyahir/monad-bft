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

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZero,
    ops::Range,
};

use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use lru::LruCache;
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hasher, HasherType},
    signing_domain,
};
use monad_dataplane::RecvUdpMsg;
use monad_merkle::{MerkleHash, MerkleProof, MerkleTree};
use monad_raptor::SOURCE_SYMBOLS_MIN;
use monad_types::{Epoch, NodeId, Stake};
use rand::seq::SliceRandom;
use tracing::{debug, warn};

use crate::{
    decoding::{DecoderCache, DecodingContext, TryDecodeError, TryDecodeStatus},
    message::MAX_MESSAGE_SIZE,
    util::{
        compute_hash, unix_ts_ms_now, AppMessageHash, BroadcastMode, BuildTarget, EpochValidators,
        HexBytes, NodeIdHash, ReBroadcastGroupMap, Redundancy,
    },
    SIGNATURE_SIZE,
};

const _: () = assert!(
    MAX_MERKLE_TREE_DEPTH <= 0xF,
    "merkle tree depth must be <= 4 bits"
);

pub const SIGNATURE_CACHE_SIZE: NonZero<usize> = NonZero::new(10_000).unwrap();

// We assume an MTU of at least 1280 (the IPv6 minimum MTU), which for the maximum Merkle tree
// depth of 9 gives a symbol size of 960 bytes, which we will use as the minimum chunk length for
// received packets, and we'll drop received chunks that are smaller than this to mitigate attacks
// involving a peer sending us a message as a very large set of very small chunks.
const MIN_CHUNK_LENGTH: usize = 960;

// Drop a message to be transmitted if it would lead to more than this number of packets
// to be transmitted.  This can happen in Broadcast mode when the message is large or
// if we have many peers to transmit the message to.
const MAX_NUM_PACKETS: usize = 65535;

// For a message with K source symbols, we accept up to the first MAX_REDUNDANCY * K
// encoded symbols.
//
// Any received encoded symbol with an ESI equal to or greater than MAX_REDUNDANCY * K
// will be discarded, as a protection against DoS and algorithmic complexity attacks.
//
// We pick 7 because that is the largest value that works for all values of K, as K
// can be at most 8192, and there can be at most 65521 encoding symbol IDs.
pub const MAX_REDUNDANCY: Redundancy = Redundancy::from_u8(7);

// For a tree depth of 1, every encoded symbol is its own Merkle tree, and there will be no
// Merkle proof section in the constructed RaptorCast packets.
//
// For a tree depth of 9, the index of the rightmost Merkle tree leaf will be 0xff, and the
// Merkle leaf index field is 8 bits wide.
const MIN_MERKLE_TREE_DEPTH: u8 = 1;
const MAX_MERKLE_TREE_DEPTH: u8 = 9;

pub(crate) struct UdpState<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    max_age_ms: u64,

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    decoder_cache: DecoderCache<CertificateSignaturePubKey<ST>>,

    signature_cache:
        LruCache<[u8; HEADER_LEN as usize + 20], NodeId<CertificateSignaturePubKey<ST>>>,
}

impl<ST: CertificateSignatureRecoverable> UdpState<ST> {
    pub fn new(self_id: NodeId<CertificateSignaturePubKey<ST>>, max_age_ms: u64) -> Self {
        Self {
            self_id,
            max_age_ms,

            decoder_cache: DecoderCache::default(),
            signature_cache: LruCache::new(SIGNATURE_CACHE_SIZE),
        }
    }

    /// Given a RecvUdpMsg, emits all decoded messages while rebroadcasting as necessary
    #[tracing::instrument(level = "debug", name = "udp_handle_message", skip_all)]
    pub fn handle_message(
        &mut self,
        current_epoch: Epoch,
        group_map: &ReBroadcastGroupMap<ST>,
        epoch_validators: &BTreeMap<Epoch, EpochValidators<ST>>,
        rebroadcast: impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>, Bytes, u16),
        message: RecvUdpMsg,
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let self_id = self.self_id;
        let self_hash = compute_hash(&self_id);

        let mut broadcast_batcher =
            BroadcastBatcher::new(self_id, rebroadcast, &message.payload, message.stride);

        let mut messages = Vec::new(); // The return result; decoded messages

        for payload_start_idx in (0..message.payload.len()).step_by(message.stride.into()) {
            // scoped variables are dropped in reverse order of declaration.
            // when *batch_guard is dropped, packets can get flushed
            let mut batch_guard = broadcast_batcher.create_flush_guard();

            let payload_end_idx =
                (payload_start_idx + usize::from(message.stride)).min(message.payload.len());
            let payload = message.payload.slice(payload_start_idx..payload_end_idx);
            // "message" here means a raptor-casted chunk (AKA r10 symbol), not the whole final message (proposal)
            let parsed_message = match parse_message::<ST>(
                &mut self.signature_cache,
                payload,
                self.max_age_ms,
            ) {
                Ok(message) => message,
                Err(err) => {
                    tracing::debug!(src_addr = ?message.src_addr, ?err, "unable to parse message");
                    continue;
                }
            };

            // Ignore chunk if self is the author
            // This can happen if a peer validator rebroadcasts a message back to self
            if parsed_message.author == self.self_id {
                tracing::trace!(
                    app_message_hash =? parsed_message.app_message_hash,
                    encoding_symbol_id =? parsed_message.chunk_id,
                    "received raptor chunk generated by self"
                );
                continue;
            }

            // Enforce a minimum chunk size for messages consisting of multiple source chunks.
            if parsed_message.chunk.len() < MIN_CHUNK_LENGTH
                && usize::try_from(parsed_message.app_message_len).unwrap()
                    > parsed_message.chunk.len()
            {
                tracing::debug!(
                    src_addr = ?message.src_addr,
                    chunk_length = parsed_message.chunk.len(),
                    MIN_CHUNK_LENGTH,
                    "dropping undersized received message",
                );
                continue;
            }

            let maybe_broadcast_mode = match (
                parsed_message.broadcast,
                parsed_message.secondary_broadcast,
            ) {
                (true, false) => Some(BroadcastMode::Primary),
                (false, true) => Some(BroadcastMode::Secondary),
                (false, false) => None,
                (true, true) => {
                    // invalid to have both primary and secondary broadcast bit set
                    debug!(
                        ?parsed_message.author,
                        "Receiving invalid message with both broadcast and secondary broadcast bit set"
                    );
                    continue;
                }
            };

            // Note: The check that parsed_message.author is valid is already
            // done in iterate_rebroadcast_peers(), but we want to drop invalid
            // chunks ASAP, before changing `recently_decoded_state`.
            if let Some(broadcast_mode) = maybe_broadcast_mode {
                if !group_map.check_source(
                    Epoch(parsed_message.epoch),
                    &parsed_message.author,
                    broadcast_mode,
                ) {
                    tracing::debug!(
                        src_addr = ?message.src_addr,
                        author =? parsed_message.author,
                        epoch =? parsed_message.epoch,
                        "not in raptorcast group"
                    );
                    continue;
                }
            } else if self_hash != parsed_message.recipient_hash {
                tracing::debug!(
                    src_addr = ?message.src_addr,
                    ?self_hash,
                    recipient_hash =? parsed_message.recipient_hash,
                    "dropping spoofed message"
                );
                continue;
            }

            tracing::trace!(
                src_addr = ?message.src_addr,
                app_message_len = ?parsed_message.app_message_len,
                self_id =? self.self_id,
                author =? parsed_message.author,
                unix_ts_ms = parsed_message.unix_ts_ms,
                app_message_hash =? parsed_message.app_message_hash,
                encoding_symbol_id =? parsed_message.chunk_id as usize,
                "received encoded symbol"
            );

            let mut try_rebroadcast_symbol = || {
                // rebroadcast raptorcast chunks if necessary
                if let Some(broadcast_mode) = maybe_broadcast_mode {
                    if self_hash == parsed_message.recipient_hash {
                        let maybe_targets = group_map.iterate_rebroadcast_peers(
                            Epoch(parsed_message.epoch),
                            &parsed_message.author,
                            broadcast_mode,
                        );
                        if let Some(targets) = maybe_targets {
                            batch_guard.queue_broadcast(
                                payload_start_idx,
                                payload_end_idx,
                                &parsed_message.author,
                                || targets.cloned().collect(),
                            )
                        }
                    }
                }
            };

            let validator_set = epoch_validators
                .get(&Epoch(parsed_message.epoch))
                .map(|ev| &ev.validators);

            let decoding_context =
                DecodingContext::new(validator_set, unix_ts_ms_now(), current_epoch);

            match self
                .decoder_cache
                .try_decode(&parsed_message, &decoding_context)
            {
                Err(TryDecodeError::InvalidSymbol(err)) => {
                    err.log(&parsed_message, &self.self_id);
                }

                Err(TryDecodeError::UnableToReconstructSourceData) => {
                    tracing::error!("failed to reconstruct source data");
                }

                Err(TryDecodeError::AppMessageHashMismatch { expected, actual }) => {
                    tracing::error!(
                        ?self_id,
                        author =? parsed_message.author,
                        ?expected,
                        ?actual,
                        "mismatch message hash"
                    );
                }

                Ok(TryDecodeStatus::RejectedByCache) => {
                    tracing::warn!(
                        ?self_id,
                        author =? parsed_message.author,
                        chunk_id = parsed_message.chunk_id,
                        "message rejected by cache, author may be flooding messages",
                    );
                }

                Ok(TryDecodeStatus::RecentlyDecoded) | Ok(TryDecodeStatus::NeedsMoreSymbols) => {
                    // TODO: cap rebroadcast symbols based on some multiple of esis.
                    try_rebroadcast_symbol();
                }

                Ok(TryDecodeStatus::Decoded {
                    author,
                    app_message,
                }) => {
                    // TODO: cap rebroadcast symbols based on some multiple of esis.
                    try_rebroadcast_symbol();
                    messages.push((author, app_message));
                }
            }
        }

        messages
    }
}

/// Stuff to include:
/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 1 bit => broadcast or not
/// - 1 bit => secondary broadcast or not (full-node raptorcast)
/// - 2 bits => unused
/// - 4 bits => Merkle tree depth
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp in milliseconds
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + chunk_len + payload))
///
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 1 byte => Chunk's merkle leaf idx
/// - 1 byte => reserved
/// - 2 bytes (u16) => This chunk's id
/// - rest => data
///
//
//
//
// pub struct M {
//     signature: [u8; 65],
//     version: u16,
//     broadcast: bool,
//     secondary_broadcast: bool,
//     merkle_tree_depth: u8,
//     epoch: u64,
//     unix_ts_ms: u64,
//     app_message_id: [u8; 20],
//     app_message_len: u32,
//
//     merkle_proof: Vec<[u8; 20]>,
//
//     chunk_recipient: [u8; 20],
//     chunk_merkle_leaf_idx: u8,
//     reserved: u8,
//     chunk_id: u16,
//
//     data: Bytes,
// }
pub const HEADER_LEN: u16 = SIGNATURE_SIZE as u16 // Sender signature (65 bytes)
            + 2  // Version
            + 1  // Broadcast bit, Secondary Broadcast bit, 2 unused bits, 4 bits for Merkle Tree Depth
            + 8  // Epoch #
            + 8  // Unix timestamp
            + 20 // AppMessage hash
            + 4; // AppMessage length
const CHUNK_HEADER_LEN: u16 = 20 // Chunk recipient hash
            + 1  // Chunk's merkle leaf idx
            + 1  // reserved
            + 2; // Chunk idx

pub fn build_messages<ST>(
    key: &ST::KeyPairType,
    segment_size: u16, // Each chunk in the returned Vec (Bytes element of the tuple) will be limited to this size
    app_message: Bytes, // This is the actual message that gets raptor-10 encoded and split into UDP chunks
    redundancy: Redundancy,
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let app_message_len: u32 = app_message.len().try_into().expect("message too big");

    build_messages_with_length(
        key,
        segment_size,
        app_message,
        app_message_len,
        redundancy,
        epoch_no,
        unix_ts_ms,
        build_target,
        known_addresses,
    )
}

// This should be called with app_message.len() == app_message_len, but we allow the caller
// to specify a different app_message_len to allow one of the unit tests to build an invalid
// (oversized) message that build_messages() would normally not allow you to build, in order
// to verify that the RaptorCast receive path doesn't crash when it receives such a message,
// as previous versions of the RaptorCast receive path would indeed crash when receiving
// such a message.
pub fn build_messages_with_length<ST>(
    key: &ST::KeyPairType,
    segment_size: u16,
    app_message: Bytes,
    app_message_len: u32,
    redundancy: Redundancy,
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    if app_message_len == 0 {
        tracing::warn!("build_messages_with_length() called with app_message_len = 0");
        return Vec::new();
    }

    if redundancy == Redundancy::ZERO {
        tracing::error!("build_messages_with_length() called with redundancy = 0");
        return Vec::new();
    }

    // body_size is the amount of space available for payload+proof in a single
    // UDP datagram. Our raptorcast encoding needs around 108 + 24 = 132 bytes
    // in each datagram. Typically:
    //      body_size = 1452 - (108 + 24) = 1320
    let body_size = segment_size
        .checked_sub(HEADER_LEN + CHUNK_HEADER_LEN)
        .expect("segment_size too small");

    let is_broadcast = matches!(
        build_target,
        BuildTarget::Broadcast(_) | BuildTarget::Raptorcast(_) | BuildTarget::FullNodeRaptorCast(_)
    );

    let self_id = NodeId::new(key.pubkey());

    // TODO make this more sophisticated
    let tree_depth: u8 = 6; // corresponds to 32 chunks (2^(h-1))
    assert!(tree_depth >= MIN_MERKLE_TREE_DEPTH);
    assert!(tree_depth <= MAX_MERKLE_TREE_DEPTH);

    let chunks_per_merkle_batch: usize = 2_usize // = 32
        .checked_pow(u32::from(tree_depth) - 1)
        .expect("tree depth too big");
    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1); // = 100

    // data_size is the amount of space available for raw payload (app_message)
    // in a single UDP datagram. Typically:
    //      data_size = 1452 - (108 + 24) - 100 = 1220
    let data_size = body_size.checked_sub(proof_size).expect("proof too big");
    let is_raptor_broadcast = matches!(build_target, BuildTarget::Raptorcast(_));
    let is_secondary_raptor_broadcast = matches!(build_target, BuildTarget::FullNodeRaptorCast(_));

    // Determine how many UDP datagrams (packets) we need to send out. Each
    // datagram can only effectively transport `data_size` (~1220) bytes out of
    // a total of `app_message_len` bytes (~18% total overhead).
    let num_packets: usize = {
        let mut num_packets: usize = (app_message_len as usize)
            .div_ceil(usize::from(data_size))
            .max(SOURCE_SYMBOLS_MIN);
        // amplify by redundancy factor
        num_packets = redundancy
            .scale(num_packets)
            .expect("redundancy-scaled num_packets doesn't fit in usize");

        if let BuildTarget::Broadcast(nodes) = &build_target {
            num_packets = num_packets
                .checked_mul(nodes.len())
                .expect("num_packets doesn't fit in usize")
        }

        if num_packets > MAX_NUM_PACKETS {
            tracing::warn!(
                ?build_target,
                ?known_addresses,
                num_packets,
                MAX_NUM_PACKETS,
                "exceeded maximum number of packets in a message, dropping message",
            );
            return Vec::new();
        }

        num_packets
    };

    // Create a long flat message, concatenating the (future) UDP bodies of all
    // datagrams. This includes everything except Ethernet, IP and UDP headers.
    let mut message = BytesMut::zeroed(segment_size as usize * num_packets);
    let app_message_hash: AppMessageHash = HexBytes({
        let mut hasher = HasherType::new();
        hasher.update(&app_message);
        hasher.hash().0[..20].try_into().unwrap()
    });

    // Each chunk_data[0..num_packets-1] is a reference to a slice of bytes
    // from the long flat `message` above. Each slice excludes the UDP buffer
    // span where we'd write the raptorcast header and proof.
    // Each chunk_data[ii] is a tuple (chunk_index, slice), where the first 20
    // bytes of the slice contains the hash of the chunk's destination node id.
    //                  chunk_datas[0]                  chunk_datas[1]                  chunk_datas[2]
    // | HEADER, proof, _______________| HEADER, proof, _______________| HEADER, proof, _______________|
    // |...........................................message.............................................|
    // |.........MTU UDP DATAGRAM......|
    let mut chunk_datas = message
        .chunks_mut(segment_size.into())
        .map(|chunk| (None, &mut chunk[(HEADER_LEN + proof_size).into()..]))
        .collect_vec();
    assert_eq!(chunk_datas.len(), num_packets);

    // the GSO-aware indices into `message`
    let mut outbound_gso_idx: Vec<(SocketAddr, Range<usize>)> = Vec::new();

    // populate chunk_recipient and outbound_gso_idx
    match build_target {
        BuildTarget::PointToPoint(to) => {
            let Some(addr) = known_addresses.get(to) else {
                tracing::warn!(
                    ?to,
                    "RaptorCast build_message PointToPoint not sending message, address unknown"
                );
                return Vec::new();
            };

            outbound_gso_idx.push((*addr, 0..segment_size as usize * num_packets));
            let node_hash = compute_hash(to);
            for (chunk_idx, (chunk_symbol_id, chunk_data)) in chunk_datas.iter_mut().enumerate() {
                // populate chunk_recipient
                chunk_data[0..20].copy_from_slice(node_hash.as_slice());
                *chunk_symbol_id = Some(chunk_idx as u16);
            }
        }
        BuildTarget::Broadcast(nodes) => {
            assert!(is_broadcast && !is_raptor_broadcast && !is_secondary_raptor_broadcast);
            let total_validators = nodes.len();
            let mut running_validator_count = 0;
            tracing::debug!(
                ?self_id,
                unix_ts_ms,
                app_message_len,
                ?redundancy,
                data_size,
                num_packets,
                ?app_message_hash,
                "RaptorCast Broadcast v2v message"
            );
            for node_id in nodes.iter() {
                let start_idx: usize = num_packets * running_validator_count / total_validators;
                running_validator_count += 1;
                let end_idx: usize = num_packets * running_validator_count / total_validators;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * segment_size as usize..end_idx * segment_size as usize,
                    ));
                } else {
                    tracing::warn!(
                        ?node_id,
                        "RaptorCast build_message Broadcast not sending message, address unknown"
                    )
                }
                let node_hash = compute_hash(node_id);
                for (chunk_idx, (chunk_symbol_id, chunk_data)) in
                    chunk_datas[start_idx..end_idx].iter_mut().enumerate()
                {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(node_hash.as_slice());
                    *chunk_symbol_id = Some(chunk_idx as u16);
                }
            }
        }
        BuildTarget::Raptorcast(epoch_validators) => {
            assert!(is_broadcast && is_raptor_broadcast && !is_secondary_raptor_broadcast);

            tracing::trace!(
                ?self_id,
                unix_ts_ms,
                app_message_len,
                ?redundancy,
                data_size,
                num_packets,
                ?app_message_hash,
                "RaptorCast v2v message"
            );

            assert!(!epoch_validators.is_empty());

            // generate chunks if epoch validators is not empty
            // FIXME should self be included in total_stake?
            let total_stake: Stake = epoch_validators.total_stake();

            if total_stake == Stake::ZERO {
                tracing::warn!(
                    ?self_id,
                    "RaptorCast build_message got zero total stake, not sending message"
                );
                return Vec::new();
            }

            let mut running_stake = Stake::ZERO;
            let mut chunk_idx = 0_u16;
            let mut validator_set: Vec<_> = epoch_validators.iter().collect();
            // Group shuffling so chunks for small proposals aren't always assigned
            // to the same nodes, until researchers come up with something better.
            validator_set.shuffle(&mut rand::thread_rng());
            for (node_id, stake) in validator_set {
                let start_idx: usize =
                    (num_packets as f64 * (running_stake / total_stake)) as usize;
                running_stake += stake;
                let end_idx: usize = (num_packets as f64 * (running_stake / total_stake)) as usize;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * segment_size as usize..end_idx * segment_size as usize,
                    ));
                } else {
                    tracing::warn!(
                        ?node_id,
                        "RaptorCast build_message Raptorcast not sending message, address unknown"
                    )
                }

                let node_hash = compute_hash(node_id);
                for (chunk_symbol_id, chunk_data) in chunk_datas[start_idx..end_idx].iter_mut() {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(node_hash.as_slice());
                    *chunk_symbol_id = Some(chunk_idx);
                    chunk_idx += 1;
                }
            }
        }
        BuildTarget::FullNodeRaptorCast(group) => {
            assert!(is_broadcast && !is_raptor_broadcast && is_secondary_raptor_broadcast);

            tracing::trace!(
                ?self_id,
                unix_ts_ms,
                app_message_len,
                ?redundancy,
                data_size,
                num_packets,
                ?app_message_hash,
                "RaptorCast v2fn message"
            );

            let total_peers = group.size_excl_self();
            let mut pp = 0;
            let mut chunk_idx = 0_u16;
            // Group shuffling so chunks for small proposals aren't always assigned
            // to the same nodes, until researchers come up with something better.
            for node_id in group.iter_skip_self_and_author(&self_id, rand::random::<usize>()) {
                let start_idx: usize = num_packets * pp / total_peers;
                pp += 1;
                let end_idx: usize = num_packets * pp / total_peers;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * segment_size as usize..end_idx * segment_size as usize,
                    ));
                } else {
                    tracing::warn!(?node_id, "not sending v2fn message, address unknown")
                }
                let node_hash = compute_hash(node_id);
                for (chunk_symbol_id, chunk_data) in chunk_datas[start_idx..end_idx].iter_mut() {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(node_hash.as_slice());
                    *chunk_symbol_id = Some(chunk_idx);
                    chunk_idx += 1;
                }
            }
        }
    };

    // In practice, a "symbol" is a UDP datagram payload of some 1220 bytes
    let encoder = match monad_raptor::Encoder::new(&app_message, usize::from(data_size)) {
        Ok(encoder) => encoder,
        Err(err) => {
            // TODO: signal this error to the caller
            tracing::warn!(?err, "unable to create Encoder, dropping message");
            return Vec::new();
        }
    };

    // populates the following chunk-specific stuff
    // - chunk_id: u16
    // - chunk_payload
    for (maybe_chunk_id, chunk_data) in chunk_datas.iter_mut() {
        let chunk_id = maybe_chunk_id.expect("generated chunk was not assigned an id");
        let chunk_len: u16 = data_size;

        let cursor = chunk_data;
        let (_cursor_chunk_recipient, cursor) = cursor.split_at_mut(20);
        let (_cursor_chunk_merkle_leaf_idx, cursor) = cursor.split_at_mut(1);
        let (_cursor_chunk_reserved, cursor) = cursor.split_at_mut(1);
        let (cursor_chunk_id, cursor) = cursor.split_at_mut(2);
        cursor_chunk_id.copy_from_slice(&chunk_id.to_le_bytes());
        let (cursor_chunk_payload, _cursor) = cursor.split_at_mut(chunk_len.into());

        // for BuildTarget::Broadcast, we will be encoding each chunk_id once per recipient
        //
        // we could cache these as an optimization, but probably doesn't make a big difference in
        // practice, because we're generally using BuildTarget::Broadcast for small messages.
        //
        // can revisit this later
        encoder.encode_symbol(
            &mut cursor_chunk_payload[..chunk_len.into()],
            chunk_id.into(),
        );
    }

    // At this point, everything BELOW chunk_merkle_leaf_idx is populated
    // populate merkle trees/roots/leaf_idx + signatures (cached)
    let version: u16 = 0;
    let epoch_no: u64 = epoch_no;
    let unix_ts_ms: u64 = unix_ts_ms;
    message
        // .par_chunks_mut(segment_size as usize * chunks_per_merkle_batch)
        .chunks_mut(segment_size as usize * chunks_per_merkle_batch)
        .for_each(|merkle_batch| {
            let mut merkle_batch = merkle_batch.chunks_mut(segment_size as usize).collect_vec();
            let merkle_leaves = merkle_batch
                .iter_mut()
                .enumerate()
                .map(|(chunk_idx, chunk)| {
                    let chunk_payload = &mut chunk[(HEADER_LEN + proof_size).into()..];
                    assert_eq!(
                        chunk_payload.len(),
                        CHUNK_HEADER_LEN as usize + data_size as usize
                    );
                    // populate merkle_leaf_idx
                    chunk_payload[20] = chunk_idx.try_into().expect("chunk idx doesn't fit in u8");

                    let mut hasher = HasherType::new();
                    hasher.update(chunk_payload);
                    hasher.hash()
                })
                .collect_vec();
            let merkle_tree = MerkleTree::new_with_depth(&merkle_leaves, tree_depth);
            let mut header_with_root = {
                let mut data = [0_u8; HEADER_LEN as usize + 20];
                let cursor = &mut data;
                let (_cursor_signature, cursor) = cursor.split_at_mut(SIGNATURE_SIZE);
                let (cursor_version, cursor) = cursor.split_at_mut(2);
                cursor_version.copy_from_slice(&version.to_le_bytes());
                let (cursor_broadcast_merkle_depth, cursor) = cursor.split_at_mut(1);
                cursor_broadcast_merkle_depth[0] = ((is_raptor_broadcast as u8) << 7)
                    | ((is_secondary_raptor_broadcast as u8) << 6)
                    | (tree_depth & 0b0000_1111); // tree_depth max 4 bits
                let (cursor_epoch_no, cursor) = cursor.split_at_mut(8);
                cursor_epoch_no.copy_from_slice(&epoch_no.to_le_bytes());
                let (cursor_unix_ts_ms, cursor) = cursor.split_at_mut(8);
                cursor_unix_ts_ms.copy_from_slice(&unix_ts_ms.to_le_bytes());
                let (cursor_app_message_hash, cursor) = cursor.split_at_mut(20);
                cursor_app_message_hash.copy_from_slice(&app_message_hash.0);
                let (cursor_app_message_len, cursor) = cursor.split_at_mut(4);
                cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());

                cursor.copy_from_slice(merkle_tree.root());
                // 65 // Sender signature
                // 2  // Version
                // 1  // Broadcast bit, Secondary broadcast bit, 2 unused bits, 4 bits for Merkle Tree Depth
                // 8  // Epoch #
                // 8  // Unix timestamp
                // 20 // AppMessage hash
                // 4  // AppMessage length
                // --
                // 20 // Merkle root

                data
            };
            let signature = ST::sign::<signing_domain::RaptorcastChunk>(
                &header_with_root[SIGNATURE_SIZE..],
                key,
            )
            .serialize();
            assert_eq!(signature.len(), SIGNATURE_SIZE);
            header_with_root[..SIGNATURE_SIZE].copy_from_slice(&signature);
            let header = &header_with_root[..HEADER_LEN as usize];
            for (leaf_idx, chunk) in merkle_batch.into_iter().enumerate() {
                chunk[..HEADER_LEN as usize].copy_from_slice(header);
                for (proof_idx, proof) in merkle_tree
                    .proof(leaf_idx as u8)
                    .siblings()
                    .iter()
                    .enumerate()
                {
                    let offset = HEADER_LEN as usize + 20 * proof_idx;
                    chunk[offset..offset + 20].copy_from_slice(proof);
                }
            }
        });

    let message = message.freeze();

    outbound_gso_idx
        .into_iter()
        .map(|(addr, range)| (addr, message.slice(range)))
        .collect()
}

#[derive(Clone, Debug)]
pub struct ValidatedMessage<PT>
where
    PT: PubKey,
{
    pub message: Bytes,

    // `author` is recovered from the public key in the chunk signature, which
    // was signed by the validator who encoded the proposal into raptorcast.
    // This applies to both validator-to-validator and validator-to-full-node
    // raptorcasting.
    pub author: NodeId<PT>,
    pub epoch: u64,
    pub unix_ts_ms: u64,
    pub app_message_hash: AppMessageHash,
    pub app_message_len: u32,
    pub broadcast: bool,
    pub secondary_broadcast: bool,
    pub recipient_hash: NodeIdHash, // if this matches our node_id, then we need to re-broadcast RaptorCast chunks
    pub chunk_id: u16,
    pub chunk: Bytes, // raptor-coded portion
}

#[derive(Debug)]
pub enum MessageValidationError {
    UnknownVersion,
    TooShort,
    TooLong,
    InvalidSignature,
    InvalidTreeDepth,
    InvalidMerkleProof,
    InvalidTimestamp {
        timestamp: u64,
        max: u64,
        delta: i64,
    },
}

/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 1 bit => broadcast or not
/// - 1 bit => secondary broadcast or not (full-node raptorcast)
/// - 2 bits => unused
/// - 4 bits => Merkle tree depth
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + chunk_len + payload))
///
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 1 byte => Chunk's merkle leaf idx
/// - 1 byte => reserved
/// - 2 bytes (u16) => This chunk's id
/// - rest => data
pub fn parse_message<ST>(
    signature_cache: &mut LruCache<
        [u8; HEADER_LEN as usize + 20],
        NodeId<CertificateSignaturePubKey<ST>>,
    >,
    message: Bytes,
    max_age_ms: u64,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
{
    let mut cursor: Bytes = message.clone();
    let mut split_off = |mid| {
        if mid > cursor.len() {
            Err(MessageValidationError::TooShort)
        } else {
            Ok(cursor.split_to(mid))
        }
    };
    let cursor_signature = split_off(SIGNATURE_SIZE)?;
    let signature =
        ST::deserialize(&cursor_signature).map_err(|_| MessageValidationError::InvalidSignature)?;

    let cursor_version = split_off(2)?;
    let version = u16::from_le_bytes(cursor_version.as_ref().try_into().expect("u16 is 2 bytes"));
    if version != 0 {
        return Err(MessageValidationError::UnknownVersion);
    }

    let cursor_broadcast_tree_depth = split_off(1)?[0];
    let broadcast = (cursor_broadcast_tree_depth & (1 << 7)) != 0;
    let secondary_broadcast = (cursor_broadcast_tree_depth & (1 << 6)) != 0;
    let tree_depth = cursor_broadcast_tree_depth & 0b0000_1111; // bottom 4 bits

    if !(MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&tree_depth) {
        return Err(MessageValidationError::InvalidTreeDepth);
    }

    let cursor_epoch = split_off(8)?;
    let epoch = u64::from_le_bytes(cursor_epoch.as_ref().try_into().expect("u64 is 8 bytes"));

    let cursor_unix_ts_ms = split_off(8)?;
    let unix_ts_ms = u64::from_le_bytes(
        cursor_unix_ts_ms
            .as_ref()
            .try_into()
            .expect("u64 is 8 bytes"),
    );

    ensure_valid_timestamp(unix_ts_ms, max_age_ms)?;

    let cursor_app_message_hash = split_off(20)?;
    let app_message_hash: AppMessageHash = HexBytes(
        cursor_app_message_hash
            .as_ref()
            .try_into()
            .expect("Hash is 20 bytes"),
    );

    let cursor_app_message_len = split_off(4)?;
    let app_message_len = u32::from_le_bytes(
        cursor_app_message_len
            .as_ref()
            .try_into()
            .expect("u32 is 4 bytes"),
    );

    if app_message_len as usize > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    };

    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1);

    let mut merkle_proof = Vec::new();
    for _ in 0..tree_depth - 1 {
        let cursor_sibling = split_off(20)?;
        let sibling =
            MerkleHash::try_from(cursor_sibling.as_ref()).expect("MerkleHash is 20 bytes");
        merkle_proof.push(sibling);
    }

    let cursor_recipient = split_off(20)?;
    let recipient_hash: NodeIdHash = HexBytes(
        cursor_recipient
            .as_ref()
            .try_into()
            .expect("Hash is 20 bytes"),
    );

    let cursor_merkle_idx = split_off(1)?[0];
    let merkle_proof = MerkleProof::new_from_leaf_idx(merkle_proof, cursor_merkle_idx)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;

    let _cursor_reserved = split_off(1)?;

    let cursor_chunk_id = split_off(2)?;
    let chunk_id = u16::from_le_bytes(cursor_chunk_id.as_ref().try_into().expect("u16 is 2 bytes"));

    let cursor_payload = cursor;
    if cursor_payload.is_empty() {
        // handle the degenerate case
        return Err(MessageValidationError::TooShort);
    }

    let leaf_hash = {
        let mut hasher = HasherType::new();
        hasher.update(
            &message[HEADER_LEN as usize + proof_size as usize..
                // HEADER_LEN as usize
                //     + proof_size as usize
                //     + CHUNK_HEADER_LEN as usize
                //     + payload_len as usize
                ],
        );
        hasher.hash()
    };
    let root = merkle_proof
        .compute_root(&leaf_hash)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;
    let mut signed_over = [0_u8; HEADER_LEN as usize + 20];
    // TODO can avoid this copy if necessary
    signed_over[..HEADER_LEN as usize].copy_from_slice(&message[..HEADER_LEN as usize]);
    signed_over[HEADER_LEN as usize..].copy_from_slice(&root);

    let author = *signature_cache.try_get_or_insert(signed_over, || {
        let author = signature
            .recover_pubkey::<signing_domain::RaptorcastChunk>(&signed_over[SIGNATURE_SIZE..])
            .map_err(|_| MessageValidationError::InvalidSignature)?;
        Ok(NodeId::new(author))
    })?;

    Ok(ValidatedMessage {
        message,
        author,
        epoch,
        unix_ts_ms,
        app_message_hash,
        app_message_len,
        broadcast,
        secondary_broadcast,
        recipient_hash,
        chunk_id,
        chunk: cursor_payload,
    })
}

fn ensure_valid_timestamp(unix_ts_ms: u64, max_age_ms: u64) -> Result<(), MessageValidationError> {
    let current_time_ms = if let Ok(current_time_elapsed) = std::time::UNIX_EPOCH.elapsed() {
        current_time_elapsed.as_millis() as u64
    } else {
        warn!("system time is before unix epoch, ignoring timestamp");
        return Ok(());
    };
    let delta = (current_time_ms as i64).saturating_sub(unix_ts_ms as i64);
    if delta.unsigned_abs() > max_age_ms {
        Err(MessageValidationError::InvalidTimestamp {
            timestamp: unix_ts_ms,
            max: max_age_ms,
            delta,
        })
    } else {
        Ok(())
    }
}

struct BroadcastBatch<PT: PubKey> {
    author: NodeId<PT>,
    targets: Vec<NodeId<PT>>,

    start_idx: usize,
    end_idx: usize,
}
pub(crate) struct BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    self_id: NodeId<PT>,
    rebroadcast: F,
    message: &'a Bytes,
    stride: u16,

    batch: Option<BroadcastBatch<PT>>,
}
impl<F, PT> Drop for BroadcastBatcher<'_, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    fn drop(&mut self) {
        self.flush()
    }
}
impl<'a, F, PT> BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    pub fn new(self_id: NodeId<PT>, rebroadcast: F, message: &'a Bytes, stride: u16) -> Self {
        Self {
            self_id,
            rebroadcast,
            message,
            stride,
            batch: None,
        }
    }

    pub fn create_flush_guard<'g>(&'g mut self) -> BatcherGuard<'a, 'g, F, PT>
    where
        'a: 'g,
    {
        BatcherGuard {
            batcher: self,
            flush_batch: true,
        }
    }

    fn flush(&mut self) {
        if let Some(batch) = self.batch.take() {
            tracing::trace!(
                self_id =? self.self_id,
                author =? batch.author,
                num_targets = batch.targets.len(),
                num_bytes = batch.end_idx - batch.start_idx,
                "rebroadcasting chunks"
            );
            (self.rebroadcast)(
                batch.targets,
                self.message.slice(batch.start_idx..batch.end_idx),
                self.stride,
            );
        }
    }
}
pub(crate) struct BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    batcher: &'g mut BroadcastBatcher<'a, F, PT>,
    flush_batch: bool,
}
impl<'a, 'g, F, PT> BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    pub(crate) fn queue_broadcast(
        &mut self,
        payload_start_idx: usize,
        payload_end_idx: usize,
        author: &NodeId<PT>,
        targets: impl FnOnce() -> Vec<NodeId<PT>>,
    ) {
        self.flush_batch = false;
        if self
            .batcher
            .batch
            .as_ref()
            .is_some_and(|batch| &batch.author == author)
        {
            let batch = self.batcher.batch.as_mut().unwrap();
            assert_eq!(batch.end_idx, payload_start_idx);
            batch.end_idx = payload_end_idx;
        } else {
            self.batcher.flush();
            self.batcher.batch = Some(BroadcastBatch {
                author: *author,
                targets: targets(),

                start_idx: payload_start_idx,
                end_idx: payload_end_idx,
            })
        }
    }
}
impl<'a, 'g, F, PT> Drop for BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    fn drop(&mut self) {
        if self.flush_batch {
            self.batcher.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use bytes::{Bytes, BytesMut};
    use itertools::Itertools;
    use lru::LruCache;
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hasher, HasherType},
    };
    use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, RecvUdpMsg};
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{Epoch, NodeId, Round, RoundSpan, Stake};
    use rstest::*;

    use super::{MessageValidationError, UdpState};
    use crate::{
        udp::{build_messages, parse_message, SIGNATURE_CACHE_SIZE},
        util::{BuildTarget, EpochValidators, Group, ReBroadcastGroupMap, Redundancy},
    };

    type SignatureType = SecpSignature;
    type KeyPairType = KeyPair;

    fn validator_set() -> (
        KeyPairType,
        EpochValidators<SignatureType>,
        HashMap<NodeId<CertificateSignaturePubKey<SignatureType>>, SocketAddr>,
    ) {
        const NUM_KEYS: u8 = 100;
        let mut keys = (0_u8..NUM_KEYS)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPairType::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
                .collect(),
        };

        let known_addresses = keys
            .iter()
            .skip(NUM_KEYS as usize / 10) // test some missing known_addresses
            .enumerate()
            .map(|(idx, key)| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), idx as u16),
                )
            })
            .collect();

        (keys.pop().unwrap(), validators, known_addresses)
    }

    const EPOCH: u64 = 5;
    const UNIX_TS_MS: u64 = 5;

    #[test]
    fn test_roundtrip() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let app_message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(&app_message);
            hasher.hash()
        };

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message.clone(),
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message =
                    parse_message::<SignatureType>(&mut signature_cache, message.clone(), u64::MAX)
                        .expect("valid message");
                assert_eq!(parsed_message.message, message);
                assert_eq!(parsed_message.app_message_hash.0, app_message_hash.0[..20]);
                assert_eq!(parsed_message.unix_ts_ms, UNIX_TS_MS);
                assert!(parsed_message.broadcast);
                assert_eq!(parsed_message.app_message_len, app_message.len() as u32);
                assert_eq!(parsed_message.author, NodeId::new(key.pubkey()));
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let mut message: BytesMut = aggregate_message
                    .split_to(DEFAULT_SEGMENT_SIZE.into())
                    .as_ref()
                    .into();
                // try flipping each bit
                for bit_idx in 0..message.len() * 8 {
                    let old_byte = message[bit_idx / 8];
                    // flip bit
                    message[bit_idx / 8] = old_byte ^ (1 << (bit_idx % 8));
                    let maybe_parsed = parse_message::<SignatureType>(
                        &mut signature_cache,
                        message.clone().into(),
                        u64::MAX,
                    );

                    // check that decoding fails
                    assert!(
                        maybe_parsed.is_err()
                            || maybe_parsed.unwrap().author != NodeId::new(key.pubkey())
                    );

                    // reset bit
                    message[bit_idx / 8] = old_byte;
                }
            }
        }
    }

    #[test]
    fn test_raptorcast_chunk_ids() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let mut used_ids = HashSet::new();

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message =
                    parse_message::<SignatureType>(&mut signature_cache, message.clone(), u64::MAX)
                        .expect("valid message");
                let newly_inserted = used_ids.insert(parsed_message.chunk_id);
                assert!(newly_inserted);
            }
        }
    }

    #[test]
    fn test_broadcast_bit() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let epoch_validators = validators.view_without(vec![&self_id]);
        let full_nodes = Group::new_fullnode_group(
            epoch_validators.iter_nodes().cloned().collect(),
            &self_id,
            self_id,
            RoundSpan::new(Round(1), Round(100)).unwrap(),
        );

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let build_targets = vec![
            BuildTarget::Raptorcast(epoch_validators),
            BuildTarget::FullNodeRaptorCast(&full_nodes),
        ];

        for build_target in build_targets {
            let messages = build_messages::<SignatureType>(
                &key,
                DEFAULT_SEGMENT_SIZE, // segment_size
                app_message.clone(),
                Redundancy::from_u8(2),
                EPOCH, // epoch_no
                UNIX_TS_MS,
                build_target.clone(),
                &known_addresses,
            );

            let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

            for (_to, mut aggregate_message) in messages {
                while !aggregate_message.is_empty() {
                    let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                    let parsed_message = parse_message::<SignatureType>(
                        &mut signature_cache,
                        message.clone(),
                        u64::MAX,
                    )
                    .expect("valid message");

                    match build_target {
                        BuildTarget::Raptorcast(_) => {
                            assert!(parsed_message.broadcast);
                            assert!(!parsed_message.secondary_broadcast);
                        }
                        BuildTarget::FullNodeRaptorCast(_) => {
                            assert!(!parsed_message.broadcast);
                            assert!(parsed_message.secondary_broadcast);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    #[test]
    fn test_broadcast_chunk_ids() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 8].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Broadcast(epoch_validators.into()),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let mut used_ids: HashMap<SocketAddr, HashSet<_>> = HashMap::new();

        let messages_len = messages.len();
        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message =
                    parse_message::<SignatureType>(&mut signature_cache, message.clone(), u64::MAX)
                        .expect("valid message");
                let newly_inserted = used_ids
                    .entry(to)
                    .or_default()
                    .insert(parsed_message.chunk_id);
                assert!(newly_inserted);
            }
        }

        assert_eq!(used_ids.len(), messages_len);
        let ids = used_ids.values().next().unwrap().clone();
        assert!(used_ids.values().all(|x| x == &ids)); // check that all recipients are sent same ids
        assert!(ids.contains(&0)); // check that starts from idx 0
    }

    #[test]
    fn test_handle_message_stride_slice() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let mut group_map = ReBroadcastGroupMap::new(self_id);
        let node_stake_pairs: Vec<_> = validators
            .validators
            .iter()
            .map(|(node_id, stake)| (*node_id, *stake))
            .collect();
        group_map.push_group_validator_set(node_stake_pairs, Epoch(1));
        let validator_set = [(Epoch(1), validators)].into_iter().collect();

        let mut udp_state = UdpState::<SignatureType>::new(self_id, u64::MAX);

        // payload will fail to parse but shouldn't panic on index error
        let payload: Bytes = vec![1_u8; 1024 * 8 + 1].into();
        let recv_msg = RecvUdpMsg {
            src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            payload,
            stride: 1024,
        };

        udp_state.handle_message(
            Epoch(1),
            &group_map,
            &validator_set,
            |_targets, _payload, _stride| {},
            recv_msg,
        );
    }

    #[rstest]
    #[case(-2 * 60 * 60 * 1000, u64::MAX, true)]
    #[case(2 * 60 * 60 * 1000, u64::MAX, true)]
    #[case(-2 * 60 * 60 * 1000, 0, false)]
    #[case(2 * 60 * 60 * 1000, 0, false)]
    #[case(-30_000, 60_000, true)]
    #[case(-120_000, 60_000, false)]
    #[case(120_000, 60_000, false)]
    #[case(30_000, 60_000, true)]
    #[case(-90_000, 60_000, false)]
    #[case(90_000, 60_000, false)]
    fn test_timestamp_validation(
        #[case] timestamp_offset_ms: i64,
        #[case] max_age_ms: u64,
        #[case] should_succeed: bool,
    ) {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);
        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let current_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        let test_timestamp = (current_time as i64 + timestamp_offset_ms) as u64;

        let app_message = Bytes::from_static(b"test message");
        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE,
            app_message,
            Redundancy::from_u8(1),
            0,
            test_timestamp,
            BuildTarget::Broadcast(epoch_validators.into()),
            &known_addresses,
        );
        let message = messages.into_iter().next().unwrap().1;
        let result = parse_message::<SignatureType>(&mut signature_cache, message, max_age_ms);

        if should_succeed {
            assert!(result.is_ok(), "unexpected success: {:?}", result.err());
        } else {
            assert!(result.is_err());
            match result.err().unwrap() {
                MessageValidationError::InvalidTimestamp { .. } => {}
                other => panic!("unexpected error {:?}", other),
            }
        }
    }
}
