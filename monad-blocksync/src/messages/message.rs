use alloy_rlp::{encode_list, Decodable, Encodable, Header};
use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader, ExecutionProtocol},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hashable, Hasher},
};

const BLOCK_SYNC_REQUEST_MESSAGE_NAME: &str = "BlockSyncRequestMessage";
const BLOCK_SYNC_RESPONSE_MESSAGE_NAME: &str = "BlockSyncResponseMessage";
const BLOCK_SYNC_HEADERS_RESPONSE_NAME: &str = "BlockSyncHeadersResponse";
const BLOCK_SYNC_PAYLOAD_RESPONSE_NAME: &str = "BlockSyncBodyResponse";

/// Request block sync message sent to a peer
///
/// The node sends the block sync request either missing blocks headers or
/// a single payload
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum BlockSyncRequestMessage {
    Headers(BlockRange),
    Payload(ConsensusBlockBodyId),
}

impl Encodable for BlockSyncRequestMessage {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_REQUEST_MESSAGE_NAME;
        match self {
            Self::Headers(b) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &b];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Payload(id) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for BlockSyncRequestMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_REQUEST_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncRequestMessage",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Headers(BlockRange::decode(&mut payload)?)),
            2 => Ok(Self::Payload(ConsensusBlockBodyId::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncRequestMessage",
            )),
        }
    }
}

impl Hashable for BlockSyncRequestMessage {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Found((BlockRange, Vec<ConsensusBlockHeader<ST, SCT, EPT>>)),
    NotAvailable(BlockRange),
}

impl<ST, SCT, EPT> BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn get_block_range(&self) -> BlockRange {
        match self {
            BlockSyncHeadersResponse::Found((block_range, _)) => *block_range,
            BlockSyncHeadersResponse::NotAvailable(block_range) => *block_range,
        }
    }
}

impl<ST, SCT, EPT> Encodable for BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_HEADERS_RESPONSE_NAME;
        match self {
            Self::Found((block_range, blocks)) => {
                let enc: [&dyn Encodable; 4] = [&name, &1u8, &block_range, &blocks];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NotAvailable(block_range) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &block_range];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_HEADERS_RESPONSE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncHeaderResponse",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Found((
                BlockRange::decode(&mut payload)?,
                Vec::<_>::decode(&mut payload)?,
            ))),
            2 => Ok(Self::NotAvailable(BlockRange::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncHeadersResponse",
            )),
        }
    }
}

impl<ST, SCT, EPT> Hashable for BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    Found(ConsensusBlockBody<EPT>),
    NotAvailable(ConsensusBlockBodyId),
}

impl<EPT> BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn get_payload_id(&self) -> ConsensusBlockBodyId {
        match self {
            BlockSyncBodyResponse::Found(payload) => payload.get_id(),
            BlockSyncBodyResponse::NotAvailable(payload_id) => *payload_id,
        }
    }
}

impl<EPT> Encodable for BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_PAYLOAD_RESPONSE_NAME;
        match self {
            Self::Found(payload) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &payload];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NotAvailable(payload_id) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &payload_id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<EPT> Decodable for BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_PAYLOAD_RESPONSE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncBodyResponse",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Found(ConsensusBlockBody::decode(&mut payload)?)),
            2 => Ok(Self::NotAvailable(ConsensusBlockBodyId::decode(
                &mut payload,
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncBodyResponse",
            )),
        }
    }
}

impl<EPT> Hashable for BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    HeadersResponse(BlockSyncHeadersResponse<ST, SCT, EPT>),
    PayloadResponse(BlockSyncBodyResponse<EPT>),
}

impl<ST, SCT, EPT> BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn found_headers(
        block_range: BlockRange,
        headers: Vec<ConsensusBlockHeader<ST, SCT, EPT>>,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::Found((
            block_range,
            headers,
        )))
    }

    pub fn headers_not_available(
        block_range: BlockRange,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::NotAvailable(
            block_range,
        ))
    }

    pub fn found_payload(
        payload: ConsensusBlockBody<EPT>,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncBodyResponse::Found(payload))
    }

    pub fn payload_not_available(
        payload_id: ConsensusBlockBodyId,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncBodyResponse::NotAvailable(payload_id))
    }
}

impl<ST, SCT, EPT> Encodable for BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_RESPONSE_MESSAGE_NAME;
        match self {
            Self::HeadersResponse(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::PayloadResponse(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_RESPONSE_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncResponseMessage",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::HeadersResponse(BlockSyncHeadersResponse::decode(
                &mut payload,
            )?)),
            2 => Ok(Self::PayloadResponse(BlockSyncBodyResponse::decode(
                &mut payload,
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncResponseMessage",
            )),
        }
    }
}

impl<ST, SCT, EPT> Hashable for BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}
