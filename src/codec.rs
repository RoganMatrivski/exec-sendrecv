use iroh_blobs::ticket::BlobTicket;

use crate::snapshot::Snapshot;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PeerMessages {
    DirSnapshot(Snapshot),
    PayloadInfo { total_size: u64, ticket: BlobTicket },
    Progress { current: u64, total: u64 },
    Ack,
    ErrorMsg(String),
}

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

type PeerSink<W> = SymmetricallyFramed<
    FramedWrite<W, LengthDelimitedCodec>,
    PeerMessages,
    SymmetricalBincode<PeerMessages>,
>;

type PeerStream<R> = SymmetricallyFramed<
    FramedRead<R, LengthDelimitedCodec>,
    PeerMessages,
    SymmetricalBincode<PeerMessages>,
>;

pub fn peer_channel<W, R>(send: W, recv: R) -> (PeerSink<W>, PeerStream<R>)
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    // TODO: Use LengthDelimitedCodec::builder().max_frame_bytes(...) to set appropriate limits.
    let sink = SymmetricallyFramed::new(
        FramedWrite::new(send, LengthDelimitedCodec::new()),
        SymmetricalBincode::<PeerMessages>::default(),
    );
    // TODO: Consider manual framing or streaming if messages grow beyond memory-safe frame limits.
    let stream = SymmetricallyFramed::new(
        FramedRead::new(recv, LengthDelimitedCodec::new()),
        SymmetricalBincode::<PeerMessages>::default(),
    );
    (sink, stream)
}
