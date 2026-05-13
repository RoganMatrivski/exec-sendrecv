use iroh_blobs::ticket::BlobTicket;

use crate::snapshot::Snapshot;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PeerMessages {
    DirSnapshot(Snapshot),
    Progress {
        current: u64,
        total: u64,
    },
    Ack,
    ErrorMsg(String),

    PayloadInfo {
        total_size: u64,
        ticket: BlobTicket,
        delete_targets: Vec<std::path::PathBuf>,
    },
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
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(64 * 1024 * 1024) // 64MB
        .new_codec();

    let sink = SymmetricallyFramed::new(
        FramedWrite::new(send, codec.clone()),
        SymmetricalBincode::<PeerMessages>::default(),
    );
    let stream = SymmetricallyFramed::new(
        FramedRead::new(recv, codec),
        SymmetricalBincode::<PeerMessages>::default(),
    );
    (sink, stream)
}
