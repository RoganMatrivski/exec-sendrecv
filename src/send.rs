use std::path::PathBuf;

use color_eyre::eyre::{self, Context};
use futures::{SinkExt, StreamExt};
use iroh_blobs::ticket::BlobTicket;
use tracing::Instrument;

use crate::{broker, node::Node, ALPN};

pub async fn run(broker_id: &str, recv_code: &str, path: &PathBuf) -> eyre::Result<()> {
    let span = tracing::info_span!(
        "handler.send",
        broker_id = %broker_id,
        path = ?path,
    );

    async move {
        tracing::info!("starting send handler");

        let node = Node::new().await?;
        tracing::debug!("node created");

        let broker_addr = broker::resolve_broker_addr(broker_id);
        let recv_code = recv_code.split_whitespace().collect::<Vec<_>>().join("");

        tracing::info!("looking up receiver via broker");
        let peer_ticket = broker::broker_lookup(node.endpoint(), broker_addr, &recv_code).await?;
        tracing::info!(?peer_ticket, "found receiver");

        let conn = node
            .endpoint()
            .connect(peer_ticket, ALPN)
            .await
            .wrap_err("Failed to connect to iroh endpoint")?;
        tracing::info!("Connection established to receiver");

        let (send, recv) = conn.open_bi().await?;
        tracing::info!("Bidi-stream opened");

        let (mut sink, mut stream) = crate::codec::peer_channel(send, recv);

        // Send an initial message to trigger the receiver's accept_bi()
        sink.send(crate::codec::PeerMessages::Ack).await?;
        sink.flush().await?;

        while let Some(msg) = stream.next().await {
            match msg? {
                crate::codec::PeerMessages::DirSnapshot(snapshot) => {
                    let src = crate::snapshot::Snapshot::capture(path)?;

                    let diffs = snapshot.diff(&src);
                    let (deleted, others): (Vec<_>, Vec<_>) = diffs
                        .iter()
                        .partition(|x| matches!(x, crate::snapshot::Change::Deleted(_)));

                    let changed_added =
                        others.into_iter().map(|x| x.get_path()).collect::<Vec<_>>();
                    let deleted = deleted
                        .into_iter()
                        .map(|x| x.get_path())
                        .collect::<Vec<_>>();

                    tracing::trace!(
                        changed_added = ?changed_added,
                        deleted = ?deleted,
                        "Changes detected in directory"
                    );

                    let root = dunce::canonicalize(path)?;

                    let (root_tag, total_size) = node
                        .create_collection(root, changed_added.into_iter())
                        .await?;
                    tracing::info!(
                        hash = %root_tag.hash(),
                        format = ?root_tag.format(),
                        "collection built"
                    );

                    let ticket =
                        BlobTicket::new(node.addr().await?, root_tag.hash(), root_tag.format());
                    tracing::debug!(
                        ticket_addr = ?ticket.addr(),
                        ticket_hash = %ticket.hash(),
                        ticket_format = ?ticket.format(),
                        "built blob ticket"
                    );

                    sink.send(crate::codec::PeerMessages::PayloadInfo {
                        total_size,
                        ticket,
                        delete_targets: deleted,
                    })
                    .await?;
                }

                crate::codec::PeerMessages::ErrorMsg(e) => {
                    // TODO: Properly handle error from peer and stop execution gracefully.
                    tracing::warn!(e);
                }
                crate::codec::PeerMessages::Progress { current, total } => {
                    // TODO: Implement these
                    ()
                }

                crate::codec::PeerMessages::Ack => {
                    tracing::info!("Received final Ack from receiver");
                    break;
                }
                _ => (),
            }
        }

        tracing::info!("shutting down router");
        node.router.shutdown().await?;
        conn.close(0u32.into(), b"bye");
        tracing::info!("send handler done");

        // TODO: Find better way to do this
        // when tx dropped it should've be gone
        std::process::exit(0);
    }
    .instrument(span)
    .await
}
