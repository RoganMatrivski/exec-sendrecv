use std::path::PathBuf;

use color_eyre::eyre::{self, Context};
use futures::{AsyncReadExt, SinkExt, StreamExt};
use iroh_blobs::ticket::BlobTicket;
use iroh_tickets::endpoint::EndpointTicket;

use crate::{node::Node, ALPN};

#[tracing::instrument(skip(node))]
pub async fn run(node: Node, peer_ticket: EndpointTicket, path: &PathBuf) -> eyre::Result<()> {
    tracing::info!("starting send handler");

    let conn = node
        .endpoint()
        .connect(peer_ticket, ALPN)
        .await
        .wrap_err("Failed to connect to iroh endpoint")?;
    tracing::info!("Connection established to receiver");

    let (send, recv) = conn.open_bi().await?;
    tracing::info!("Bidi-stream opened");

    // Send an initial message to trigger the receiver's accept_bi()
    let (mut sink, mut stream) = crate::codec::peer_channel(send, recv);
    sink.send(crate::codec::PeerMessages::Ack).await?;
    sink.flush().await?;

    // Make progress bar for sender to track
    let pb = crate::MPB.add(indicatif::ProgressBar::new(0));
    pb.set_style(indicatif::ProgressStyle::with_template(
        "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})",
    )?);
    pb.set_message("Sending");

    // Start consuming stream, we'll accept uni-stream when we know it's coming
    while let Some(msg) = stream.next().await {
        match msg? {
            crate::codec::PeerMessages::DirSnapshot(snapshot) => {
                let src = crate::snapshot::Snapshot::capture(path)?;

                let diffs = snapshot.diff(&src);
                let (deleted, others): (Vec<_>, Vec<_>) = diffs
                    .iter()
                    .partition(|x| matches!(x, crate::snapshot::Change::Deleted(_)));

                let changed_added = others.into_iter().map(|x| x.get_path()).collect::<Vec<_>>();
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
                    ticket_format = %ticket.format(),
                    "built blob ticket"
                );

                sink.send(crate::codec::PeerMessages::PayloadInfo {
                    total_size,
                    ticket,
                    delete_targets: deleted,
                })
                .await?;

                // Now we expect the uni-stream
                let mut progress_stream = conn.accept_uni().await?;

                let pb_clone = pb.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 16];
                    while progress_stream.read_exact(&mut buf).await.is_ok() {
                        let current = u64::from_be_bytes(buf[0..8].try_into().unwrap());
                        let total = u64::from_be_bytes(buf[8..16].try_into().unwrap());
                        pb_clone.set_position(current);
                        pb_clone.set_length(total);
                    }
                });
            }

            crate::codec::PeerMessages::PayloadInfo { .. } => {
                // Ignore, was local echo
            }

            crate::codec::PeerMessages::ErrorMsg(e) => {
                // TODO: Properly handle error from peer and stop execution gracefully.
                tracing::warn!(e);
            }

            crate::codec::PeerMessages::Ack => {
                tracing::info!("Received final Ack from receiver");
                pb.finish_with_message("Done sending");

                ()
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
