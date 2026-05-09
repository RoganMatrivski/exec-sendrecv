use std::path::PathBuf;

use color_eyre::eyre::{self, Context};
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

        tracing::debug!(?path, "building collection");
        let root = dunce::canonicalize(path)?;
        let files = walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|x| !x.file_type().is_dir())
            .map(walkdir::DirEntry::into_path);

        let (root_tag, total_size) = node.create_collection(root, files).await?;
        tracing::info!(
            hash = %root_tag.hash(),
            format = ?root_tag.format(),
            "collection built"
        );

        let ticket = BlobTicket::new(node.addr().await?, root_tag.hash(), root_tag.format());
        tracing::debug!(
            ticket_addr = ?ticket.addr(),
            ticket_hash = %ticket.hash(),
            ticket_format = ?ticket.format(),
            "built blob ticket"
        );

        let payload = crate::node::InfoPayload {
            blob_ticket: ticket,
            total_bytes: total_size,
        };

        let payload_bin: Vec<u8> = postcard::to_stdvec(&payload)?;

        tracing::info!(?peer_ticket, "connecting to receiver");
        let conn = node
            .endpoint()
            .connect(peer_ticket, ALPN)
            .await
            .wrap_err("Failed to connect to iroh endpoint")?;
        tracing::debug!("connection established");

        let (mut send, mut recv_ack) = conn.open_bi().await?;
        tracing::debug!("opened bidi stream to receiver");

        tracing::info!("sending ticket payload");
        tokio::io::AsyncWriteExt::write_all(&mut send, &payload_bin).await?;
        send.finish()?;
        tracing::debug!("ticket sent and stream finished");

        tracing::info!("waiting for receiver ack");
        let mut ack = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut recv_ack, &mut ack).await?;

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
