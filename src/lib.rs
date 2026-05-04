use std::{
    collections::BTreeSet,
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};

use color_eyre::eyre::{Context, ContextCompat};
use iroh::{
    endpoint::presets,
    protocol::{ProtocolHandler, Router},
    Endpoint, EndpointAddr, PublicKey,
};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket, BlobsProtocol};

const ALPN: &[u8] = b"i/dont/like/this/rock/robert";

#[derive(Debug, Clone)]
struct TicketReceiver {
    store: MemStore,
    endpoint: Endpoint,
    filedir: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Payload<B: Display, F: Display> {
    blob: B,
    filename: F,
}

impl ProtocolHandler for TicketReceiver {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let store = self.store.clone();
        let endpoint = self.endpoint.clone();

        let mut recv = conn.accept_uni().await?;

        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;

        let payload = String::from_utf8(buf).expect("Failed to parse payload");

        tracing::debug!(payload, "RECV Payload");

        let payload: Payload<BlobTicket, std::borrow::Cow<'_, str>> =
            serde_json::from_str(&payload).expect("Failed parsing payload");
        let ticket: BlobTicket = payload.blob;

        let dest = PathBuf::from_str(&payload.filename).expect("Failed parsing filename as path");

        let dest_dir = ensure_dir(&self.filedir).expect("Failed using path as dir");
        let dest = dest_dir.join(&dest);
        let dest = std::path::absolute(dest)?;

        let dl = store.downloader(&endpoint);

        dl.download(ticket.hash(), Some(ticket.addr().id)).await?;

        tracing::info!(?dest, "Done downloading. Copying...");

        store
            .blobs()
            .export(ticket.hash(), dest)
            .await
            .expect("Failed copying from memory to local");

        Ok(())
    }
}

use std::{fs, io};

pub fn ensure_dir(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref();

    if path.as_os_str().is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
    }

    // Creates it if missing; succeeds if it already exists as a directory.
    fs::create_dir_all(&path)?;

    Ok(path.to_path_buf())
}

pub enum Handler {
    Send(PublicKey, PathBuf),
    Receive(PathBuf),
}

impl Handler {
    pub async fn run(&self) -> color_eyre::eyre::Result<()> {
        match self {
            Handler::Send(id, path) => {
                let mdns = iroh::address_lookup::mdns::MdnsAddressLookup::builder();
                let endpoint = Endpoint::builder(presets::N0)
                    .address_lookup(mdns)
                    .bind()
                    .await?;
                let store = MemStore::new();

                let blobs = BlobsProtocol::new(&store, None);

                tracing::debug!(?path, "Hashing file");
                let tag = store.blobs().add_path(&path).await?;

                let ticket = BlobTicket::new(endpoint.addr(), tag.hash, tag.format);
                let filename = path
                    .file_name()
                    .wrap_err("Failed getting filename from path")?
                    .to_string_lossy();

                let payload = Payload {
                    blob: ticket,
                    filename,
                };

                let addr = EndpointAddr {
                    id: id.clone(),         // your PublicKey
                    addrs: BTreeSet::new(), // empty set -> discovery will be used
                };

                let conn = endpoint
                    .connect(addr, ALPN)
                    .await
                    .wrap_err("Failed to connect to iroh endpoint")?;

                let mut send = conn.open_uni().await?;

                tokio::io::AsyncWriteExt::write_all(
                    &mut send,
                    serde_json::to_string(&payload)?.as_bytes(),
                )
                .await?;
                send.finish()?;

                let router = Router::builder(endpoint)
                    .accept(iroh_blobs::ALPN, blobs)
                    .spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
            Handler::Receive(filedir) => {
                let mdns = iroh::address_lookup::mdns::MdnsAddressLookup::builder();
                let endpoint = Endpoint::builder(presets::N0)
                    .address_lookup(mdns)
                    .bind()
                    .await?;
                let store = MemStore::new();

                let id = endpoint.id();
                tracing::info!(?id, "App iD");

                let handler = TicketReceiver {
                    store,
                    filedir: filedir.clone(),
                    endpoint: endpoint.clone(),
                };

                let router = Router::builder(endpoint).accept(ALPN, handler).spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
        }

        Ok(())
    }
}
