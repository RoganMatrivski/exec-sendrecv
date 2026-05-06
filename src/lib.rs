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
use sha2::Digest;

mod broker;

const ALPN: &[u8] = b"i/dont/like/this/rock/robert";
const BROKER_ALPN: &[u8] = b"i/dont/like/this/rock/robert/broker";

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
    Send(String, String, PathBuf),
    Receive(String, PathBuf),
    Broker(String),
}

impl Handler {
    pub async fn run(&self) -> color_eyre::eyre::Result<()> {
        match self {
            Handler::Send(broker_id, recv_code, path) => {
                let endpoint = get_endpoint_builder()?.bind().await?;
                let store = MemStore::new();

                // Derive broker's PublicKey from the shared broker_id
                let broker_key = broker::broker_public_key(broker_id);

                // Ask broker for the receiver's PublicKey
                tracing::info!("Looking up receiver via broker...");
                let receiver_key = broker::broker_lookup(&endpoint, broker_key, &recv_code).await?;
                tracing::info!(?receiver_key, "Found receiver");

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
                    id: receiver_key,       // your PublicKey
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
            Handler::Receive(broker_id, filedir) => {
                let endpoint = get_endpoint_builder()?.bind().await?;
                let store = MemStore::new();

                let id = endpoint.id();
                let fingerprint = get_device_code();
                tracing::info!(?id, "App ID: {fingerprint}");
                let key = endpoint.id();

                // Derive broker's PublicKey and register ourselves
                let broker_key = broker::broker_public_key(broker_id);
                broker::broker_register(&endpoint, broker_key, &fingerprint, key).await?;

                println!("Your code (give this to sender): {fingerprint}");
                tracing::info!("Registered with broker. Waiting for sender...");

                let handler = TicketReceiver {
                    store,
                    filedir: filedir.clone(),
                    endpoint: endpoint.clone(),
                };

                let router = Router::builder(endpoint).accept(ALPN, handler).spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
            Handler::Broker(client_id) => {
                let secret_key = broker::derive_secret_key(&client_id);

                let endpoint = get_endpoint_builder()?
                    .secret_key(secret_key)
                    .bind()
                    .await?;

                tracing::info!("Broker pubkey: {}", endpoint.id());

                let handler = broker::BrokerHandler::default();

                let router = Router::builder(endpoint)
                    .accept(BROKER_ALPN, handler)
                    .spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
        }

        Ok(())
    }
}

pub fn get_device_code() -> String {
    use machineid_rs::{Encryption, HWIDComponent, IdBuilder};

    let mut builder = IdBuilder::new(Encryption::SHA256);
    let fingerprint = builder
        .add_component(HWIDComponent::SystemID)
        .add_component(HWIDComponent::Username)
        .build("")
        .expect("Failed getting device fingerprint");

    let bytes = fingerprint.as_bytes();
    let hash = sha2::Sha256::digest(bytes);
    let n = u64::from_le_bytes(hash[..8].try_into().unwrap());
    let id = (n % 9_900_000_000) + 100_000_000;
    format!("{id}")
}

fn get_endpoint_builder() -> color_eyre::eyre::Result<iroh::endpoint::Builder> {
    // let dns = iroh::dns::DnsResolver::builder()
    //     .with_nameservers(vec![
    //         ("1.1.1.1:443".parse()?, iroh::dns::DnsProtocol::Https),
    //         ("1.0.0.1:443".parse()?, iroh::dns::DnsProtocol::Https),
    //     ])
    //     .build();

    let endpoint_builder = Endpoint::builder(presets::N0)
        // .clear_address_lookup()
        // .dns_resolver(dns)
        .address_lookup(iroh::address_lookup::mdns::MdnsAddressLookup::builder());
    // let endpoint_builder = Endpoint::builder(presets::Minimal)
    //     .relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::from_iter(vec![
    //         iroh::RelayUrl::from_str("https://relay.srv3.rgmtrv.my.id/")?,
    //     ])))
    //     .dns_resolver(dns)
    //     .addr_filter(iroh::endpoint_info::AddrFilter::unfiltered())
    //     // .address_lookup(iroh::address_lookup::PkarrPublisher::n0_dns())
    //     // .address_lookup(iroh::address_lookup::DnsAddressLookup::n0_dns())
    //     .address_lookup(iroh::address_lookup::mdns::MdnsAddressLookup::builder());

    Ok(endpoint_builder)
}
